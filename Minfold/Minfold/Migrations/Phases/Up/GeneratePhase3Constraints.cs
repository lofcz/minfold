using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Up;

public static class GeneratePhase3Constraints
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable> targetSchema,
        ConcurrentDictionary<string, SqlTable> currentSchema)
    {
        StringBuilder sb = new StringBuilder();
        
        // Phase 3: Foreign key constraints
        // Collect FK changes from modified tables
        List<ForeignKeyChange> allFkChanges = new List<ForeignKeyChange>();
        foreach (TableDiff tableDiff in diff.ModifiedTables)
        {
            allFkChanges.AddRange(tableDiff.ForeignKeyChanges);
        }

        // Group FK changes by constraint name for multi-column FKs
        Dictionary<string, List<ForeignKeyChange>> fkGroups = new Dictionary<string, List<ForeignKeyChange>>();
        foreach (ForeignKeyChange fkChange in allFkChanges)
        {
            string fkName = (fkChange.NewForeignKey ?? fkChange.OldForeignKey)?.Name ?? string.Empty;
            if (!fkGroups.ContainsKey(fkName))
            {
                fkGroups[fkName] = new List<ForeignKeyChange>();
            }
            fkGroups[fkName].Add(fkChange);
        }

        // Generate FK statements: Drop first, then Add
        // Drop FKs that are being dropped or modified (need to drop old before adding new)
        // Use unique suffix to avoid conflicts with Phase 0 FK drops
        foreach (KeyValuePair<string, List<ForeignKeyChange>> fkGroup in fkGroups.OrderBy(g => g.Key))
        {
            ForeignKeyChange firstChange = fkGroup.Value[0];
            if (firstChange.ChangeType == ForeignKeyChangeType.Drop && firstChange.OldForeignKey != null)
            {
                string uniqueSuffix = MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(
                    firstChange.OldForeignKey.Schema, 
                    firstChange.OldForeignKey.Table, 
                    firstChange.OldForeignKey.Name, 
                    "phase3drop");
                sb.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(firstChange.OldForeignKey, uniqueSuffix));
            }
            else if (firstChange.ChangeType == ForeignKeyChangeType.Modify && firstChange.OldForeignKey != null)
            {
                // For modifications, drop the old FK before adding the new one
                string uniqueSuffix = MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(
                    firstChange.OldForeignKey.Schema, 
                    firstChange.OldForeignKey.Table, 
                    firstChange.OldForeignKey.Name, 
                    "phase3modify");
                sb.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(firstChange.OldForeignKey, uniqueSuffix));
            }
        }

        // Collect all FKs to be added (from new tables and modified tables)
        // Use NOCHECK → CHECK pattern to handle circular dependencies and improve performance
        List<(List<SqlForeignKey> FkGroup, bool WasNoCheck)> fksToAdd = new List<(List<SqlForeignKey>, bool)>();
        HashSet<string> processedFks = new HashSet<string>();

        // Add new FKs from new tables
        foreach (SqlTable newTable in diff.NewTables)
        {
            foreach (SqlTableColumn column in newTable.Columns.Values)
            {
                foreach (SqlForeignKey fk in column.ForeignKeys)
                {
                    if (!processedFks.Contains(fk.Name))
                    {
                        List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { fk };
                        // Find other columns with same FK (multi-column FK)
                        foreach (SqlTableColumn otherColumn in newTable.Columns.Values)
                        {
                            foreach (SqlForeignKey otherFk in otherColumn.ForeignKeys)
                            {
                                if (otherFk.Name == fk.Name && otherFk.Table == fk.Table && otherFk != fk)
                                {
                                    fkGroup.Add(otherFk);
                                }
                            }
                        }
                        // Store original NotEnforced state
                        bool wasNoCheck = fkGroup[0].NotEnforced;
                        fksToAdd.Add((fkGroup, wasNoCheck));
                        processedFks.Add(fk.Name);
                    }
                }
            }
        }

        // Add new/modified FKs from modified tables
        foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Add || c.ChangeType == ForeignKeyChangeType.Modify))
        {
            if (fkChange.NewForeignKey != null && !processedFks.Contains(fkChange.NewForeignKey.Name))
            {
                // Find all columns with this FK name
                TableDiff? tableDiff = diff.ModifiedTables.FirstOrDefault(t => t.TableName.Equals(fkChange.NewForeignKey.Table, StringComparison.OrdinalIgnoreCase));
                if (tableDiff != null)
                {
                    // Group multi-column FKs
                    List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { fkChange.NewForeignKey };
                    foreach (ForeignKeyChange otherFkChange in allFkChanges)
                    {
                        if (otherFkChange.NewForeignKey != null &&
                            otherFkChange.NewForeignKey.Name == fkChange.NewForeignKey.Name &&
                            otherFkChange.NewForeignKey.Table == fkChange.NewForeignKey.Table &&
                            otherFkChange.NewForeignKey != fkChange.NewForeignKey)
                        {
                            fkGroup.Add(otherFkChange.NewForeignKey);
                        }
                    }
                    // Store original NotEnforced state
                    bool wasNoCheck = fkGroup[0].NotEnforced;
                    fksToAdd.Add((fkGroup, wasNoCheck));
                    processedFks.Add(fkChange.NewForeignKey.Name);
                }
            }
        }

        // Create all FKs with NOCHECK first (avoids circular dependency issues and reduces lock time)
        Dictionary<string, SqlTable> tablesDict = new Dictionary<string, SqlTable>(targetSchema);
        foreach (var (fkGroup, wasNoCheck) in fksToAdd.OrderBy(g => g.FkGroup[0].Table).ThenBy(g => g.FkGroup[0].Name))
        {
            // Force NOCHECK during creation to avoid circular dependency issues
            string fkSql = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDict, forceNoCheck: true);
            sb.Append(fkSql);
            sb.AppendLine();
        }

        // Restore CHECK state for FKs that weren't originally NOCHECK
        // IMPORTANT: CHECK CONSTRAINT doesn't always restore is_not_trusted correctly after WITH NOCHECK
        // So we need to drop and recreate the FK with WITH CHECK to ensure correct NotEnforced state
        foreach (var (fkGroup, wasNoCheck) in fksToAdd.OrderBy(g => g.FkGroup[0].Table).ThenBy(g => g.FkGroup[0].Name))
        {
            if (!wasNoCheck)
            {
                SqlForeignKey firstFk = fkGroup[0];
                // Drop the FK that was created with NOCHECK
                // Use unique suffix to avoid conflicts with Phase 0 FK drops
                string uniqueSuffix = MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(firstFk.Schema, firstFk.Table, firstFk.Name, "phase3check");
                sb.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(firstFk, uniqueSuffix));
                
                // Recreate it with WITH CHECK to ensure correct NotEnforced state
                string fkSqlWithCheck = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDict, forceNoCheck: false);
                if (!string.IsNullOrEmpty(fkSqlWithCheck))
                {
                    sb.Append(fkSqlWithCheck);
                    sb.AppendLine();
                }
            }
        }

        // Add PRIMARY KEY constraints for columns that are gaining PK status or being rebuilt with PK
        // Get current schema (after modifications) to find new PK columns
        ConcurrentDictionary<string, SqlTable> currentSchemaAfterChanges = MigrationSchemaSnapshot.ApplySchemaDiffToTarget(targetSchema, diff);
        HashSet<string> tablesWithPkAdded = new HashSet<string>();
        
        foreach (TableDiff tableDiff in diff.ModifiedTables.OrderBy(t => t.TableName))
        {
            // Check if any columns are gaining PK status, being rebuilt with PK, or keeping PK status after modification
            bool needsPkRestored = false;
            foreach (ColumnChange change in tableDiff.ColumnChanges)
            {
                if (change.NewColumn != null && change.NewColumn.IsPrimaryKey)
                {
                    if (change.ChangeType == ColumnChangeType.Add || 
                        (change.ChangeType == ColumnChangeType.Modify && change.OldColumn != null && !change.OldColumn.IsPrimaryKey) ||
                        (change.ChangeType == ColumnChangeType.Modify && change.OldColumn != null && change.OldColumn.IsPrimaryKey) ||
                        (change.ChangeType == ColumnChangeType.Rebuild && change.OldColumn != null && change.OldColumn.IsPrimaryKey))
                    {
                        // PK needs to be restored if:
                        // 1. New column with PK (Add)
                        // 2. Column gaining PK status (Modify: OldColumn not PK, NewColumn is PK)
                        // 3. Column keeping PK status after modification (Modify: OldColumn was PK, NewColumn is PK) - PK was dropped in Phase 0
                        // 4. Column being rebuilt that was already PK (Rebuild: OldColumn was PK, NewColumn is PK)
                        needsPkRestored = true;
                        break;
                    }
                }
            }
            
            if (needsPkRestored && !tablesWithPkAdded.Contains(tableDiff.TableName.ToLowerInvariant()))
            {
                // Get all PK columns from the new schema (after changes)
                if (currentSchemaAfterChanges.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? newTable))
                {
                    List<SqlTableColumn> allPkColumns = newTable.Columns.Values.Where(c => c.IsPrimaryKey).OrderBy(c => c.OrdinalPosition).ToList();
                    if (allPkColumns.Count > 0)
                    {
                        List<string> pkColumnNames = allPkColumns.Select(c => c.Name).ToList();
                        string pkConstraintName = $"PK_{tableDiff.TableName}";
                        sb.Append(GeneratePrimaryKeys.GenerateAddPrimaryKeyStatement(tableDiff.TableName, pkColumnNames, pkConstraintName, newTable.Schema));
                        tablesWithPkAdded.Add(tableDiff.TableName.ToLowerInvariant());
                    }
                }
            }
        }

        // Add PRIMARY KEY constraints for new tables (already included in CREATE TABLE, so no action needed)

        // Restore FKs that were dropped in Phase 0 due to PK dependencies
        // After restoring PKs, we need to restore FKs that reference those PKs
        // These FKs were dropped in Phase 0 but aren't tracked as changes in the diff
        // ALSO restore FKs that reference columns whose types changed (propagated type changes)
        // These FKs were dropped in Phase 0 but need to be restored after column modifications
        HashSet<string> tablesWithColumnTypeChanges = new HashSet<string>();
        foreach (TableDiff tableDiff in diff.ModifiedTables)
        {
            foreach (ColumnChange change in tableDiff.ColumnChanges)
            {
                if (change.NewColumn != null && change.OldColumn != null && 
                    (change.NewColumn.SqlType != change.OldColumn.SqlType || change.ChangeType == ColumnChangeType.Rebuild))
                {
                    // This column's type changed or was rebuilt - FKs referencing it need to be restored
                    tablesWithColumnTypeChanges.Add(tableDiff.TableName.ToLowerInvariant());
                    string changeReason = change.ChangeType == ColumnChangeType.Rebuild 
                        ? "rebuilt" 
                        : $"{change.OldColumn.SqlType} -> {change.NewColumn.SqlType}";
                    MigrationLogger.Log($"  [TYPE CHANGE/REBUILD DETECTED] {tableDiff.TableName}.{change.NewColumn.Name}: {changeReason}");
                    break;
                }
            }
        }
        
        // Also check for tables that had columns added/modified that might be referenced by FKs
        // This handles cases where the type change detection above might miss some scenarios
        foreach (TableDiff tableDiff in diff.ModifiedTables)
        {
            foreach (ColumnChange change in tableDiff.ColumnChanges)
            {
                if (change.NewColumn != null && (
                    change.ChangeType == ColumnChangeType.Add ||
                    change.ChangeType == ColumnChangeType.Modify ||
                    change.ChangeType == ColumnChangeType.Rebuild))
                {
                    // Check if this column is a primary key or if it's being modified
                    // which might affect FKs that reference this table
                    if (change.NewColumn.IsPrimaryKey || change.OldColumn?.IsPrimaryKey == true)
                    {
                        // This table has PK changes - FKs referencing it need restoration
                        if (!tablesWithColumnTypeChanges.Contains(tableDiff.TableName.ToLowerInvariant()))
                        {
                            tablesWithColumnTypeChanges.Add(tableDiff.TableName.ToLowerInvariant());
                            MigrationLogger.Log($"  [PK CHANGE DETECTED] {tableDiff.TableName}.{change.NewColumn.Name}: Primary key {change.ChangeType}");
                        }
                    }
                }
            }
        }
        
        // CRITICAL FIX: Also check for any tables that have rebuild operations
        // This ensures FK restoration is triggered when tables are rebuilt
        foreach (TableDiff tableDiff in diff.ModifiedTables)
        {
            bool hasRebuild = tableDiff.ColumnChanges.Any(c => c.ChangeType == ColumnChangeType.Rebuild);
            if (hasRebuild)
            {
                // This table was rebuilt - FKs referencing it need restoration
                if (!tablesWithColumnTypeChanges.Contains(tableDiff.TableName.ToLowerInvariant()))
                {
                    tablesWithColumnTypeChanges.Add(tableDiff.TableName.ToLowerInvariant());
                    MigrationLogger.Log($"  [REBUILD DETECTED] {tableDiff.TableName}: Table was rebuilt, FKs need restoration");
                }
            }
        }
        
        // Combine tables with PK restored and tables with column type changes
        HashSet<string> tablesNeedingFkRestoration = new HashSet<string>(tablesWithPkAdded);
        tablesNeedingFkRestoration.UnionWith(tablesWithColumnTypeChanges);
        
        MigrationLogger.Log($"  [FK RESTORATION] Tables needing FK restoration: {string.Join(", ", tablesNeedingFkRestoration)}");
        MigrationLogger.Log($"  [FK RESTORATION] Tables with PK restored: {string.Join(", ", tablesWithPkAdded)}");
        MigrationLogger.Log($"  [FK RESTORATION] Tables with column type changes: {string.Join(", ", tablesWithColumnTypeChanges)}");
        
        Dictionary<string, HashSet<string>> fkInspectionSources;
        Dictionary<string, SqlTable> fkInspectionTables = BuildFkInspectionTables(currentSchema, targetSchema, diff, out fkInspectionSources);

        foreach (string tableNeedingRestoration in tablesNeedingFkRestoration)
        {
            MigrationLogger.Log($"  [FK RESTORATION] Checking FKs referencing table: {tableNeedingRestoration}");
            // Find all FKs in current schema (database state BEFORE Phase 0) that reference this table's PK columns or columns that changed type
            // Use currentSchema because it represents what actually existed before the migration started
            // This ensures we restore FKs that actually existed and were dropped in Phase 0, not extra FKs that might exist
            // We check against targetSchema to verify the FK should still exist (references a PK column or column with type change)
            foreach (KeyValuePair<string, SqlTable> tablePair in fkInspectionTables)
            {
                string sourceInfo = fkInspectionSources.TryGetValue(tablePair.Key, out HashSet<string>? sources)
                    ? string.Join("/", sources.OrderBy(s => s))
                    : "unknown";
                MigrationLogger.Log($"    [FK RESTORATION] Checking table: {tablePair.Value.Name}, sources: {sourceInfo}, columns: {string.Join(", ", tablePair.Value.Columns.Keys)}");
                foreach (SqlTableColumn column in tablePair.Value.Columns.Values)
                {
                    MigrationLogger.Log($"      [FK RESTORATION] Column {column.Name} has {column.ForeignKeys.Count} FKs: {string.Join(", ", column.ForeignKeys.Select(fk => fk.Name))}");
                    foreach (SqlForeignKey fk in column.ForeignKeys)
                    {
                        MigrationLogger.Log($"      [FK RESTORATION] Checking FK {fk.Name}: {fk.Table}.{fk.Column} -> {fk.RefTable}.{fk.RefColumn}");
                        // Check if this FK references the table whose PK was restored or whose column type changed
                        if (fk.RefTable.ToLowerInvariant() == tableNeedingRestoration)
                        {
                            MigrationLogger.Log($"    [CHECK FK] {fk.Name} from {fk.Table}.{fk.Column} references {fk.RefTable}.{fk.RefColumn}");
                            // Verify the FK references a PK column (for PK restoration) or any column (for type change restoration)
                            if (targetSchema.TryGetValue(fk.RefTable.ToLowerInvariant(), out SqlTable? refTable))
                            {
                                bool shouldRestore = false;
                                string restoreReason = "";
                                if (refTable.Columns.TryGetValue(fk.RefColumn.ToLowerInvariant(), out SqlTableColumn? refColumn))
                                {
                                    // Restore if:
                                    // 1. FK references a PK column (PK was restored)
                                    // 2. FK references a column whose type changed (type propagation)
                                    if (tablesWithPkAdded.Contains(tableNeedingRestoration))
                                    {
                                        // Check if the referenced column is actually a PK in the target schema
                                        if (refColumn.IsPrimaryKey)
                                        {
                                            shouldRestore = true;
                                            restoreReason = $"references PK column {fk.RefColumn} in {tableNeedingRestoration} (PK restored)";
                                            MigrationLogger.Log($"      [MATCH] PK column condition met: IsPrimaryKey={refColumn.IsPrimaryKey}, tablesWithPkAdded.Contains={tablesWithPkAdded.Contains(tableNeedingRestoration)}");
                                        }
                                        else
                                        {
                                            // This FK references a table that had PK restored, but this specific column isn't a PK
                                            // This can happen if a table has multiple columns and only some are PKs
                                            MigrationLogger.Log($"      [NO MATCH - NOT PK] Referenced column {fk.RefColumn} is not a PK in {tableNeedingRestoration}");
                                        }
                                    }
                                    else if (tablesWithColumnTypeChanges.Contains(tableNeedingRestoration))
                                    {
                                        // Check if this column's type changed (including Rebuild changes)
                                        TableDiff? refTableDiff = diff.ModifiedTables.FirstOrDefault(t => t.TableName.Equals(tableNeedingRestoration, StringComparison.OrdinalIgnoreCase));
                                        if (refTableDiff != null)
                                        {
                                            foreach (ColumnChange change in refTableDiff.ColumnChanges)
                                            {
                                                if (change.NewColumn != null && change.OldColumn != null &&
                                                    change.NewColumn.Name.Equals(fk.RefColumn, StringComparison.OrdinalIgnoreCase))
                                                {
                                                    // Check for type changes OR rebuild operations
                                                    if (change.NewColumn.SqlType != change.OldColumn.SqlType || 
                                                        change.ChangeType == ColumnChangeType.Rebuild)
                                                    {
                                                        shouldRestore = true;
                                                        string changeType = change.ChangeType == ColumnChangeType.Rebuild 
                                                            ? "rebuilt" 
                                                            : $"type changed: {change.OldColumn.SqlType} -> {change.NewColumn.SqlType}";
                                                        restoreReason = $"references column {fk.RefColumn} in {tableNeedingRestoration} ({changeType})";
                                                        MigrationLogger.Log($"      [MATCH] Type change or rebuild condition met: {change.ChangeType}");
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    else
                                    {
                                        MigrationLogger.Log($"      [NO MATCH] IsPrimaryKey={refColumn.IsPrimaryKey}, tablesWithPkAdded.Contains={tablesWithPkAdded.Contains(tableNeedingRestoration)}, tablesWithColumnTypeChanges.Contains={tablesWithColumnTypeChanges.Contains(tableNeedingRestoration)}");
                                    }
                                }
                                else
                                {
                                    MigrationLogger.Log($"      [ERROR] Referenced column {fk.RefColumn} not found in {fk.RefTable}");
                                }
                                
                                if (shouldRestore)
                                {
                                    // Only restore if not already processed (not in diff changes)
                                    if (!processedFks.Contains(fk.Name))
                                    {
                                        MigrationLogger.Log($"  [RESTORE FK] {fk.Name} ({restoreReason})");
                                        
                                        // Group multi-column FKs
                                        List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { fk };
                                        // Find other columns with same FK name (multi-column FK)
                                        foreach (SqlTableColumn otherColumn in tablePair.Value.Columns.Values)
                                        {
                                            foreach (SqlForeignKey otherFk in otherColumn.ForeignKeys)
                                            {
                                                if (otherFk.Name == fk.Name && otherFk.Table == fk.Table && otherFk != fk)
                                                {
                                                    fkGroup.Add(otherFk);
                                                }
                                            }
                                        }
                                        
                                        // Generate FK statement (after PK/column modifications are done, so it's safe)
                                        bool wasNoCheck = fkGroup[0].NotEnforced;
                                        string fkSql = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDict, forceNoCheck: true);
                                        sb.Append(fkSql);
                                        sb.AppendLine();
                                        
                                        // Restore CHECK state if needed
                                        if (!wasNoCheck)
                                        {
                                            SqlForeignKey firstFk = fkGroup[0];
                                            // Use unique suffix to avoid conflicts with Phase 0 FK drops
                                            string uniqueSuffix = MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(firstFk.Schema, firstFk.Table, firstFk.Name, "phase3check");
                                            sb.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(firstFk, uniqueSuffix));
                                            string fkSqlWithCheck = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDict, forceNoCheck: false);
                                            if (!string.IsNullOrEmpty(fkSqlWithCheck))
                                            {
                                                sb.Append(fkSqlWithCheck);
                                                sb.AppendLine();
                                            }
                                        }
                                        
                                        processedFks.Add(fk.Name);
                                    }
                                    else
                                    {
                                        MigrationLogger.Log($"  [SKIP FK] {fk.Name} (already processed)");
                                    }
                                }
                            }
                            else
                            {
                                MigrationLogger.Log($"    [ERROR] Referenced table {fk.RefTable} not found in target schema");
                            }
                        }
                    }
                }
            }
        }

        // Handle index changes (drop modified/dropped indexes, add new/modified indexes)
        // Drop indexes that are being dropped or modified (need to drop old before adding new)
        foreach (TableDiff tableDiff in diff.ModifiedTables.OrderBy(t => t.TableName))
        {
            foreach (IndexChange indexChange in tableDiff.IndexChanges)
            {
                if (indexChange.ChangeType == IndexChangeType.Drop && indexChange.OldIndex != null)
                {
                    sb.AppendLine(GenerateIndexes.GenerateDropIndexStatement(indexChange.OldIndex));
                }
                else if (indexChange.ChangeType == IndexChangeType.Modify && indexChange.OldIndex != null)
                {
                    // For modifications, drop the old index before adding the new one
                    sb.AppendLine(GenerateIndexes.GenerateDropIndexStatement(indexChange.OldIndex));
                }
            }
        }

        // Add indexes for new tables
        foreach (SqlTable newTable in diff.NewTables)
        {
            foreach (SqlIndex index in newTable.Indexes)
            {
                sb.Append(GenerateIndexes.GenerateCreateIndexStatement(index));
                sb.AppendLine();
            }
        }

        // Add new/modified indexes from modified tables
        foreach (TableDiff tableDiff in diff.ModifiedTables.OrderBy(t => t.TableName))
        {
            foreach (IndexChange indexChange in tableDiff.IndexChanges.Where(c => c.ChangeType == IndexChangeType.Add || c.ChangeType == IndexChangeType.Modify))
            {
                if (indexChange.NewIndex != null)
                {
                    sb.Append(GenerateIndexes.GenerateCreateIndexStatement(indexChange.NewIndex));
                    sb.AppendLine();
                }
            }
        }
        
        return sb.ToString().Trim();
    }

    private static Dictionary<string, SqlTable> BuildFkInspectionTables(
        ConcurrentDictionary<string, SqlTable> currentSchema,
        ConcurrentDictionary<string, SqlTable> targetSchema,
        SchemaDiff diff,
        out Dictionary<string, HashSet<string>> tableSources)
    {
        Dictionary<string, SqlTable> result = new Dictionary<string, SqlTable>(StringComparer.OrdinalIgnoreCase);
        Dictionary<string, HashSet<string>> localTableSources = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

        // Helper to check if FK references valid tables/columns in target schema
        bool IsFkValid(SqlForeignKey fk)
        {
            if (!targetSchema.TryGetValue(fk.RefTable.ToLowerInvariant(), out SqlTable? refTable))
            {
                MigrationLogger.Log($"    [FK VALIDATION] FK {fk.Name}: Referenced table {fk.RefTable} not found in target schema");
                return false;
            }

            if (!refTable.Columns.TryGetValue(fk.RefColumn.ToLowerInvariant(), out _))
            {
                MigrationLogger.Log($"    [FK VALIDATION] FK {fk.Name}: Referenced column {fk.RefTable}.{fk.RefColumn} not found in target schema");
                return false;
            }

            return true;
        }

        // Helper to check if two FKs are fully equal (not just name+column)
        bool AreFksFullyEqual(SqlForeignKey fk1, SqlForeignKey fk2)
        {
            return fk1.Name.Equals(fk2.Name, StringComparison.OrdinalIgnoreCase) &&
                   fk1.Table.Equals(fk2.Table, StringComparison.OrdinalIgnoreCase) &&
                   fk1.Column.Equals(fk2.Column, StringComparison.OrdinalIgnoreCase) &&
                   fk1.RefTable.Equals(fk2.RefTable, StringComparison.OrdinalIgnoreCase) &&
                   fk1.RefColumn.Equals(fk2.RefColumn, StringComparison.OrdinalIgnoreCase) &&
                   fk1.NotEnforced == fk2.NotEnforced &&
                   fk1.NotForReplication == fk2.NotForReplication &&
                   fk1.DeleteAction == fk2.DeleteAction &&
                   fk1.UpdateAction == fk2.UpdateAction;
        }

        // Helper to find FK by name in a column's FK list
        SqlForeignKey? FindFkByName(List<SqlForeignKey> fks, string fkName)
        {
            return fks.FirstOrDefault(fk => fk.Name.Equals(fkName, StringComparison.OrdinalIgnoreCase));
        }

        // Step 1: Start with targetSchema as source of truth (desired final state)
        foreach (SqlTable targetTable in targetSchema.Values)
        {
            string tableKey = targetTable.Name.ToLowerInvariant();
            localTableSources[tableKey] = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "target" };

            Dictionary<string, SqlTableColumn> clonedColumns = new Dictionary<string, SqlTableColumn>(targetTable.Columns.Count, StringComparer.OrdinalIgnoreCase);
            foreach (KeyValuePair<string, SqlTableColumn> columnPair in targetTable.Columns)
            {
                // Only include FKs that reference valid tables/columns
                List<SqlForeignKey> validFks = columnPair.Value.ForeignKeys
                    .Where(fk => IsFkValid(fk))
                    .ToList();

                clonedColumns[columnPair.Key] = columnPair.Value with
                {
                    ForeignKeys = validFks
                };
            }

            List<SqlIndex> clonedIndexes = new List<SqlIndex>(targetTable.Indexes);
            result[tableKey] = new SqlTable(targetTable.Name, clonedColumns, clonedIndexes, targetTable.Schema);
        }

        // Step 2: Fill gaps from currentSchema - only add FKs that don't exist in targetSchema
        // This handles cases where FKs were temporarily dropped but need restoration
        foreach (SqlTable currentTable in currentSchema.Values)
        {
            string tableKey = currentTable.Name.ToLowerInvariant();

            if (!localTableSources.TryGetValue(tableKey, out HashSet<string>? sourceSet))
            {
                sourceSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                localTableSources[tableKey] = sourceSet;
            }
            sourceSet.Add("current");

            if (!result.TryGetValue(tableKey, out SqlTable? mergedTable))
            {
                // Table doesn't exist in target - clone from current (shouldn't happen, but safety)
                Dictionary<string, SqlTableColumn> clonedColumns = new Dictionary<string, SqlTableColumn>(currentTable.Columns.Count, StringComparer.OrdinalIgnoreCase);
                foreach (KeyValuePair<string, SqlTableColumn> columnPair in currentTable.Columns)
                {
                    List<SqlForeignKey> validFks = columnPair.Value.ForeignKeys
                        .Where(fk => IsFkValid(fk))
                        .ToList();

                    clonedColumns[columnPair.Key] = columnPair.Value with
                    {
                        ForeignKeys = validFks
                    };
                }

                List<SqlIndex> clonedIndexes = new List<SqlIndex>(currentTable.Indexes);
                result[tableKey] = new SqlTable(currentTable.Name, clonedColumns, clonedIndexes, currentTable.Schema);
                continue;
            }

            // Merge FKs from currentSchema that don't exist in targetSchema
            foreach (KeyValuePair<string, SqlTableColumn> columnPair in currentTable.Columns)
            {
                string columnKey = columnPair.Key.ToLowerInvariant();
                if (!mergedTable.Columns.TryGetValue(columnKey, out SqlTableColumn? mergedColumn))
                {
                    // Column doesn't exist in target - add it (shouldn't happen, but safety)
                    List<SqlForeignKey> validFks = columnPair.Value.ForeignKeys
                        .Where(fk => IsFkValid(fk))
                        .ToList();

                    mergedTable.Columns[columnKey] = columnPair.Value with
                    {
                        ForeignKeys = validFks
                    };
                    continue;
                }

                // Check each FK from currentSchema
                foreach (SqlForeignKey currentFk in columnPair.Value.ForeignKeys)
                {
                    // Skip if FK is not valid (references non-existent tables/columns)
                    if (!IsFkValid(currentFk))
                    {
                        continue;
                    }

                    // Check if FK already exists in merged result (from targetSchema)
                    SqlForeignKey? existingFk = FindFkByName(mergedColumn.ForeignKeys, currentFk.Name);
                    
                    if (existingFk == null)
                    {
                        // FK doesn't exist in targetSchema - add it from currentSchema
                        // This handles temporarily dropped FKs that need restoration
                        mergedColumn.ForeignKeys.Add(currentFk);
                        MigrationLogger.Log($"    [FK MERGE] Added FK {currentFk.Name} from currentSchema (not in targetSchema)");
                    }
                    else if (!AreFksFullyEqual(existingFk, currentFk))
                    {
                        // FK exists but definitions differ - targetSchema wins (already in mergedColumn)
                        MigrationLogger.Log($"    [FK MERGE] FK {currentFk.Name} differs between current and target - using target definition");
                    }
                    // If FKs are fully equal, no action needed (targetSchema already has it)
                }
            }
        }

        // Step 3: Process diff changes - respect explicit FK modifications
        HashSet<string> explicitlyDroppedFks = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (TableDiff tableDiff in diff.ModifiedTables)
        {
            foreach (ForeignKeyChange fkChange in tableDiff.ForeignKeyChanges)
            {
                if (fkChange.ChangeType == ForeignKeyChangeType.Drop && fkChange.OldForeignKey != null)
                {
                    // FK is explicitly dropped - don't restore it
                    explicitlyDroppedFks.Add(fkChange.OldForeignKey.Name);
                    MigrationLogger.Log($"    [FK DIFF] FK {fkChange.OldForeignKey.Name} explicitly dropped - will not restore");
                    continue;
                }

                SqlForeignKey? fkToAdd = null;
                if (fkChange.ChangeType == ForeignKeyChangeType.Modify)
                {
                    // For Modify, use NewForeignKey (the desired state)
                    fkToAdd = fkChange.NewForeignKey;
                }
                else if (fkChange.ChangeType == ForeignKeyChangeType.Add)
                {
                    // For Add, use NewForeignKey
                    fkToAdd = fkChange.NewForeignKey;
                }
                // Drop is handled above

                if (fkToAdd == null)
                {
                    continue;
                }

                // Validate FK references exist
                if (!IsFkValid(fkToAdd))
                {
                    MigrationLogger.Log($"    [FK DIFF] Skipping FK {fkToAdd.Name} from diff - invalid references");
                    continue;
                }

                string tableKey = fkToAdd.Table.ToLowerInvariant();
                if (!result.TryGetValue(tableKey, out SqlTable? mergedTable))
                {
                    // Table doesn't exist - skip
                    continue;
                }

                if (!mergedTable.Columns.TryGetValue(fkToAdd.Column.ToLowerInvariant(), out SqlTableColumn? mergedColumn))
                {
                    // Column doesn't exist - skip
                    continue;
                }

                // Check if FK already exists
                SqlForeignKey? existingFk = FindFkByName(mergedColumn.ForeignKeys, fkToAdd.Name);
                
                if (existingFk == null)
                {
                    // FK doesn't exist - add it
                    mergedColumn.ForeignKeys.Add(fkToAdd);
                    MigrationLogger.Log($"    [FK DIFF] Added FK {fkToAdd.Name} from diff changes");
                }
                else if (!AreFksFullyEqual(existingFk, fkToAdd))
                {
                    // FK exists but differs - replace with diff version (diff takes precedence)
                    mergedColumn.ForeignKeys.Remove(existingFk);
                    mergedColumn.ForeignKeys.Add(fkToAdd);
                    MigrationLogger.Log($"    [FK DIFF] Replaced FK {fkToAdd.Name} with diff version");
                }
            }
        }

        // Step 4: Remove explicitly dropped FKs from result
        foreach (SqlTable table in result.Values)
        {
            foreach (SqlTableColumn column in table.Columns.Values)
            {
                column.ForeignKeys.RemoveAll(fk => explicitlyDroppedFks.Contains(fk.Name));
            }
        }

        tableSources = localTableSources;
        return result;
    }
}
