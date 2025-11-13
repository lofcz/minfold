using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Up;

public static class GeneratePhase3Constraints
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable> targetSchema)
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
        foreach (KeyValuePair<string, List<ForeignKeyChange>> fkGroup in fkGroups.OrderBy(g => g.Key))
        {
            ForeignKeyChange firstChange = fkGroup.Value[0];
            if (firstChange.ChangeType == ForeignKeyChangeType.Drop && firstChange.OldForeignKey != null)
            {
                sb.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(firstChange.OldForeignKey));
            }
            else if (firstChange.ChangeType == ForeignKeyChangeType.Modify && firstChange.OldForeignKey != null)
            {
                // For modifications, drop the old FK before adding the new one
                sb.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(firstChange.OldForeignKey));
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
                sb.AppendLine($"ALTER TABLE [{firstFk.Schema}].[{firstFk.Table}] DROP CONSTRAINT [{firstFk.Name}];");
                
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
            // Check if any columns are gaining PK status or being rebuilt with PK
            bool needsPkRestored = false;
            foreach (ColumnChange change in tableDiff.ColumnChanges)
            {
                if (change.NewColumn != null && change.NewColumn.IsPrimaryKey)
                {
                    if (change.ChangeType == ColumnChangeType.Add || 
                        (change.ChangeType == ColumnChangeType.Modify && change.OldColumn != null && !change.OldColumn.IsPrimaryKey) ||
                        (change.ChangeType == ColumnChangeType.Rebuild && change.OldColumn != null && change.OldColumn.IsPrimaryKey))
                    {
                        // PK needs to be restored if:
                        // 1. New column with PK (Add)
                        // 2. Column gaining PK status (Modify: OldColumn not PK, NewColumn is PK)
                        // 3. Column being rebuilt that was already PK (Rebuild: OldColumn was PK, NewColumn is PK)
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
        foreach (string tableWithPkRestored in tablesWithPkAdded)
        {
            // Find all FKs in target schema that reference this table's PK columns
            foreach (KeyValuePair<string, SqlTable> tablePair in targetSchema)
            {
                foreach (SqlTableColumn column in tablePair.Value.Columns.Values)
                {
                    foreach (SqlForeignKey fk in column.ForeignKeys)
                    {
                        // Check if this FK references the table whose PK was restored
                        if (fk.RefTable.ToLowerInvariant() == tableWithPkRestored)
                        {
                            // Verify the FK references a PK column
                            if (targetSchema.TryGetValue(fk.RefTable.ToLowerInvariant(), out SqlTable? refTable))
                            {
                                if (refTable.Columns.TryGetValue(fk.RefColumn.ToLowerInvariant(), out SqlTableColumn? refColumn) && refColumn.IsPrimaryKey)
                                {
                                    // Only restore if not already processed (not in diff changes)
                                    if (!processedFks.Contains(fk.Name))
                                    {
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
                                        
                                        // Generate FK statement (after PK is restored, so it's safe)
                                        bool wasNoCheck = fkGroup[0].NotEnforced;
                                        string fkSql = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDict, forceNoCheck: true);
                                        sb.Append(fkSql);
                                        sb.AppendLine();
                                        
                                        // Restore CHECK state if needed
                                        if (!wasNoCheck)
                                        {
                                            SqlForeignKey firstFk = fkGroup[0];
                                            sb.AppendLine($"ALTER TABLE [{firstFk.Schema}].[{firstFk.Table}] DROP CONSTRAINT [{firstFk.Name}];");
                                            string fkSqlWithCheck = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDict, forceNoCheck: false);
                                            if (!string.IsNullOrEmpty(fkSqlWithCheck))
                                            {
                                                sb.Append(fkSqlWithCheck);
                                                sb.AppendLine();
                                            }
                                        }
                                        
                                        processedFks.Add(fk.Name);
                                    }
                                }
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
}

