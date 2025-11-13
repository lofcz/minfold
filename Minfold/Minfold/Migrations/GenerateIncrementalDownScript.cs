using System.Collections.Concurrent;
using System.Text;

namespace Minfold;

public static class GenerateIncrementalDownScript
{
    public static string Generate(
        SchemaDiff downDiff,
        ConcurrentDictionary<string, SqlTable> currentSchema,
        ConcurrentDictionary<string, SqlSequence> currentSequences,
        ConcurrentDictionary<string, SqlStoredProcedure> currentProcedures,
        ConcurrentDictionary<string, SqlTable> targetSchema,
        ConcurrentDictionary<string, SqlSequence> targetSequences,
        ConcurrentDictionary<string, SqlStoredProcedure> targetProcedures)
    {
        // Generate down script using downDiff (reverse of upDiff)
        // Transaction is managed by MigrationApplier.ExecuteMigrationScript using ADO.NET transactions
        StringBuilder downScript = new StringBuilder();
        downScript.AppendLine("-- Generated using Minfold, do not edit manually");
        downScript.AppendLine("SET XACT_ABORT ON;");
        downScript.AppendLine();

        SchemaDiff diff = downDiff;

        // Reverse index changes
        // For Modify: OldIndex = current state (after migration), NewIndex = target state (before migration)
        // For Add: NewIndex = from targetSchema (was dropped by migration) → should be ADDED back
        // For Drop: OldIndex = from currentSchema (was added by migration) → should be DROPPED
        foreach (TableDiff tableDiff in diff.ModifiedTables.OrderByDescending(t => t.TableName))
        {
            foreach (IndexChange indexChange in tableDiff.IndexChanges.Where(c => c.ChangeType == IndexChangeType.Modify))
            {
                // Drop current index (OldIndex) and add back target index (NewIndex)
                if (indexChange.OldIndex != null)
                {
                    downScript.AppendLine(GenerateIndexes.GenerateDropIndexStatement(indexChange.OldIndex));
                }
                if (indexChange.NewIndex != null)
                {
                    downScript.Append(GenerateIndexes.GenerateCreateIndexStatement(indexChange.NewIndex));
                    downScript.AppendLine();
                }
            }
            
            // Drop indexes that were added by the migration
            // Drop changes: index exists in currentSchema (after migration) but not in targetSchema (before migration)
            foreach (IndexChange indexChange in tableDiff.IndexChanges.Where(c => c.ChangeType == IndexChangeType.Drop))
            {
                if (indexChange.OldIndex != null)
                {
                    downScript.AppendLine(GenerateIndexes.GenerateDropIndexStatement(indexChange.OldIndex));
                }
            }
            
            // Add back indexes that were dropped by the migration
            // Add changes: index exists in targetSchema (before migration) but not in currentSchema (after migration)
            foreach (IndexChange indexChange in tableDiff.IndexChanges.Where(c => c.ChangeType == IndexChangeType.Add))
            {
                if (indexChange.NewIndex != null)
                {
                    downScript.Append(GenerateIndexes.GenerateCreateIndexStatement(indexChange.NewIndex));
                    downScript.AppendLine();
                }
            }
        }

        // Reverse PRIMARY KEY changes - drop PKs first (before restoring columns)
        // We need to drop PK if:
        // 1. Any columns gained PK status (new PK columns)
        // 2. Any columns that are currently PKs need to be modified or dropped (losing PK status)
        // We'll add back old PKs after restoring columns
        StringBuilder pkRestoreScript = new StringBuilder(); // Store PK restorations for after column restoration
        foreach (TableDiff tableDiff in diff.ModifiedTables.OrderByDescending(t => t.TableName))
        {
            bool needsPkDropped = false;
            
            // Check if any columns gained PK status (need to drop new PK)
            foreach (ColumnChange change in tableDiff.ColumnChanges)
            {
                if (change.NewColumn != null && change.NewColumn.IsPrimaryKey)
                {
                    if (change.ChangeType == ColumnChangeType.Add || 
                        (change.ChangeType == ColumnChangeType.Modify && change.OldColumn != null && !change.OldColumn.IsPrimaryKey))
                    {
                        needsPkDropped = true;
                        break;
                    }
                }
            }
            
            // Also check if any columns that are currently PKs need to be modified or dropped
            // For dropped columns: OldColumn is from current schema (after migration), so check OldColumn.IsPrimaryKey
            // For modified columns: NewColumn is from current schema (after migration), so check if it's a PK that will lose PK status
            if (!needsPkDropped)
            {
                foreach (ColumnChange change in tableDiff.ColumnChanges)
                {
                    // Check if a column that is currently a PK is being dropped
                    // OldColumn represents the current state (after migration), so check if it's a PK
                    if (change.ChangeType == ColumnChangeType.Drop && change.OldColumn != null && change.OldColumn.IsPrimaryKey)
                    {
                        needsPkDropped = true;
                        break;
                    }
                    // Check if a column that is currently a PK is being modified to lose PK status
                    // NewColumn represents the current state (after migration)
                    if (change.ChangeType == ColumnChangeType.Modify && change.NewColumn != null && change.NewColumn.IsPrimaryKey && 
                        change.OldColumn != null && !change.OldColumn.IsPrimaryKey)
                    {
                        needsPkDropped = true;
                        break;
                    }
                }
            }
            
            if (needsPkDropped)
            {
                // Get current PK columns from schema after changes to drop
                ConcurrentDictionary<string, SqlTable> schemaAfterChanges = MigrationSchemaSnapshot.ApplySchemaDiffToTarget(targetSchema, diff);
                if (schemaAfterChanges.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? newTable))
                {
                    List<SqlTableColumn> newPkColumns = newTable.Columns.Values.Where(c => c.IsPrimaryKey).OrderBy(c => c.OrdinalPosition).ToList();
                    if (newPkColumns.Count > 0)
                    {
                        string pkConstraintName = $"PK_{tableDiff.TableName}";
                        downScript.AppendLine(GeneratePrimaryKeys.GenerateDropPrimaryKeyStatement(tableDiff.TableName, pkConstraintName, newTable.Schema));
                    }
                }
            }
            
            // Store PK restoration for after column restoration
            // We need to restore PK if the target schema (before migration) had columns as PKs
            // For Drop: NewColumn is null, so check if OldColumn (current state) was a PK that needs to be restored
            //   Actually, for Drop: OldColumn is from current schema, NewColumn is null. We need to check targetSchema.
            // For Modify: NewColumn is from target schema (what we want), so check NewColumn.IsPrimaryKey
            bool needsPkRestored = false;
            HashSet<string> columnsThatLostPk = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            
            foreach (ColumnChange change in tableDiff.ColumnChanges)
            {
                if (change.ChangeType == ColumnChangeType.Drop)
                {
                    // For dropped columns, check if they were PKs in the target schema (before migration)
                    // We need to check the target schema directly since NewColumn is null
                    if (targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTable))
                    {
                        if (targetTable.Columns.TryGetValue(change.OldColumn?.Name.ToLowerInvariant() ?? "", out SqlTableColumn? targetColumn) && targetColumn.IsPrimaryKey)
                        {
                            needsPkRestored = true;
                            columnsThatLostPk.Add(change.OldColumn!.Name);
                        }
                    }
                }
                else if (change.ChangeType == ColumnChangeType.Modify && change.NewColumn != null)
                {
                    // NewColumn is from target schema (what we want to restore to)
                    // If it was a PK in the target schema, we need to restore it
                    if (change.NewColumn.IsPrimaryKey)
                    {
                        needsPkRestored = true;
                        columnsThatLostPk.Add(change.NewColumn.Name);
                    }
                }
            }
            
            if (needsPkRestored)
            {
                // Get all original PK columns from target schema
                if (targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTable))
                {
                    List<SqlTableColumn> originalPkColumns = targetTable.Columns.Values.Where(c => c.IsPrimaryKey).OrderBy(c => c.OrdinalPosition).ToList();
                    if (originalPkColumns.Count > 0)
                    {
                        List<string> pkColumnNames = originalPkColumns.Select(c => c.Name).ToList();
                        string pkConstraintName = $"PK_{tableDiff.TableName}";
                        pkRestoreScript.Append(GeneratePrimaryKeys.GenerateAddPrimaryKeyStatement(tableDiff.TableName, pkColumnNames, pkConstraintName, targetTable.Schema));
                    }
                }
            }
        }

        // Reverse FK changes
        // Collect FK changes from modified tables for down script
        List<ForeignKeyChange> allFkChanges = new List<ForeignKeyChange>();
        foreach (TableDiff tableDiff in diff.ModifiedTables)
        {
            allFkChanges.AddRange(tableDiff.ForeignKeyChanges);
        }
        
        // Also collect FKs from dropped tables (they need to be dropped before the table is dropped)
        // When a table is dropped, FKs referencing it are also dropped, but FKs ON the dropped table need to be dropped first
        foreach (string droppedTableName in diff.DroppedTableNames)
        {
            if (currentSchema != null && 
                currentSchema.TryGetValue(droppedTableName.ToLowerInvariant(), out SqlTable? droppedTable))
            {
                foreach (SqlTableColumn column in droppedTable.Columns.Values)
                {
                    foreach (SqlForeignKey fk in column.ForeignKeys)
                    {
                        // Add as a Drop change (FK exists in current schema but will be dropped)
                        allFkChanges.Add(new ForeignKeyChange(ForeignKeyChangeType.Drop, fk, null));
                    }
                }
            }
        }
        
        // Also collect FKs that reference dropped tables (they need to be dropped before the referenced table is dropped)
        foreach (string droppedTableName in diff.DroppedTableNames)
        {
            foreach (TableDiff tableDiff in diff.ModifiedTables)
            {
                foreach (SqlTableColumn column in (currentSchema?[tableDiff.TableName.ToLowerInvariant()]?.Columns.Values ?? Enumerable.Empty<SqlTableColumn>()))
                {
                    foreach (SqlForeignKey fk in column.ForeignKeys)
                    {
                        if (fk.RefTable.Equals(droppedTableName, StringComparison.OrdinalIgnoreCase))
                        {
                            // FK references a dropped table, need to drop it
                            if (!allFkChanges.Any(c => c.OldForeignKey?.Name == fk.Name && c.OldForeignKey?.Table == fk.Table))
                            {
                                allFkChanges.Add(new ForeignKeyChange(ForeignKeyChangeType.Drop, fk, null));
                            }
                        }
                    }
                }
            }
        }
        
        MigrationLogger.Log($"\n=== FK Changes for down script ===");
        MigrationLogger.Log($"Total FK changes: {allFkChanges.Count}");
        foreach (var fkChange in allFkChanges)
        {
            MigrationLogger.Log($"  ChangeType: {fkChange.ChangeType}, FK: {fkChange.OldForeignKey?.Name ?? fkChange.NewForeignKey?.Name ?? "null"}");
        }
        
        // Drop FKs that need to be dropped or modified
        // For Modify: drop the new FK (current state after migration)
        foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Modify))
        {
            if (fkChange.NewForeignKey != null)
            {
                MigrationLogger.Log($"  [DROP FK] {fkChange.NewForeignKey.Name} (Modify)");
                downScript.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(fkChange.NewForeignKey));
            }
        }
        
        // Drop added FKs (were added by migration, need to drop in down script)
        foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Add))
        {
            if (fkChange.NewForeignKey != null)
            {
                MigrationLogger.Log($"  [DROP FK] {fkChange.NewForeignKey.Name} (Add)");
                downScript.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(fkChange.NewForeignKey));
            }
        }
        
        // Drop FKs that reference dropped tables (must be dropped before the referenced table)
        foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Drop))
        {
            if (fkChange.OldForeignKey != null)
            {
                // Check if this FK references a dropped table
                bool referencesDroppedTable = diff.DroppedTableNames.Any(name => 
                    name.Equals(fkChange.OldForeignKey.RefTable, StringComparison.OrdinalIgnoreCase));
                
                if (referencesDroppedTable)
                {
                    MigrationLogger.Log($"  [DROP FK] {fkChange.OldForeignKey.Name} (references dropped table {fkChange.OldForeignKey.RefTable})");
                    downScript.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(fkChange.OldForeignKey));
                }
            }
        }
        
        // Collect all FKs to be restored (from Modify and Drop changes)
        // Use NOCHECK → CHECK pattern to handle circular dependencies and preserve NotEnforced state
        // IMPORTANT: Get the original FK state from targetSchema (before migration), not from OldForeignKey (after migration)
        // IMPORTANT: Only restore FKs that existed before the migration (Modify/Drop), NOT FKs that were added (Add)
        List<(List<SqlForeignKey> FkGroup, bool WasNoCheck)> fksToRestore = new List<(List<SqlForeignKey>, bool)>();
        HashSet<string> processedFksDown = new HashSet<string>();
        
        // Track FKs that were added (should NOT be restored, only dropped)
        HashSet<string> addedFkNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Add))
        {
            if (fkChange.NewForeignKey != null)
            {
                addedFkNames.Add(fkChange.NewForeignKey.Name);
            }
        }
        
        // Collect FKs from Modify changes (restore old FK)
        foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Modify))
        {
            if (fkChange.OldForeignKey != null && 
                !processedFksDown.Contains(fkChange.OldForeignKey.Name) &&
                !addedFkNames.Contains(fkChange.OldForeignKey.Name))
            {
                // Get the original FK from targetSchema to get correct NotEnforced state
                // IMPORTANT: If FK doesn't exist in targetSchema, it was added in the up migration and shouldn't be restored
                SqlForeignKey? originalFk = null;
                if (targetSchema.TryGetValue(fkChange.OldForeignKey.Table.ToLowerInvariant(), out SqlTable? targetTable))
                {
                    foreach (SqlTableColumn column in targetTable.Columns.Values)
                    {
                        foreach (SqlForeignKey fk in column.ForeignKeys)
                        {
                            if (fk.Name.Equals(fkChange.OldForeignKey.Name, StringComparison.OrdinalIgnoreCase))
                            {
                                originalFk = fk;
                                break;
                            }
                        }
                        if (originalFk != null) break;
                    }
                }
                
                // Skip if FK doesn't exist in targetSchema (it was added in the up migration, not modified)
                if (originalFk == null)
                {
                    continue;
                }
                
                // Use original FK from targetSchema if found, otherwise fall back to OldForeignKey
                SqlForeignKey fkToUse = originalFk ?? fkChange.OldForeignKey;
                
                // Group multi-column FKs (ensure no duplicate columns)
                List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { fkToUse };
                HashSet<string> addedColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { fkToUse.Column };
                foreach (ForeignKeyChange otherFkChange in allFkChanges)
                {
                    if (otherFkChange.OldForeignKey != null &&
                        otherFkChange.OldForeignKey.Name == fkChange.OldForeignKey.Name &&
                        otherFkChange.OldForeignKey.Table == fkChange.OldForeignKey.Table &&
                        !addedColumns.Contains(otherFkChange.OldForeignKey.Column))
                    {
                        // Try to get original FK for this one too
                        SqlForeignKey? otherOriginalFk = null;
                        if (targetSchema.TryGetValue(otherFkChange.OldForeignKey.Table.ToLowerInvariant(), out SqlTable? otherTargetTable))
                        {
                            foreach (SqlTableColumn column in otherTargetTable.Columns.Values)
                            {
                                foreach (SqlForeignKey fk in column.ForeignKeys)
                                {
                                    if (fk.Name.Equals(otherFkChange.OldForeignKey.Name, StringComparison.OrdinalIgnoreCase) &&
                                        fk.Column.Equals(otherFkChange.OldForeignKey.Column, StringComparison.OrdinalIgnoreCase))
                                    {
                                        otherOriginalFk = fk;
                                        break;
                                    }
                                }
                                if (otherOriginalFk != null) break;
                            }
                        }
                        SqlForeignKey fkToAdd = otherOriginalFk ?? otherFkChange.OldForeignKey;
                        fkGroup.Add(fkToAdd);
                        addedColumns.Add(fkToAdd.Column);
                    }
                }
                // Store original NotEnforced state from targetSchema FK
                bool wasNoCheck = fkGroup[0].NotEnforced;
                fksToRestore.Add((fkGroup, wasNoCheck));
                processedFksDown.Add(fkChange.OldForeignKey.Name);
            }
        }
        
        // Collect FKs from Drop changes (restore dropped FK)
        foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Drop))
        {
            if (fkChange.OldForeignKey != null && 
                !processedFksDown.Contains(fkChange.OldForeignKey.Name) &&
                !addedFkNames.Contains(fkChange.OldForeignKey.Name))
            {
                // Get the original FK from targetSchema to get correct NotEnforced state
                // IMPORTANT: If FK doesn't exist in targetSchema, it was added in the up migration and shouldn't be restored
                SqlForeignKey? originalFk = null;
                if (targetSchema.TryGetValue(fkChange.OldForeignKey.Table.ToLowerInvariant(), out SqlTable? targetTable))
                {
                    foreach (SqlTableColumn column in targetTable.Columns.Values)
                    {
                        foreach (SqlForeignKey fk in column.ForeignKeys)
                        {
                            if (fk.Name.Equals(fkChange.OldForeignKey.Name, StringComparison.OrdinalIgnoreCase))
                            {
                                originalFk = fk;
                                break;
                            }
                        }
                        if (originalFk != null) break;
                    }
                }
                
                // Skip if FK doesn't exist in targetSchema (it was added in the up migration, not dropped)
                if (originalFk == null)
                {
                    continue;
                }
                
                // Use original FK from targetSchema if found, otherwise fall back to OldForeignKey
                SqlForeignKey fkToUse = originalFk ?? fkChange.OldForeignKey;
                
                // Group multi-column FKs (ensure no duplicate columns)
                List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { fkToUse };
                HashSet<string> addedColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { fkToUse.Column };
                foreach (ForeignKeyChange otherFkChange in allFkChanges)
                {
                    if (otherFkChange.OldForeignKey != null &&
                        otherFkChange.OldForeignKey.Name == fkChange.OldForeignKey.Name &&
                        otherFkChange.OldForeignKey.Table == fkChange.OldForeignKey.Table &&
                        !addedColumns.Contains(otherFkChange.OldForeignKey.Column))
                    {
                        // Try to get original FK for this one too
                        SqlForeignKey? otherOriginalFk = null;
                        if (targetSchema.TryGetValue(otherFkChange.OldForeignKey.Table.ToLowerInvariant(), out SqlTable? otherTargetTable))
                        {
                            foreach (SqlTableColumn column in otherTargetTable.Columns.Values)
                            {
                                foreach (SqlForeignKey fk in column.ForeignKeys)
                                {
                                    if (fk.Name.Equals(otherFkChange.OldForeignKey.Name, StringComparison.OrdinalIgnoreCase) &&
                                        fk.Column.Equals(otherFkChange.OldForeignKey.Column, StringComparison.OrdinalIgnoreCase))
                                    {
                                        otherOriginalFk = fk;
                                        break;
                                    }
                                }
                                if (otherOriginalFk != null) break;
                            }
                        }
                        SqlForeignKey fkToAdd = otherOriginalFk ?? otherFkChange.OldForeignKey;
                        fkGroup.Add(fkToAdd);
                        addedColumns.Add(fkToAdd.Column);
                    }
                }
                // Store original NotEnforced state from targetSchema FK
                bool wasNoCheck = fkGroup[0].NotEnforced;
                fksToRestore.Add((fkGroup, wasNoCheck));
                processedFksDown.Add(fkChange.OldForeignKey.Name);
            }
        }
        
        // Create all FKs with NOCHECK first (avoids circular dependency issues and reduces lock time)
        Dictionary<string, SqlTable> tablesDictDown = new Dictionary<string, SqlTable>(targetSchema);
        foreach (var (fkGroup, wasNoCheck) in fksToRestore.OrderBy(g => g.FkGroup[0].Table).ThenBy(g => g.FkGroup[0].Name))
        {
            // Force NOCHECK during creation to avoid circular dependency issues
            string fkSql = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDictDown, forceNoCheck: true);
            downScript.Append(fkSql);
            downScript.AppendLine();
        }
        
        // Restore CHECK state for FKs that weren't originally NOCHECK
        // IMPORTANT: CHECK CONSTRAINT doesn't always restore is_not_trusted correctly after WITH NOCHECK
        // So we need to drop and recreate the FK with WITH CHECK to ensure correct NotEnforced state
        foreach (var (fkGroup, wasNoCheck) in fksToRestore.OrderBy(g => g.FkGroup[0].Table).ThenBy(g => g.FkGroup[0].Name))
        {
            if (!wasNoCheck)
            {
                SqlForeignKey firstFk = fkGroup[0];
                // Drop the FK that was created with NOCHECK
                downScript.AppendLine($"ALTER TABLE [{firstFk.Schema}].[{firstFk.Table}] DROP CONSTRAINT [{firstFk.Name}];");
                
                // Recreate it with WITH CHECK to ensure correct NotEnforced state
                string fkSqlWithCheck = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDictDown, forceNoCheck: false);
                if (!string.IsNullOrEmpty(fkSqlWithCheck))
                {
                    downScript.Append(fkSqlWithCheck);
                    downScript.AppendLine();
                }
            }
        }

        // Reverse column modifications (in reverse order)
        // Important: Drop added identity columns BEFORE restoring identity to modified columns
        // to avoid "Multiple identity columns" error
        // Track which columns we're adding to avoid duplicates
        // Also track which columns we're dropping to ensure we don't add a column we just dropped
        HashSet<string> columnsBeingAdded = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        HashSet<string> columnsBeingDropped = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        
        foreach (TableDiff tableDiff in diff.ModifiedTables.OrderByDescending(t => t.TableName))
        {
            // Log column changes for debugging
            MigrationLogger.Log($"\n=== Processing table: {tableDiff.TableName} ===");
            foreach (ColumnChange change in tableDiff.ColumnChanges)
            {
                MigrationLogger.Log($"  ChangeType: {change.ChangeType}, Column: {change.OldColumn?.Name ?? change.NewColumn?.Name ?? "null"}");
                if (change.OldColumn != null) MigrationLogger.Log($"    OldColumn: IsIdentity={change.OldColumn.IsIdentity}, IsPrimaryKey={change.OldColumn.IsPrimaryKey}");
                if (change.NewColumn != null) MigrationLogger.Log($"    NewColumn: IsIdentity={change.NewColumn.IsIdentity}, IsPrimaryKey={change.NewColumn.IsPrimaryKey}");
            }
            
            // Pre-calculate lists
            List<ColumnChange> dropChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Drop).ToList();
            List<ColumnChange> addChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Add).ToList();
            List<ColumnChange> modifyChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Modify).ToList();
            
            // Check if we have Modify changes that require DROP+ADD, and Drop changes that would leave the modified column as the only one
            // In this case, we need to process Modify BEFORE Drop to avoid the "only column" error
            bool needsModifyBeforeDrop = false;
            if (modifyChanges.Count > 0 && dropChanges.Count > 0 && currentSchema != null)
            {
                if (currentSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? currentTable))
                {
                    int currentDataColumnCount = currentTable.Columns.Values.Count(c => !c.IsComputed);
                    
                    // Check if any Modify change requires DROP+ADD and would be the only column after drops
                    foreach (ColumnChange modifyChange in modifyChanges)
                    {
                        if (modifyChange.OldColumn != null && modifyChange.NewColumn != null)
                        {
                            bool requiresDropAdd = (modifyChange.OldColumn.IsComputed || modifyChange.NewColumn.IsComputed) ||
                                                  (modifyChange.OldColumn.IsIdentity != modifyChange.NewColumn.IsIdentity);
                            
                            if (requiresDropAdd && currentDataColumnCount - dropChanges.Count == 1)
                            {
                                // After dropping other columns, this would be the only column
                                needsModifyBeforeDrop = true;
                                break;
                            }
                        }
                    }
                }
            }
            
            // First pass: Drop columns that were added by the migration
            // For Drop changes: column exists in currentSchema (after migration) but not in targetSchema (before migration)
            // This means the column was ADDED by the migration, so we need to DROP it during rollback
            // BUT: if we need to process Modify before Drop, skip this for now
            if (!needsModifyBeforeDrop)
            {
                foreach (ColumnChange change in dropChanges)
                {
                    if (change.OldColumn != null)
                    {
                        string columnKey = $"{tableDiff.TableName}.{change.OldColumn.Name}";
                        columnsBeingDropped.Add(columnKey);
                        MigrationLogger.Log($"  [DROP] {columnKey}");
                        downScript.AppendLine(GenerateColumns.GenerateDropColumnStatement(change.OldColumn.Name, tableDiff.TableName));
                    }
                }
            }

            // Second pass: Restore columns that were dropped by the migration AND reverse modifications
            // IMPORTANT: Interleave Add and Modify operations based on their original positions in targetSchema
            // to preserve column order. For example, if original order was id, col1, col2, col3:
            // - id (Modify) should be restored first
            // - col1, col2, col3 (Add) should be restored after id
            
            // Get target table to determine original column order
            if (!targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTable))
            {
                targetTable = null;
            }
            
            // Create a combined list of Add and Modify operations, ordered by original position
            List<(ColumnChange Change, int OriginalPosition, bool IsModify)> allRestoreOperations = new List<(ColumnChange, int, bool)>();
            
            // Add Modify operations (these restore modified columns)
            foreach (ColumnChange change in modifyChanges)
            {
                if (change.NewColumn != null && targetTable != null)
                {
                    if (targetTable.Columns.TryGetValue(change.NewColumn.Name.ToLowerInvariant(), out SqlTableColumn? targetCol))
                    {
                        allRestoreOperations.Add((change, targetCol.OrdinalPosition, true));
                    }
                }
            }
            
            // Add Add operations (these restore dropped columns)
            foreach (ColumnChange change in addChanges)
            {
                if (change.NewColumn != null && targetTable != null)
                {
                    if (targetTable.Columns.TryGetValue(change.NewColumn.Name.ToLowerInvariant(), out SqlTableColumn? targetCol))
                    {
                        allRestoreOperations.Add((change, targetCol.OrdinalPosition, false));
                    }
                }
            }
            
            // Sort by original position to preserve column order
            allRestoreOperations = allRestoreOperations.OrderBy(op => op.OriginalPosition).ToList();
            
            // Process operations in order, but handle Modify operations specially (they need DROP+ADD logic)
            foreach (var (change, originalPosition, isModify) in allRestoreOperations)
            {
                if (isModify)
                {
                    // Handle Modify operation (restore modified column)
                    if (change.OldColumn != null && change.NewColumn != null)
                    {
                        // For down script, we're restoring OldColumn, so use that for the key
                        string columnKey = $"{tableDiff.TableName}.{change.OldColumn.Name}";
                        
                        MigrationLogger.Log($"  [MODIFY] {columnKey}: OldColumn.IsIdentity={change.OldColumn.IsIdentity}, NewColumn.IsIdentity={change.NewColumn.IsIdentity}");
                        
                        // Get schema from current schema or default to dbo
                        string schema = "dbo";
                        if (currentSchema != null && currentSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? currentTable))
                        {
                            schema = currentTable.Schema;
                        }
                        
                        // Check if identity or computed property changed - SQL Server requires DROP + ADD
                        bool requiresDropAdd = (change.OldColumn.IsIdentity != change.NewColumn.IsIdentity) ||
                                               (change.OldColumn.IsComputed != change.NewColumn.IsComputed);
                        
                        if (requiresDropAdd)
                        {
                            string changeType = change.OldColumn.IsIdentity != change.NewColumn.IsIdentity ? "identity" : "computed";
                            MigrationLogger.Log($"    Reversing {changeType} change: {change.OldColumn.Name} -> {change.NewColumn.Name}");
                            
                            // If the column being dropped has a primary key constraint, drop it first
                            if (change.OldColumn.IsPrimaryKey)
                            {
                                string pkConstraintName = $"PK_{tableDiff.TableName}";
                                downScript.AppendLine(GeneratePrimaryKeys.GenerateDropPrimaryKeyStatement(tableDiff.TableName, pkConstraintName, schema));
                                MigrationLogger.Log($"    Dropped PK constraint before dropping column {change.OldColumn.Name}");
                            }
                            
                            // Create a reversed TableDiff for the safe wrapper to check column state
                            // In down script, currentSchema is the state after up migration (has OldColumn)
                            // We need to check if NewColumn would be the only column when we restore it
                            // If needsModifyBeforeDrop, include Drop changes in the diff so safe wrapper can detect single-column scenario
                            List<ColumnChange> reversedDiffChanges = new List<ColumnChange> { new ColumnChange(ColumnChangeType.Modify, change.OldColumn, change.NewColumn) };
                            if (needsModifyBeforeDrop)
                            {
                                // Include Drop changes so GenerateSafeColumnDropAndAdd can detect that this would be the only column
                                reversedDiffChanges.AddRange(dropChanges);
                            }
                            TableDiff reversedDiff = new TableDiff(
                                tableDiff.TableName,
                                reversedDiffChanges,
                                new List<ForeignKeyChange>(),
                                new List<IndexChange>()
                            );
                            
                            // Use currentSchema (state after up migration) to check if safe
                            // Drop OldColumn (current state) and add NewColumn (target state)
                            string safeDropAdd = GenerateColumns.GenerateSafeColumnDropAndAdd(
                                change.OldColumn,  // Drop this (current state after migration)
                                change.NewColumn,  // Add this (target state before migration)
                                tableDiff.TableName,
                                schema,
                                reversedDiff,
                                currentSchema
                            );
                            downScript.Append(safeDropAdd);
                            columnsBeingAdded.Add(columnKey); // Track that we've processed this Modify
                            MigrationLogger.Log($"    Added back {change.NewColumn.Name}");
                        }
                        else
                        {
                            // No identity/computed change, can use ALTER COLUMN
                            // Generate ALTER COLUMN statement to reverse the modification
                            string reverseAlter = GenerateColumns.GenerateAlterColumnStatement(change.OldColumn, change.NewColumn, tableDiff.TableName, schema);
                            if (!string.IsNullOrEmpty(reverseAlter))
                            {
                                downScript.Append(reverseAlter);
                                MigrationLogger.Log($"    Reversed column modification: {change.OldColumn.Name} -> {change.NewColumn.Name}");
                            }
                            columnsBeingAdded.Add(columnKey); // Track that we've processed this Modify
                        }
                    }
                }
                else
                {
                    // Handle Add operation (restore dropped column)
                    if (change.NewColumn != null)
                    {
                        string columnKey = $"{tableDiff.TableName}.{change.NewColumn.Name}";
                        // Only add if we haven't already added it and we haven't dropped it in this script
                        if (!columnsBeingAdded.Contains(columnKey) && !columnsBeingDropped.Contains(columnKey))
                        {
                            columnsBeingAdded.Add(columnKey);
                            MigrationLogger.Log($"  [ADD] {columnKey}");
                            downScript.Append(GenerateColumns.GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName));
                        }
                        else
                        {
                            MigrationLogger.Log($"  [SKIP ADD] {columnKey} (already added or being dropped)");
                        }
                    }
                }
            }

            // Third pass: Handle any remaining Modify operations that weren't processed above
            // (Modify operations should have been processed in second pass, but check for any missed ones)
            foreach (ColumnChange change in modifyChanges)
            {
                if (change.OldColumn != null && change.NewColumn != null)
                {
                    // For down script, we're restoring OldColumn, so use that for the key
                    string columnKey = $"{tableDiff.TableName}.{change.OldColumn.Name}";
                    
                    // Skip if already processed in second pass
                    if (columnsBeingAdded.Contains(columnKey))
                    {
                        continue;
                    }
                    
                    MigrationLogger.Log($"  [MODIFY] {columnKey}: OldColumn.IsIdentity={change.OldColumn.IsIdentity}, NewColumn.IsIdentity={change.NewColumn.IsIdentity}");
                    
                    // Get schema from current schema or default to dbo
                    string schema = "dbo";
                    if (currentSchema != null && currentSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? currentTable))
                    {
                        schema = currentTable.Schema;
                    }
                    
                    // Check if identity property changed - SQL Server requires DROP + ADD (same as up script)
                    if (change.OldColumn.IsIdentity != change.NewColumn.IsIdentity)
                    {
                        // For down script: reverse the change (from OldColumn back to NewColumn)
                        // Current state is OldColumn (after up migration), target is NewColumn (before up migration)
                        // We need to drop OldColumn (current) and add NewColumn (target)
                        MigrationLogger.Log($"    Reversing identity change: {change.OldColumn.Name} (IsIdentity={change.OldColumn.IsIdentity}) -> {change.NewColumn.Name} (IsIdentity={change.NewColumn.IsIdentity})");
                        
                        // If the column being dropped has a primary key constraint, drop it first
                        if (change.OldColumn.IsPrimaryKey)
                        {
                            string pkConstraintName = $"PK_{tableDiff.TableName}";
                            downScript.AppendLine(GeneratePrimaryKeys.GenerateDropPrimaryKeyStatement(tableDiff.TableName, pkConstraintName, schema));
                            MigrationLogger.Log($"    Dropped PK constraint before dropping column {change.OldColumn.Name}");
                        }
                        
                        // Create a reversed TableDiff for the safe wrapper to check column state
                        // In down script, currentSchema is the state after up migration (has OldColumn)
                        // We need to check if NewColumn would be the only column when we restore it
                        // If needsModifyBeforeDrop, include Drop changes in the diff so safe wrapper can detect single-column scenario
                        List<ColumnChange> reversedDiffChanges = new List<ColumnChange> { new ColumnChange(ColumnChangeType.Modify, change.OldColumn, change.NewColumn) };
                        if (needsModifyBeforeDrop)
                        {
                            // Include Drop changes so GenerateSafeColumnDropAndAdd can detect that this would be the only column
                            reversedDiffChanges.AddRange(dropChanges);
                        }
                        TableDiff reversedDiff = new TableDiff(
                            tableDiff.TableName,
                            reversedDiffChanges,
                            new List<ForeignKeyChange>(),
                            new List<IndexChange>()
                        );
                        
                        // Use currentSchema (state after up migration) to check if safe
                        // Drop OldColumn (current state) and add NewColumn (target state)
                        string safeDropAdd = GenerateColumns.GenerateSafeColumnDropAndAdd(
                            change.OldColumn,  // Drop this (current state after migration)
                            change.NewColumn,  // Add this (target state before migration)
                            tableDiff.TableName,
                            schema,
                            reversedDiff,
                            currentSchema
                        );
                        
                        if (columnsBeingAdded.Add(columnKey))
                        {
                            downScript.Append(safeDropAdd);
                            MigrationLogger.Log($"    Added back {change.OldColumn.Name}");
                        }
                        else
                        {
                            MigrationLogger.Log($"    Skipped adding {change.OldColumn.Name} (already added)");
                        }
                    }
                    // Check if it's a computed column change - these need DROP + ADD
                    else if (change.OldColumn.IsComputed || change.NewColumn.IsComputed)
                    {
                        // For down script: reverse the change (from OldColumn back to NewColumn)
                        // Drop OldColumn (current state) and add NewColumn (target state)
                        MigrationLogger.Log($"    Reversing computed column change: {change.OldColumn.Name} -> {change.NewColumn.Name}");
                        
                        // If the column being dropped has a primary key constraint, drop it first
                        if (change.OldColumn.IsPrimaryKey)
                        {
                            string pkConstraintName = $"PK_{tableDiff.TableName}";
                            downScript.AppendLine(GeneratePrimaryKeys.GenerateDropPrimaryKeyStatement(tableDiff.TableName, pkConstraintName, schema));
                            MigrationLogger.Log($"    Dropped PK constraint before dropping column {change.OldColumn.Name}");
                        }
                        
                        // If needsModifyBeforeDrop, include Drop changes in the diff so safe wrapper can detect single-column scenario
                        List<ColumnChange> reversedDiffChanges = new List<ColumnChange> { new ColumnChange(ColumnChangeType.Modify, change.OldColumn, change.NewColumn) };
                        if (needsModifyBeforeDrop)
                        {
                            // Include Drop changes so GenerateSafeColumnDropAndAdd can detect that this would be the only column
                            reversedDiffChanges.AddRange(dropChanges);
                        }
                        TableDiff reversedDiff = new TableDiff(
                            tableDiff.TableName,
                            reversedDiffChanges,
                            new List<ForeignKeyChange>(),
                            new List<IndexChange>()
                        );
                        
                        string safeDropAdd = GenerateColumns.GenerateSafeColumnDropAndAdd(
                            change.OldColumn,  // Drop this (current state after migration)
                            change.NewColumn,  // Add this (target state before migration)
                            tableDiff.TableName,
                            schema,
                            reversedDiff,
                            currentSchema
                        );
                        
                        if (columnsBeingAdded.Add(columnKey))
                        {
                            downScript.Append(safeDropAdd);
                        }
                    }
                    else
                    {
                        // Reverse the modification: change from OldColumn (current) to NewColumn (target)
                        string reverseAlter = GenerateColumns.GenerateAlterColumnStatement(change.OldColumn, change.NewColumn, tableDiff.TableName, schema);
                        if (!string.IsNullOrEmpty(reverseAlter))
                        {
                            downScript.Append(reverseAlter);
                        }
                    }
                }
            }
            
            // Fourth pass: If we skipped Drop changes earlier (needsModifyBeforeDrop), process them now
            if (needsModifyBeforeDrop)
            {
                foreach (ColumnChange change in dropChanges)
                {
                    if (change.OldColumn != null)
                    {
                        string columnKey = $"{tableDiff.TableName}.{change.OldColumn.Name}";
                        columnsBeingDropped.Add(columnKey);
                        MigrationLogger.Log($"  [DROP] {columnKey} (after Modify to avoid only-column error)");
                        downScript.AppendLine(GenerateColumns.GenerateDropColumnStatement(change.OldColumn.Name, tableDiff.TableName));
                    }
                }
            }
        }

        // Add back old PKs (after columns have been restored)
        downScript.Append(pkRestoreScript);

        // Recreate dropped tables (in forward order, so dependencies are created first)
        if (diff.DroppedTableNames.Count > 0)
        {
            MigrationLogger.Log($"\n=== Recreating dropped tables: {string.Join(", ", diff.DroppedTableNames)} ===");
        }
        foreach (string droppedTableName in diff.DroppedTableNames)
        {
            // Find the table schema from target schema (before migration was applied)
            if (targetSchema.TryGetValue(droppedTableName.ToLowerInvariant(), out SqlTable? droppedTable))
            {
                // Generate CREATE TABLE statement
                string createTableSql = GenerateTables.GenerateCreateTableStatement(droppedTable);
                downScript.AppendLine(createTableSql);
                downScript.AppendLine();
                
                // Generate FK constraints for the recreated table
                HashSet<string> processedFksDown1 = new HashSet<string>();
                foreach (SqlTableColumn column in droppedTable.Columns.Values)
                {
                    foreach (SqlForeignKey fk in column.ForeignKeys)
                    {
                        if (!processedFksDown1.Contains(fk.Name))
                        {
                            // Group multi-column FKs
                            List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { fk };
                            foreach (SqlTableColumn otherColumn in droppedTable.Columns.Values)
                            {
                                foreach (SqlForeignKey otherFk in otherColumn.ForeignKeys)
                                {
                                    if (otherFk.Name == fk.Name && otherFk.Table == fk.Table && otherFk != fk)
                                    {
                                        fkGroup.Add(otherFk);
                                    }
                                }
                            }
                            string fkSql = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, new Dictionary<string, SqlTable>(targetSchema));
                            downScript.Append(fkSql);
                            downScript.AppendLine();
                            processedFksDown1.Add(fk.Name);
                        }
                    }
                }
            }
        }

        // Recreate new tables (tables that were dropped by the migration and need to be restored)
        // In downDiff, NewTables = tables in targetSchema (before migration) but not in currentSchema (after migration)
        // These are tables that were DROPPED by the migration, so we need to RECREATE them in the down script
        if (diff.NewTables.Count > 0)
        {
            MigrationLogger.Log($"\n=== Recreating new tables (were dropped by migration): {string.Join(", ", diff.NewTables.Select(t => t.Name))} ===");
        }
        foreach (SqlTable newTable in diff.NewTables.OrderBy(t => t.Name))
        {
            // Generate CREATE TABLE statement
            string createTableSql = GenerateTables.GenerateCreateTableStatement(newTable);
            downScript.AppendLine(createTableSql);
            downScript.AppendLine();
            
            // Generate FK constraints for the recreated table
            HashSet<string> processedFksDown2 = new HashSet<string>();
            foreach (SqlTableColumn column in newTable.Columns.Values)
            {
                foreach (SqlForeignKey fk in column.ForeignKeys)
                {
                    if (!processedFksDown2.Contains(fk.Name))
                    {
                        // Group multi-column FKs
                        List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { fk };
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
                        string fkSql = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, new Dictionary<string, SqlTable>(targetSchema));
                        downScript.Append(fkSql);
                        downScript.AppendLine();
                        processedFksDown2.Add(fk.Name);
                    }
                }
            }
        }
        
        // Drop tables that were added by the migration (in reverse order)
        // In downDiff, DroppedTableNames = tables in currentSchema (after migration) but not in targetSchema (before migration)
        // These are tables that were ADDED by the migration, so we need to DROP them in the down script
        for (int i = diff.DroppedTableNames.Count - 1; i >= 0; i--)
        {
            string droppedTableName = diff.DroppedTableNames[i];
            // Get schema from current schema (after migration)
            string schema = "dbo";
            if (currentSchema != null && currentSchema.TryGetValue(droppedTableName.ToLowerInvariant(), out SqlTable? droppedTable))
            {
                schema = droppedTable.Schema;
            }
            downScript.AppendLine($"DROP TABLE IF EXISTS [{schema}].[{droppedTableName}];");
        }

        // Drop sequences that were added or modified by the migration (reverse order)
        // In downDiff, DroppedSequenceNames = sequences in currentSchema (after migration) but not in targetSchema (before migration)
        // These are sequences that were ADDED by the migration, so we need to DROP them in the down script
        if (diff.DroppedSequenceNames.Count > 0)
        {
            MigrationLogger.Log($"\n=== Dropping sequences (were added by migration): {string.Join(", ", diff.DroppedSequenceNames)} ===");
        }
        foreach (string droppedSequenceName in diff.DroppedSequenceNames.OrderByDescending(s => s))
        {
            MigrationLogger.Log($"  [DROP SEQUENCE] {droppedSequenceName}");
            // Get schema from current schema (after migration)
            string schema = "dbo";
            if (currentSequences.TryGetValue(droppedSequenceName.ToLowerInvariant(), out SqlSequence? droppedSequence))
            {
                schema = droppedSequence.Schema;
            }
            downScript.AppendLine(GenerateSequences.GenerateDropSequenceStatement(droppedSequenceName, schema));
        }
        // For Modify: drop the current sequence (OldSequence - after modification) before recreating the old one
        foreach (SequenceChange sequenceChange in diff.ModifiedSequences)
        {
            if (sequenceChange.ChangeType == SequenceChangeType.Modify && sequenceChange.OldSequence != null)
            {
                downScript.AppendLine(GenerateSequences.GenerateDropSequenceStatement(sequenceChange.OldSequence.Name, sequenceChange.OldSequence.Schema));
            }
        }

        // Drop procedures that were added or modified by the migration (reverse order)
        // In downDiff, DroppedProcedureNames = procedures in currentSchema (after migration) but not in targetSchema (before migration)
        // These are procedures that were ADDED by the migration, so we need to DROP them in the down script
        foreach (string droppedProcedureName in diff.DroppedProcedureNames.OrderByDescending(p => p))
        {
            // Get schema from current schema (after migration)
            string schema = "dbo";
            if (currentProcedures.TryGetValue(droppedProcedureName.ToLowerInvariant(), out SqlStoredProcedure? droppedProcedure))
            {
                schema = droppedProcedure.Schema;
            }
            downScript.Append(GenerateProcedures.GenerateDropProcedureStatement(droppedProcedureName, schema));
            downScript.AppendLine();
        }
        // For Modify: drop the current procedure (OldProcedure - after modification) before recreating the old one
        foreach (ProcedureChange procedureChange in diff.ModifiedProcedures)
        {
            if (procedureChange.ChangeType == ProcedureChangeType.Modify && procedureChange.OldProcedure != null)
            {
                downScript.Append(GenerateProcedures.GenerateDropProcedureStatement(procedureChange.OldProcedure.Name, procedureChange.OldProcedure.Schema));
                downScript.AppendLine();
            }
        }

        // Recreate sequences that were dropped or modified by the migration
        // In downDiff, NewSequences = sequences in targetSchema (before migration) but not in currentSchema (after migration)
        // These are sequences that were DROPPED by the migration, so we need to RECREATE them in the down script
        if (diff.NewSequences.Count > 0)
        {
            MigrationLogger.Log($"\n=== Recreating sequences (were dropped by migration): {string.Join(", ", diff.NewSequences.Select(s => s.Name))} ===");
        }
        foreach (SqlSequence newSequence in diff.NewSequences.OrderBy(s => s.Name))
        {
            MigrationLogger.Log($"  [CREATE SEQUENCE] {newSequence.Name}");
            downScript.Append(GenerateSequences.GenerateCreateSequenceStatement(newSequence));
            downScript.AppendLine();
        }
        // For Modify: restore the old sequence (NewSequence - before modification)
        foreach (SequenceChange sequenceChange in diff.ModifiedSequences)
        {
            if (sequenceChange.ChangeType == SequenceChangeType.Modify && sequenceChange.NewSequence != null)
            {
                downScript.Append(GenerateSequences.GenerateCreateSequenceStatement(sequenceChange.NewSequence));
                downScript.AppendLine();
            }
        }

        // Recreate procedures that were dropped or modified by the migration
        // In downDiff, NewProcedures = procedures in targetSchema (before migration) but not in currentSchema (after migration)
        // These are procedures that were DROPPED by the migration, so we need to RECREATE them in the down script
        foreach (SqlStoredProcedure newProcedure in diff.NewProcedures.OrderBy(p => p.Name))
        {
            downScript.Append(GenerateProcedures.GenerateCreateProcedureStatement(newProcedure));
            downScript.AppendLine();
        }
        // For Modify: restore the old procedure (NewProcedure - before modification)
        foreach (ProcedureChange procedureChange in diff.ModifiedProcedures)
        {
            if (procedureChange.ChangeType == ProcedureChangeType.Modify && procedureChange.NewProcedure != null)
            {
                downScript.Append(GenerateProcedures.GenerateCreateProcedureStatement(procedureChange.NewProcedure));
                downScript.AppendLine();
            }
        }

        // Reorder phase for down script: Ensure columns are in correct order after rollback
        // Compare actual database column order (after down script operations) with target schema order
        // Target schema is what we're rolling back to (the state before the migration)
        StringBuilder downColumnReorder = new StringBuilder();
        
        // Calculate schema after all down script column operations
        // For down script: we're rolling back from currentSchema (after migration) to targetSchema (before migration)
        // So after applying down script, the schema should match targetSchema
        ConcurrentDictionary<string, SqlTable> schemaAfterDownOperations = MigrationSchemaSnapshot.ApplySchemaDiffToTarget(currentSchema ?? new ConcurrentDictionary<string, SqlTable>(), diff);
        
        // Build FK dictionary for reorder
        ConcurrentDictionary<string, SqlTable> allTablesForFkDown = new ConcurrentDictionary<string, SqlTable>(targetSchema, StringComparer.OrdinalIgnoreCase);
        
        foreach (TableDiff tableDiff in diff.ModifiedTables.OrderBy(t => t.TableName))
        {
            // Skip if table doesn't exist in targetSchema (shouldn't happen for ModifiedTables)
            if (!targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTableDown))
            {
                continue;
            }
            
            // Only reorder if there were column Add or Modify operations that could have changed column order
            // Drop operations don't change the order of remaining columns, so no reorder needed
            // IMPORTANT: Only reorder if we have Add operations OR Modify operations that require DROP+ADD
            // This is because these operations add columns at the end, which can change the order
            bool hasColumnAddOrModify = tableDiff.ColumnChanges.Any(c => 
                c.ChangeType == ColumnChangeType.Add || 
                (c.ChangeType == ColumnChangeType.Modify && c.OldColumn != null && c.NewColumn != null &&
                 ((c.OldColumn.IsIdentity != c.NewColumn.IsIdentity) || (c.OldColumn.IsComputed != c.NewColumn.IsComputed))));
            
            if (!hasColumnAddOrModify)
            {
                // No column Add/Modify operations that would change order, skip reordering
                // Drop operations don't change the order of remaining columns
                continue;
            }
            
            // Get actual table from currentSchema (before down script) - this reflects the actual database order
            // We'll compare this with targetSchema to see if reordering is needed
            SqlTable? actualTableBeforeDown = null;
            if (currentSchema != null && currentSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? currentTable))
            {
                actualTableBeforeDown = currentTable;
            }
            
            if (actualTableBeforeDown == null)
            {
                // Can't determine actual order, skip reordering
                continue;
            }
            
            // Get actual table from schemaAfterDownOperations (what database will have after down script)
            if (schemaAfterDownOperations.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? actualTableDown))
            {
                // Safety check: ensure table has columns
                if (actualTableDown.Columns.Count == 0)
                {
                    continue;
                }
                
                // Desired table is targetSchema (what we're rolling back to)
                // Compare actual order (from actualTableBeforeDown, but with columns that will remain after down script)
                // with desired order (from targetSchema)
                // Build actual table with only columns that will remain after down script
                Dictionary<string, SqlTableColumn> actualColumnsAfterDown = new Dictionary<string, SqlTableColumn>(StringComparer.OrdinalIgnoreCase);
                HashSet<string> droppedColumnNames = tableDiff.ColumnChanges
                    .Where(c => c.ChangeType == ColumnChangeType.Drop && c.OldColumn != null)
                    .Select(c => c.OldColumn!.Name.ToLowerInvariant())
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);
                
                int position = 1;
                foreach (SqlTableColumn col in actualTableBeforeDown.Columns.Values.OrderBy(c => c.OrdinalPosition))
                {
                    // Skip columns that will be dropped
                    if (droppedColumnNames.Contains(col.Name.ToLowerInvariant()))
                    {
                        continue;
                    }
                    
                    // Use column from actualTableDown (has correct properties after down script)
                    if (actualTableDown.Columns.TryGetValue(col.Name.ToLowerInvariant(), out SqlTableColumn? colAfterDown))
                    {
                        actualColumnsAfterDown[col.Name.ToLowerInvariant()] = colAfterDown with { OrdinalPosition = position++ };
                    }
                }
                
                // Add any columns that were added back (from Add operations)
                foreach (ColumnChange addChange in tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Add))
                {
                    if (addChange.NewColumn != null && actualTableDown.Columns.TryGetValue(addChange.NewColumn.Name.ToLowerInvariant(), out SqlTableColumn? newCol))
                    {
                        if (!actualColumnsAfterDown.ContainsKey(newCol.Name.ToLowerInvariant()))
                        {
                            actualColumnsAfterDown[newCol.Name.ToLowerInvariant()] = newCol with { OrdinalPosition = position++ };
                        }
                    }
                }
                
                SqlTable actualTableForReorder = new SqlTable(actualTableDown.Name, actualColumnsAfterDown, actualTableDown.Indexes, actualTableDown.Schema);
                
                // Desired table is targetSchema (what we're rolling back to)
                // Both should have the same columns, but order might differ
                (string reorderSql, List<string> constraintSql) = GenerateTables.GenerateColumnReorderStatement(
                    actualTableForReorder,      // Actual database state after down script (with correct order)
                    targetTableDown,            // Desired state (target schema - what we're rolling back to)
                    allTablesForFkDown);
                
                if (!string.IsNullOrEmpty(reorderSql))
                {
                    downColumnReorder.Append(reorderSql);
                    
                    // Recreate all constraints and indexes that were dropped during table recreation
                    foreach (string constraint in constraintSql)
                    {
                        downColumnReorder.Append(constraint);
                        downColumnReorder.AppendLine();
                    }
                }
            }
        }
        
        string downReorderContent = downColumnReorder.ToString().Trim();
        if (!string.IsNullOrEmpty(downReorderContent))
        {
            downScript.AppendLine();
            downScript.AppendLine("-- ==============================================");
            downScript.AppendLine("-- Reorder Columns (to match target schema order)");
            downScript.AppendLine("-- ==============================================");
            downScript.AppendLine();
            downScript.AppendLine(downReorderContent);
            downScript.AppendLine();
        }

        downScript.AppendLine();

        return downScript.ToString().TrimEnd();
    }
}

