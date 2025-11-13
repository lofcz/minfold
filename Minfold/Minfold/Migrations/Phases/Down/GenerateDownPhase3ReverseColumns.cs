using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Down;

public static class GenerateDownPhase3ReverseColumns
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable> currentSchema,
        ConcurrentDictionary<string, SqlTable> targetSchema)
    {
        StringBuilder content = new StringBuilder();
        
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
            List<ColumnChange> rebuildChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Rebuild).ToList();
            
            // Check if we have Modify/Rebuild changes that require DROP+ADD, and Drop changes that would leave the modified column as the only one
            // In this case, we need to process Modify/Rebuild BEFORE Drop to avoid the "only column" error
            bool needsModifyBeforeDrop = false;
            if ((modifyChanges.Count > 0 || rebuildChanges.Count > 0) && dropChanges.Count > 0 && currentSchema != null)
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
                    
                    // Check if any Rebuild change would be the only column after drops
                    if (!needsModifyBeforeDrop && rebuildChanges.Count > 0 && currentDataColumnCount - dropChanges.Count == 1)
                    {
                        needsModifyBeforeDrop = true;
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
                        content.AppendLine(GenerateColumns.GenerateDropColumnStatement(change.OldColumn.Name, tableDiff.TableName));
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
            
            // Create a combined list of Add, Modify, and Rebuild operations, ordered by original position
            List<(ColumnChange Change, int OriginalPosition, bool IsModify, bool IsRebuild)> allRestoreOperations = new List<(ColumnChange, int, bool, bool)>();
            
            // Add Modify operations (these restore modified columns)
            foreach (ColumnChange change in modifyChanges)
            {
                if (change.NewColumn != null && targetTable != null)
                {
                    if (targetTable.Columns.TryGetValue(change.NewColumn.Name.ToLowerInvariant(), out SqlTableColumn? targetCol))
                    {
                        allRestoreOperations.Add((change, targetCol.OrdinalPosition, true, false));
                    }
                }
            }
            
            // Add Rebuild operations (these restore rebuilt columns - same as Modify for down script)
            foreach (ColumnChange change in rebuildChanges)
            {
                if (change.NewColumn != null && targetTable != null)
                {
                    if (targetTable.Columns.TryGetValue(change.NewColumn.Name.ToLowerInvariant(), out SqlTableColumn? targetCol))
                    {
                        allRestoreOperations.Add((change, targetCol.OrdinalPosition, true, true));
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
                        allRestoreOperations.Add((change, targetCol.OrdinalPosition, false, false));
                    }
                }
            }
            
            // Sort by original position to preserve column order
            allRestoreOperations = allRestoreOperations.OrderBy(op => op.OriginalPosition).ToList();
            
            // Process operations in order, but handle Modify/Rebuild operations specially (they need DROP+ADD logic)
            foreach (var (change, originalPosition, isModify, isRebuild) in allRestoreOperations)
            {
                if (isModify || isRebuild)
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
                                content.AppendLine(GeneratePrimaryKeys.GenerateDropPrimaryKeyStatement(tableDiff.TableName, pkConstraintName, schema));
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
                            content.Append(safeDropAdd);
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
                                content.Append(reverseAlter);
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
                            content.Append(GenerateColumns.GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName));
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
                            content.AppendLine(GeneratePrimaryKeys.GenerateDropPrimaryKeyStatement(tableDiff.TableName, pkConstraintName, schema));
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
                            content.Append(safeDropAdd);
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
                            content.AppendLine(GeneratePrimaryKeys.GenerateDropPrimaryKeyStatement(tableDiff.TableName, pkConstraintName, schema));
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
                            content.Append(safeDropAdd);
                        }
                    }
                    else
                    {
                        // Reverse the modification: change from OldColumn (current) to NewColumn (target)
                        string reverseAlter = GenerateColumns.GenerateAlterColumnStatement(change.OldColumn, change.NewColumn, tableDiff.TableName, schema);
                        if (!string.IsNullOrEmpty(reverseAlter))
                        {
                            content.Append(reverseAlter);
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
                        content.AppendLine(GenerateColumns.GenerateDropColumnStatement(change.OldColumn.Name, tableDiff.TableName));
                    }
                }
            }
        }
        
        return content.ToString().Trim();
    }
}

