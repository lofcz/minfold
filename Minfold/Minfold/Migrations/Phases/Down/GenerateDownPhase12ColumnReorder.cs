using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Down;

public static class GenerateDownPhase12ColumnReorder
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable> currentSchema,
        ConcurrentDictionary<string, SqlTable> targetSchema)
    {
        StringBuilder content = new StringBuilder();
        
        // Reorder phase for down script: Ensure columns are in correct order after rollback
        // Compare actual database column order (after down script operations) with target schema order
        // Target schema is what we're rolling back to (the state before the migration)
        
        // Calculate schema after all down script column operations
        // For down script: we're rolling back from currentSchema (after migration) to targetSchema (before migration)
        // So after applying down script, the schema should match targetSchema
        ConcurrentDictionary<string, SqlTable> schemaAfterDownOperations = MigrationSchemaSnapshot.ApplySchemaDiffToTarget(currentSchema ?? new ConcurrentDictionary<string, SqlTable>(), diff);
        
        // Build FK dictionary for reorder
        ConcurrentDictionary<string, SqlTable> allTablesForFkDown = new ConcurrentDictionary<string, SqlTable>(targetSchema, StringComparer.OrdinalIgnoreCase);
        
        foreach (TableDiff tableDiff in diff.ModifiedTables.OrderBy(t => t.TableName))
        {
            // Skip if table doesn't exist in targetSchema (shouldn't happen for ModifiedTables)
            if (!targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTableDown) || targetTableDown == null)
            {
                continue;
            }
            
            // Only reorder if there were column Add, Modify (DROP+ADD), or Rebuild operations that could have changed column order
            // Drop operations don't change the order of remaining columns, so no reorder needed
            // IMPORTANT: Only reorder if we have Add operations OR Modify operations that require DROP+ADD OR Rebuild operations
            // This is because these operations add columns at the end, which can change the order
            bool hasColumnAddOrModify = tableDiff.ColumnChanges.Any(c => 
                c.ChangeType == ColumnChangeType.Add || 
                c.ChangeType == ColumnChangeType.Rebuild ||
                (c.ChangeType == ColumnChangeType.Modify && c.OldColumn != null && c.NewColumn != null &&
                 ((c.OldColumn.IsIdentity != c.NewColumn.IsIdentity) || (c.OldColumn.IsComputed != c.NewColumn.IsComputed))));
            
            // Check for column order differences even if there are no other column changes
            // This handles the case where columns were manually reordered (only OrdinalPosition differs)
            bool hasColumnOrderDifference = false;
            if (!hasColumnAddOrModify && tableDiff.ColumnChanges.Count == 0)
            {
                // No column changes, but check if column order differs
                if (currentSchema != null && currentSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? localCurrentTable) &&
                    targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTable))
                {
                    List<SqlTableColumn> currentOrdered = localCurrentTable.Columns.Values
                        .OrderBy(c => c.OrdinalPosition)
                        .ToList();
                    List<SqlTableColumn> targetOrdered = targetTable.Columns.Values
                        .OrderBy(c => c.OrdinalPosition)
                        .ToList();
                    
                    if (currentOrdered.Count == targetOrdered.Count && currentOrdered.Count > 0)
                    {
                        hasColumnOrderDifference = !currentOrdered.Select(c => c.Name)
                            .SequenceEqual(targetOrdered.Select(c => c.Name), StringComparer.OrdinalIgnoreCase);
                    }
                }
            }
            
            if (!hasColumnAddOrModify && !hasColumnOrderDifference)
            {
                // No column Add/Modify operations that would change order, and no order difference, skip reordering
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
            // For order-only differences, use currentSchema directly (no column changes to apply)
            SqlTable? actualTableForReorder = null;
            if (hasColumnOrderDifference && !hasColumnAddOrModify)
            {
                // Order-only difference: use currentSchema directly (has actual database state with reordered columns)
                actualTableForReorder = actualTableBeforeDown;
                MigrationLogger.Log($"  [Down] Using currentSchema as actualTableForReorder (order-only difference)");
            }
            else if (schemaAfterDownOperations.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? actualTableDown))
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
                
                actualTableForReorder = new SqlTable(actualTableDown.Name, actualColumnsAfterDown, actualTableDown.Indexes, actualTableDown.Schema);
            }
            
            if (actualTableForReorder == null)
            {
                continue;
            }
            
            // Safety check: ensure table has columns
            if (actualTableForReorder.Columns.Count == 0)
            {
                continue;
            }
            
            // Before reordering (which drops and recreates the table), we need to drop FKs that reference this table
            // Find all FKs in currentSchema that reference this table
            HashSet<string> processedFksForReorder = new HashSet<string>();
            if (currentSchema != null)
            {
                foreach (KeyValuePair<string, SqlTable> tablePair in currentSchema)
                {
                    foreach (SqlTableColumn column in tablePair.Value.Columns.Values)
                    {
                        foreach (SqlForeignKey fk in column.ForeignKeys)
                        {
                            // Check if this FK references the table being reordered
                            if (fk.RefTable.Equals(tableDiff.TableName, StringComparison.OrdinalIgnoreCase))
                            {
                                if (!processedFksForReorder.Contains(fk.Name))
                                {
                                    MigrationLogger.Log($"  [DROP FK for reorder] {fk.Name} (references {tableDiff.TableName} being reordered)");
                                    // Use unique suffix to avoid conflicts with Phase 1 FK drops
                                    string uniqueSuffix = MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(fk.Schema, fk.Table, fk.Name, "reorder");
                                    content.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(fk, uniqueSuffix));
                                    processedFksForReorder.Add(fk.Name);
                                }
                            }
                        }
                    }
                }
            }
            
            // Desired table is targetSchema (what we're rolling back to)
            // Both should have the same columns, but order might differ
            (string reorderSql, List<string> constraintSql) = GenerateTables.GenerateColumnReorderStatement(
                actualTableForReorder,      // Actual database state after down script (with correct order)
                targetTableDown,            // Desired state (target schema - what we're rolling back to)
                allTablesForFkDown);
            
            if (!string.IsNullOrEmpty(reorderSql))
            {
                content.Append(reorderSql);
                
                // Recreate all constraints and indexes that were dropped during table recreation
                foreach (string constraint in constraintSql)
                {
                    content.Append(constraint);
                    content.AppendLine();
                }
                
                // Also restore FKs from other tables that reference this reordered table
                // These FKs were dropped when we dropped the table, but GenerateColumnReorderStatement
                // only restores FKs that belong to the reordered table itself
                HashSet<string> restoredFkNames = new HashSet<string>();
                if (targetSchema != null)
                {
                    foreach (KeyValuePair<string, SqlTable> tablePair in targetSchema)
                    {
                        foreach (SqlTableColumn column in tablePair.Value.Columns.Values)
                        {
                            foreach (SqlForeignKey fk in column.ForeignKeys)
                            {
                                // Check if this FK references the table being reordered
                                if (fk.RefTable.Equals(tableDiff.TableName, StringComparison.OrdinalIgnoreCase))
                                {
                                    if (!restoredFkNames.Contains(fk.Name))
                                    {
                                        MigrationLogger.Log($"  [RESTORE FK after reorder] {fk.Name} (references {tableDiff.TableName} that was reordered)");
                                        
                                        // Group multi-column FKs
                                        List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { fk };
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
                                        
                                        // Generate FK statement (after table is recreated, so it's safe)
                                        bool wasNoCheck = fkGroup[0].NotEnforced;
                                        // Convert ConcurrentDictionary to Dictionary for GenerateForeignKeyStatement
                                        Dictionary<string, SqlTable> tablesDictForFk = new Dictionary<string, SqlTable>(allTablesForFkDown, StringComparer.OrdinalIgnoreCase);
                                        string fkSql = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDictForFk, forceNoCheck: true);
                                        content.Append(fkSql);
                                        content.AppendLine();
                                        
                                        // Restore CHECK state if needed
                                        if (!wasNoCheck)
                                        {
                                            SqlForeignKey firstFk = fkGroup[0];
                                            content.AppendLine($"ALTER TABLE [{firstFk.Schema}].[{firstFk.Table}] DROP CONSTRAINT [{firstFk.Name}];");
                                            string fkSqlWithCheck = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDictForFk, forceNoCheck: false);
                                            if (!string.IsNullOrEmpty(fkSqlWithCheck))
                                            {
                                                content.Append(fkSqlWithCheck);
                                                content.AppendLine();
                                            }
                                        }
                                        
                                        restoredFkNames.Add(fk.Name);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return content.ToString().Trim();
    }
}

