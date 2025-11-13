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
                    content.Append(reorderSql);
                    
                    // Recreate all constraints and indexes that were dropped during table recreation
                    foreach (string constraint in constraintSql)
                    {
                        content.Append(constraint);
                        content.AppendLine();
                    }
                }
            }
        }
        
        return content.ToString().Trim();
    }
}

