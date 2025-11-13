using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Up;

public static class GeneratePhase3_5ColumnReorder
{
    public static string Generate(
        SchemaDiff upDiff,
        ConcurrentDictionary<string, SqlTable> targetSchema,
        ConcurrentDictionary<string, SqlTable>? currentSchema)
    {
        StringBuilder sb = new StringBuilder();
        
        // Reorder columns to match current schema order (after all column operations and constraints)
        // Compare actual database column order (after Phase 2) with desired order
        // Desired order: current schema order (the state we're migrating TO, which has the correct order)
        
        // Calculate schema after all column changes (this is what the database looks like after Phase 2)
        MigrationLogger.Log($"\n=== Computing schemaAfterAllColumnChanges ===");
        MigrationLogger.Log($"  upDiff.ModifiedTables.Count: {upDiff.ModifiedTables.Count}");
        foreach (TableDiff tableDiff in upDiff.ModifiedTables)
        {
            MigrationLogger.Log($"  Table: {tableDiff.TableName}, ColumnChanges.Count: {tableDiff.ColumnChanges.Count}");
            foreach (ColumnChange change in tableDiff.ColumnChanges)
            {
                MigrationLogger.Log($"    ChangeType: {change.ChangeType}, Column: {change.OldColumn?.Name ?? change.NewColumn?.Name ?? "null"}");
            }
        }
        
        // Filter out Drop operations from upDiff when computing schemaAfterAllColumnChanges for the up script
        // Drop operations are for the down script (reversing the migration), not for the up script
        // We only want to process Add and Modify operations to get the state after Phase 2
        SchemaDiff upDiffFiltered = new SchemaDiff(
            upDiff.NewTables,
            upDiff.DroppedTableNames,
            upDiff.ModifiedTables.Select(td => new TableDiff(
                td.TableName,
                td.ColumnChanges.Where(c => c.ChangeType != ColumnChangeType.Drop).ToList(),
                td.ForeignKeyChanges,
                td.IndexChanges
            )).ToList(),
            upDiff.NewSequences,
            upDiff.DroppedSequenceNames,
            upDiff.ModifiedSequences,
            upDiff.NewProcedures,
            upDiff.DroppedProcedureNames,
            upDiff.ModifiedProcedures
        );
        
        MigrationLogger.Log($"  Filtered upDiffFiltered.ModifiedTables.Count: {upDiffFiltered.ModifiedTables.Count}");
        foreach (TableDiff tableDiff in upDiffFiltered.ModifiedTables)
        {
            MigrationLogger.Log($"  Table: {tableDiff.TableName}, ColumnChanges.Count (after filtering): {tableDiff.ColumnChanges.Count}");
            foreach (ColumnChange change in tableDiff.ColumnChanges)
            {
                MigrationLogger.Log($"    ChangeType: {change.ChangeType}, Column: {change.OldColumn?.Name ?? change.NewColumn?.Name ?? "null"}");
            }
        }
        
        ConcurrentDictionary<string, SqlTable> schemaAfterAllColumnChanges = MigrationSchemaSnapshot.ApplySchemaDiffToTarget(targetSchema, upDiffFiltered);
        
        // Double-check: if upDiffFiltered has Drop operations but ApplySchemaDiffToTarget didn't remove them,
        // manually remove them now. Also, if targetSchema has columns that currentSchema doesn't have,
        // those columns were dropped, so remove them from schemaAfterAllColumnChanges.
        // NOTE: We use upDiffFiltered (not diff) because we've already filtered out Drop operations
        foreach (TableDiff tableDiff in upDiffFiltered.ModifiedTables)
        {
            if (schemaAfterAllColumnChanges.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? table))
            {
                HashSet<string> droppedColumnNames = tableDiff.ColumnChanges
                    .Where(c => c.ChangeType == ColumnChangeType.Drop && c.OldColumn != null)
                    .Select(c => c.OldColumn!.Name.ToLowerInvariant())
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);
                
                // Also check: if targetSchema has columns that currentSchema doesn't have, those were dropped
                // BUT: Only do this check if the diff actually has Drop operations, to avoid false positives
                // when comparing schemas that are in different states (e.g., during up vs down script generation)
                if (droppedColumnNames.Count == 0 && 
                    targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTable) &&
                    currentSchema != null &&
                    currentSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? currentTable))
                {
                    // Only check for dropped columns if the diff doesn't already have Drop operations
                    // This prevents false positives when comparing schemas in different states
                    foreach (string targetColName in targetTable.Columns.Keys)
                    {
                        if (!currentTable.Columns.ContainsKey(targetColName))
                        {
                            // Column exists in targetSchema but not in currentSchema - it was dropped
                            // But only add it if it's not already being added by the diff
                            bool isBeingAdded = tableDiff.ColumnChanges.Any(c => 
                                c.ChangeType == ColumnChangeType.Add && 
                                c.NewColumn != null &&
                                c.NewColumn.Name.Equals(targetColName, StringComparison.OrdinalIgnoreCase));
                            
                            if (!isBeingAdded)
                            {
                                droppedColumnNames.Add(targetColName);
                            }
                        }
                    }
                }
                
                if (droppedColumnNames.Count > 0)
                {
                    MigrationLogger.Log($"  Manually removing dropped columns from {tableDiff.TableName}: [{string.Join(", ", droppedColumnNames)}]");
                    Dictionary<string, SqlTableColumn> newColumns = new Dictionary<string, SqlTableColumn>(table.Columns);
                    foreach (string droppedCol in droppedColumnNames)
                    {
                        newColumns.Remove(droppedCol);
                    }
                    schemaAfterAllColumnChanges[tableDiff.TableName.ToLowerInvariant()] = new SqlTable(table.Name, newColumns, table.Indexes, table.Schema);
                }
            }
        }
        
        MigrationLogger.Log($"  schemaAfterAllColumnChanges tables: [{string.Join(", ", schemaAfterAllColumnChanges.Keys)}]");
        foreach (var kvp in schemaAfterAllColumnChanges)
        {
            MigrationLogger.Log($"    {kvp.Key}: columns=[{string.Join(", ", kvp.Value.Columns.Values.OrderBy(c => c.OrdinalPosition).Select(c => c.Name))}]");
        }
        
        // Build a complete schema dictionary for FK generation (includes all tables from schemaAfterAllColumnChanges)
        ConcurrentDictionary<string, SqlTable> allTablesForFk = new ConcurrentDictionary<string, SqlTable>(schemaAfterAllColumnChanges, StringComparer.OrdinalIgnoreCase);
        
        // Use upDiff (not diff) for Phase 3.5 to ensure we're using the correct diff for the up script
        foreach (TableDiff tableDiff in upDiff.ModifiedTables.OrderBy(t => t.TableName))
        {
            MigrationLogger.Log($"\n=== Phase 3.5: Reorder Columns for table: {tableDiff.TableName} ===");
            
            // Skip reordering if there are no Add, Modify (DROP+ADD), or Rebuild operations that would change column order
            // Drop operations don't change the order of remaining columns, so no reorder needed
            bool hasColumnAddOrModify = tableDiff.ColumnChanges.Any(c => 
                c.ChangeType == ColumnChangeType.Add || 
                c.ChangeType == ColumnChangeType.Rebuild ||
                (c.ChangeType == ColumnChangeType.Modify && c.OldColumn != null && c.NewColumn != null &&
                 ((c.OldColumn.IsIdentity != c.NewColumn.IsIdentity) || (c.OldColumn.IsComputed != c.NewColumn.IsComputed))));
            
            bool hasDropOperations = tableDiff.ColumnChanges.Any(c => c.ChangeType == ColumnChangeType.Drop);
            
            MigrationLogger.Log($"  hasColumnAddOrModify: {hasColumnAddOrModify}, hasDropOperations: {hasDropOperations}");
            
            if (!hasColumnAddOrModify)
            {
                // No column Add/Modify operations that would change order, skip reordering
                // Drop operations don't change the order of remaining columns
                // IMPORTANT: If we only have Drop operations, skip reordering entirely
                // This prevents trying to reorder columns that were already dropped
                MigrationLogger.Log($"  Skipping reordering: no Add/Modify operations that change order");
                continue;
            }
            
            // Get actual table from schemaAfterAllColumnChanges (what database has after Phase 2 - reflects actual column order)
            if (schemaAfterAllColumnChanges.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? actualTable))
            {
                MigrationLogger.Log($"  actualTable columns after Phase 2: [{string.Join(", ", actualTable.Columns.Values.OrderBy(c => c.OrdinalPosition).Select(c => c.Name))}]");
                
                // Safety check: ensure table has columns before attempting reorder
                if (actualTable.Columns.Count == 0)
                {
                    MigrationLogger.Log($"  Skipping reordering: table has no columns");
                    continue;
                }
                
                // Build desired table with correct column order
                // Desired order: use actualTable (state after Phase 2) when columns are dropped
                // Otherwise use currentSchema (the state we're migrating TO) to preserve manual reordering
                // This ensures we only include columns that actually exist after Phase 2
                SqlTable? desiredTable = null;
                
                // Check if there are Drop operations - if so, use actualTable (state after Phase 2) as order source
                // This ensures we don't include dropped columns in the desired order
                // Otherwise, use currentSchema (preserves manual reordering)
                if (hasDropOperations)
                {
                    // Columns are being dropped - use actualTable (the state after Phase 2) directly as desiredTable
                    // This ensures we only include columns that exist after drops
                    desiredTable = actualTable;
                    MigrationLogger.Log($"  Using actualTable directly as desiredTable (hasDropOperations=true)");
                    MigrationLogger.Log($"  desiredTable columns: [{string.Join(", ", desiredTable.Columns.Values.OrderBy(c => c.OrdinalPosition).Select(c => c.Name))}]");
                }
                else
                {
                    // No drops - use currentSchema to preserve manual reordering
                    SqlTable? orderSourceTable = null;
                    if (currentSchema != null && currentSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? currentTable))
                    {
                        orderSourceTable = currentTable;
                    }
                    else if (targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTableFallback))
                    {
                        orderSourceTable = targetTableFallback;
                    }
                    
                    if (orderSourceTable == null)
                    {
                        continue;
                    }
                    
                    // Build desired column order: use order from orderSourceTable, but only include columns that exist in actualTable
                    Dictionary<string, SqlTableColumn> desiredColumns = new Dictionary<string, SqlTableColumn>(StringComparer.OrdinalIgnoreCase);
                    
                    // Start with order source columns (preserve their order - this is the desired order)
                    List<SqlTableColumn> orderSourceColumns = orderSourceTable.Columns.Values.OrderBy(c => c.OrdinalPosition).ToList();
                    
                    // Track which columns we've already added
                    HashSet<string> addedColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    
                    int position = 1;
                    
                    // Add columns from order source in their order (this is the desired order)
                    // BUT only include columns that exist in actualTable (after Phase 2)
                    // This ensures we don't try to copy columns that were dropped
                    // IMPORTANT: Use column properties directly from orderSourceTable (current or target schema)
                    // This represents the final desired state after the migration, so it has all correct properties
                    // including IDENTITY, PK, FKs, etc.
                    foreach (SqlTableColumn col in orderSourceColumns)
                    {
                        // Only include columns that exist in actualTable (after Phase 2)
                        // Columns that were dropped won't be in actualTable, so skip them
                        if (actualTable.Columns.ContainsKey(col.Name.ToLowerInvariant()))
                        {
                            // Use column from order source directly (has all correct properties)
                            // Reassign OrdinalPosition to reflect desired order (from order source)
                            desiredColumns[col.Name.ToLowerInvariant()] = col with { OrdinalPosition = position++ };
                            addedColumnNames.Add(col.Name.ToLowerInvariant());
                        }
                    }
                    
                    // Include any columns from actualTable that aren't in order source (shouldn't happen, but safety check)
                    foreach (SqlTableColumn actualCol in actualTable.Columns.Values)
                    {
                        if (!addedColumnNames.Contains(actualCol.Name.ToLowerInvariant()))
                        {
                            // Column exists in actualTable but not in order source - append at end
                            desiredColumns[actualCol.Name.ToLowerInvariant()] = actualCol with { OrdinalPosition = position++ };
                            addedColumnNames.Add(actualCol.Name.ToLowerInvariant());
                        }
                    }
                    
                    // Create desired table with correct column order
                    desiredTable = new SqlTable(actualTable.Name, desiredColumns, actualTable.Indexes, actualTable.Schema);
                }
                
                if (desiredTable == null)
                {
                    continue;
                }
                
                // Safety check: if desiredTable is the same reference as actualTable, and they have the same columns,
                // then the order should already match (since they're the same object)
                // This prevents reordering when we're using actualTable directly as desiredTable (e.g., when columns are dropped)
                bool isSameReference = ReferenceEquals(desiredTable, actualTable);
                bool hasSameColumns = isSameReference || 
                    (desiredTable.Columns.Count == actualTable.Columns.Count &&
                     desiredTable.Columns.Keys.All(k => actualTable.Columns.ContainsKey(k)) &&
                     actualTable.Columns.Keys.All(k => desiredTable.Columns.ContainsKey(k)));
                
                MigrationLogger.Log($"  isSameReference: {isSameReference}, hasSameColumns: {hasSameColumns}");
                MigrationLogger.Log($"  actualTable.Columns.Count: {actualTable.Columns.Count}, desiredTable.Columns.Count: {desiredTable.Columns.Count}");
                
                if (isSameReference && hasSameColumns)
                {
                    // Same reference and same columns - order already matches, skip reordering
                    MigrationLogger.Log($"  Skipping reordering: same reference and same columns (order already matches)");
                    continue;
                }
                
                MigrationLogger.Log($"  Calling GenerateColumnReorderStatement with:");
                MigrationLogger.Log($"    actualTable columns: [{string.Join(", ", actualTable.Columns.Values.OrderBy(c => c.OrdinalPosition).Select(c => c.Name))}]");
                MigrationLogger.Log($"    desiredTable columns: [{string.Join(", ", desiredTable.Columns.Values.OrderBy(c => c.OrdinalPosition).Select(c => c.Name))}]");
                
                (string reorderSql, List<string> constraintSql) = GenerateTables.GenerateColumnReorderStatement(
                    actualTable,      // Actual database state (from schemaAfterAllColumnChanges - reflects state after Phase 2)
                    desiredTable,     // Desired state (with columns in correct order)
                    allTablesForFk);
                
                if (!string.IsNullOrEmpty(reorderSql))
                {
                    sb.Append(reorderSql);
                    
                    // Recreate all constraints and indexes that were dropped during table recreation
                    foreach (string constraint in constraintSql)
                    {
                        sb.Append(constraint);
                        sb.AppendLine();
                    }
                }
            }
        }
        
        return sb.ToString().Trim();
    }
}

