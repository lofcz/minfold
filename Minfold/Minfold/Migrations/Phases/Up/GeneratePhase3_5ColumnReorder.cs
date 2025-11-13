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
        
        // Build a merged schema dictionary with FK metadata from base/target/current schemas
        Dictionary<string, SqlTable> schemaForFkOperations = BuildSchemaForFkOperations(
            schemaAfterAllColumnChanges,
            targetSchema,
            currentSchema,
            upDiff);
        
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
            
            // Check if there's a column order difference even if there are no column changes
            // This handles the case where columns are manually reordered (only OrdinalPosition differs)
            bool hasColumnOrderDifference = false;
            if (!hasColumnAddOrModify && tableDiff.ColumnChanges.Count == 0)
            {
                // No column changes, but check if column order differs
                if (currentSchema != null && currentSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? currentTable) &&
                    targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTable))
                {
                    List<SqlTableColumn> currentOrdered = currentTable.Columns.Values
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
                // IMPORTANT: If we only have Drop operations, skip reordering entirely
                // This prevents trying to reorder columns that were already dropped
                MigrationLogger.Log($"  Skipping reordering: no Add/Modify operations that change order and no column order difference");
                continue;
            }
            
            // Get actual table from schemaAfterAllColumnChanges (what database has after Phase 2 - reflects actual column order)
            // However, if there are no column changes, schemaAfterAllColumnChanges equals targetSchema (no changes applied)
            // So we need to use targetSchema as actualTable (what database SHOULD have) and currentSchema as desiredTable (what we want)
            SqlTable? actualTable = null;
            if (hasColumnOrderDifference && !hasColumnAddOrModify)
            {
                // No column changes, but order differs
                // actualTable = targetSchema (what database SHOULD have according to migrations)
                // desiredTable = currentSchema (what database ACTUALLY has, which is what we want to migrate TO)
                if (targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTable))
                {
                    actualTable = targetTable;
                    MigrationLogger.Log($"  Using targetSchema as actualTable (no column changes, order-only difference)");
                }
            }
            
            if (actualTable == null && schemaAfterAllColumnChanges.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? actualTableFromSchema))
            {
                actualTable = actualTableFromSchema;
            }
            
            if (actualTable != null)
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
                    // No drops - determine order source based on whether we have column order differences
                    // For order-only differences: use currentSchema as desired (what we're migrating TO)
                    // For other cases: use currentSchema to preserve manual reordering
                    SqlTable? orderSourceTable = null;
                    if (hasColumnOrderDifference && !hasColumnAddOrModify)
                    {
                        // Order-only difference: use currentSchema as desired order (what we're migrating TO)
                        if (currentSchema != null && currentSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? currentTable))
                        {
                            orderSourceTable = currentTable;
                            MigrationLogger.Log($"  Using currentSchema as order source (order-only difference, migrating TO currentSchema)");
                        }
                    }
                    else if (currentSchema != null && currentSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? currentTableForReorder))
                    {
                        orderSourceTable = currentTableForReorder;
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
                
                // Check if column order actually differs before proceeding
                // This matches the logic in GenerateColumnReorderStatement to avoid dropping FKs unnecessarily
                List<SqlTableColumn> actualOrdered = actualTable.Columns.Values
                    .OrderBy(c => c.OrdinalPosition)
                    .ToList();
                List<SqlTableColumn> desiredOrdered = desiredTable.Columns.Values
                    .OrderBy(c => c.OrdinalPosition)
                    .ToList();
                
                bool orderActuallyDiffers = !isSameReference && 
                    (actualOrdered.Count != desiredOrdered.Count ||
                     !actualOrdered.Select(c => c.Name)
                         .SequenceEqual(desiredOrdered.Select(c => c.Name), StringComparer.OrdinalIgnoreCase));
                
                if (!orderActuallyDiffers)
                {
                    // Order already matches, skip reordering (don't drop FKs)
                    MigrationLogger.Log($"  Skipping reordering: order already matches (checked column sequence)");
                    continue;
                }
                
                // Before reordering (which drops and recreates the table), we need to drop FKs that reference this table
                // IMPORTANT: Use schemaAfterAllColumnChanges (state after Phase 2) to find FKs, not currentSchema
                // This ensures we drop FKs that were added during the migration (like FK_Projects_Owner added in Phase 2)
                HashSet<string> processedFksForReorder = new HashSet<string>();
                foreach (KeyValuePair<string, SqlTable> tablePair in schemaForFkOperations)
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
                                    // Use unique suffix to avoid conflicts with Phase 0 FK drops
                                    string uniqueSuffix = MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(fk.Schema, fk.Table, fk.Name, "reorder");
                                    sb.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(fk, uniqueSuffix));
                                    processedFksForReorder.Add(fk.Name);
                                }
                            }
                        }
                    }
                }

                // Safety net: drop any remaining FKs that still reference this table (in case they weren't captured above)
                string fkCleanupSuffix = MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, tableDiff.TableName, "reorder", "cleanup");
                sb.AppendLine($"""
                    DECLARE @fkName_{fkCleanupSuffix} NVARCHAR(128);
                    DECLARE @fkSchema_{fkCleanupSuffix} NVARCHAR(128);
                    DECLARE @fkTable_{fkCleanupSuffix} NVARCHAR(128);
                    DECLARE fk_cursor_{fkCleanupSuffix} CURSOR LOCAL FAST_FORWARD FOR
                        SELECT fk.name, OBJECT_SCHEMA_NAME(fk.parent_object_id), OBJECT_NAME(fk.parent_object_id)
                        FROM sys.foreign_keys fk
                        WHERE fk.referenced_object_id = OBJECT_ID('[{actualTable.Schema}].[{tableDiff.TableName}]');
                    OPEN fk_cursor_{fkCleanupSuffix};
                    FETCH NEXT FROM fk_cursor_{fkCleanupSuffix} INTO @fkName_{fkCleanupSuffix}, @fkSchema_{fkCleanupSuffix}, @fkTable_{fkCleanupSuffix};
                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        DECLARE @dropSql_{fkCleanupSuffix} NVARCHAR(MAX) = N'ALTER TABLE [' + @fkSchema_{fkCleanupSuffix} + '].[' + @fkTable_{fkCleanupSuffix} + '] DROP CONSTRAINT [' + @fkName_{fkCleanupSuffix} + ']';
                        EXEC sp_executesql @dropSql_{fkCleanupSuffix};
                        FETCH NEXT FROM fk_cursor_{fkCleanupSuffix} INTO @fkName_{fkCleanupSuffix}, @fkSchema_{fkCleanupSuffix}, @fkTable_{fkCleanupSuffix};
                    END
                    CLOSE fk_cursor_{fkCleanupSuffix};
                    DEALLOCATE fk_cursor_{fkCleanupSuffix};
                    """);
                
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
                    
                    // Also restore FKs from other tables that reference this reordered table
                    // These FKs were dropped when we dropped the table, but GenerateColumnReorderStatement
                    // only restores FKs that belong to the reordered table itself
                    // IMPORTANT: Skip FKs that belong to the reordered table itself (they're already restored by GenerateColumnReorderStatement)
                    // IMPORTANT: Use schemaAfterAllColumnChanges (not targetSchema) because it includes FKs added in Phase 2
                    HashSet<string> restoredFkNames = new HashSet<string>();
                    HashSet<string> fksBelongingToReorderedTable = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    
                    // Collect FKs that belong to the reordered table (these are already restored by GenerateColumnReorderStatement)
                    if (desiredTable != null)
                    {
                        foreach (SqlTableColumn column in desiredTable.Columns.Values)
                        {
                            foreach (SqlForeignKey fk in column.ForeignKeys)
                            {
                                fksBelongingToReorderedTable.Add(fk.Name);
                            }
                        }
                    }
                    
                    // Use merged schema (state after Phase 2 plus target metadata) instead of targetSchema
                    // This ensures we restore FKs that were added during the migration (like FK_Projects_Owner)
                    foreach (KeyValuePair<string, SqlTable> tablePair in schemaForFkOperations)
                    {
                        // Skip the reordered table itself - its FKs are already restored
                        if (tablePair.Key.Equals(tableDiff.TableName, StringComparison.OrdinalIgnoreCase))
                        {
                            continue;
                        }
                        
                        foreach (SqlTableColumn column in tablePair.Value.Columns.Values)
                        {
                            foreach (SqlForeignKey fk in column.ForeignKeys)
                            {
                                // Check if this FK references the table being reordered
                                // AND it doesn't belong to the reordered table itself
                                if (fk.RefTable.Equals(tableDiff.TableName, StringComparison.OrdinalIgnoreCase) &&
                                    !fksBelongingToReorderedTable.Contains(fk.Name) &&
                                    !restoredFkNames.Contains(fk.Name))
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
                                    Dictionary<string, SqlTable> tablesDictForFk = new Dictionary<string, SqlTable>(allTablesForFk, StringComparer.OrdinalIgnoreCase);
                                    string fkSql = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDictForFk, forceNoCheck: true);
                                    sb.Append(fkSql);
                                    sb.AppendLine();
                                    
                                    // Restore CHECK state if needed
                                    if (!wasNoCheck)
                                    {
                                        SqlForeignKey firstFk = fkGroup[0];
                                        sb.AppendLine($"ALTER TABLE [{firstFk.Schema}].[{firstFk.Table}] DROP CONSTRAINT [{firstFk.Name}];");
                                        string fkSqlWithCheck = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDictForFk, forceNoCheck: false);
                                        if (!string.IsNullOrEmpty(fkSqlWithCheck))
                                        {
                                            sb.Append(fkSqlWithCheck);
                                            sb.AppendLine();
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
        
        return sb.ToString().Trim();
    }

    private static Dictionary<string, SqlTable> BuildSchemaForFkOperations(
        ConcurrentDictionary<string, SqlTable> baseSchema,
        ConcurrentDictionary<string, SqlTable> targetSchema,
        ConcurrentDictionary<string, SqlTable>? currentSchema,
        SchemaDiff diff)
    {
        Dictionary<string, SqlTable> result = new Dictionary<string, SqlTable>(StringComparer.OrdinalIgnoreCase);

        void MergeTable(SqlTable sourceTable)
        {
            string tableKey = sourceTable.Name.ToLowerInvariant();

            if (!result.TryGetValue(tableKey, out SqlTable? mergedTable))
            {
                Dictionary<string, SqlTableColumn> clonedColumns = new Dictionary<string, SqlTableColumn>(sourceTable.Columns.Count, StringComparer.OrdinalIgnoreCase);
                foreach (KeyValuePair<string, SqlTableColumn> columnPair in sourceTable.Columns)
                {
                    clonedColumns[columnPair.Key] = columnPair.Value with
                    {
                        ForeignKeys = new List<SqlForeignKey>(columnPair.Value.ForeignKeys)
                    };
                }

                List<SqlIndex> clonedIndexes = new List<SqlIndex>(sourceTable.Indexes);
                result[tableKey] = new SqlTable(sourceTable.Name, clonedColumns, clonedIndexes, sourceTable.Schema);
                return;
            }

            foreach (KeyValuePair<string, SqlTableColumn> columnPair in sourceTable.Columns)
            {
                if (!mergedTable.Columns.TryGetValue(columnPair.Key, out SqlTableColumn? mergedColumn))
                {
                    mergedTable.Columns[columnPair.Key] = columnPair.Value with
                    {
                        ForeignKeys = new List<SqlForeignKey>(columnPair.Value.ForeignKeys)
                    };
                    continue;
                }

                foreach (SqlForeignKey fk in columnPair.Value.ForeignKeys)
                {
                    bool alreadyExists = mergedColumn.ForeignKeys.Any(existingFk =>
                        existingFk.Name.Equals(fk.Name, StringComparison.OrdinalIgnoreCase) &&
                        existingFk.Column.Equals(fk.Column, StringComparison.OrdinalIgnoreCase));

                    if (!alreadyExists)
                    {
                        mergedColumn.ForeignKeys.Add(fk);
                    }
                }
            }
        }

        foreach (SqlTable table in baseSchema.Values)
        {
            MergeTable(table);
        }

        foreach (SqlTable table in targetSchema.Values)
        {
            MergeTable(table);
        }

        if (currentSchema is not null)
        {
            foreach (SqlTable table in currentSchema.Values)
            {
                MergeTable(table);
            }
        }

        foreach (TableDiff tableDiff in diff.ModifiedTables)
        {
            foreach (ForeignKeyChange fkChange in tableDiff.ForeignKeyChanges)
            {
                SqlForeignKey? fkToAdd = fkChange.NewForeignKey ?? fkChange.OldForeignKey;
                if (fkToAdd == null)
                {
                    continue;
                }

                string tableKey = fkToAdd.Table.ToLowerInvariant();
                if (!result.TryGetValue(tableKey, out SqlTable? mergedTable))
                {
                    SqlTable? sourceTable = baseSchema.TryGetValue(tableKey, out SqlTable? baseTable)
                        ? baseTable
                        : targetSchema.TryGetValue(tableKey, out SqlTable? targetTable)
                            ? targetTable
                            : currentSchema != null && currentSchema.TryGetValue(tableKey, out SqlTable? currentTable)
                                ? currentTable
                                : null;

                    if (sourceTable == null)
                    {
                        continue;
                    }

                    MergeTable(sourceTable);
                    mergedTable = result[tableKey];
                }

                if (!mergedTable.Columns.TryGetValue(fkToAdd.Column.ToLowerInvariant(), out SqlTableColumn? mergedColumn))
                {
                    SqlTableColumn? sourceColumn = null;
                    if (baseSchema.TryGetValue(tableKey, out SqlTable? baseTable) &&
                        baseTable.Columns.TryGetValue(fkToAdd.Column.ToLowerInvariant(), out SqlTableColumn? baseColumn))
                    {
                        sourceColumn = baseColumn;
                    }
                    else if (targetSchema.TryGetValue(tableKey, out SqlTable? targetTable) &&
                             targetTable.Columns.TryGetValue(fkToAdd.Column.ToLowerInvariant(), out SqlTableColumn? targetColumn))
                    {
                        sourceColumn = targetColumn;
                    }
                    else if (currentSchema != null &&
                             currentSchema.TryGetValue(tableKey, out SqlTable? currentTable) &&
                             currentTable.Columns.TryGetValue(fkToAdd.Column.ToLowerInvariant(), out SqlTableColumn? currentColumn))
                    {
                        sourceColumn = currentColumn;
                    }

                    if (sourceColumn == null)
                    {
                        continue;
                    }

                    mergedColumn = sourceColumn with { ForeignKeys = new List<SqlForeignKey>() };
                    mergedTable.Columns[fkToAdd.Column.ToLowerInvariant()] = mergedColumn;
                }

                bool fkAlreadyExists = mergedColumn.ForeignKeys.Any(existingFk =>
                    existingFk.Name.Equals(fkToAdd.Name, StringComparison.OrdinalIgnoreCase) &&
                    existingFk.Column.Equals(fkToAdd.Column, StringComparison.OrdinalIgnoreCase));

                if (!fkAlreadyExists)
                {
                    mergedColumn.ForeignKeys.Add(fkToAdd);
                }
            }
        }

        return result;
    }
}
