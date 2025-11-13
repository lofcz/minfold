using System.Collections.Concurrent;

namespace Minfold;

public static class MigrationSchemaComparer
{
    public static SchemaDiff CompareSchemas(
        ConcurrentDictionary<string, SqlTable> currentSchema, 
        ConcurrentDictionary<string, SqlTable> targetSchema,
        ConcurrentDictionary<string, SqlSequence>? currentSequences = null,
        ConcurrentDictionary<string, SqlSequence>? targetSequences = null,
        ConcurrentDictionary<string, SqlStoredProcedure>? currentProcedures = null,
        ConcurrentDictionary<string, SqlStoredProcedure>? targetProcedures = null)
    {
        List<SqlTable> newTables = new List<SqlTable>();
        List<string> droppedTableNames = new List<string>();
        List<TableDiff> modifiedTables = new List<TableDiff>();

        // Find new tables (in target but not in current)
        foreach (KeyValuePair<string, SqlTable> targetTable in targetSchema)
        {
            if (!currentSchema.ContainsKey(targetTable.Key))
            {
                newTables.Add(targetTable.Value);
            }
        }

        // Find dropped tables (in current but not in target)
        foreach (KeyValuePair<string, SqlTable> currentTable in currentSchema)
        {
            if (!targetSchema.ContainsKey(currentTable.Key))
            {
                droppedTableNames.Add(currentTable.Value.Name);
            }
        }

        // Find modified tables (exist in both but have differences)
        foreach (KeyValuePair<string, SqlTable> targetTable in targetSchema)
        {
            if (currentSchema.TryGetValue(targetTable.Key, out SqlTable? currentTable))
            {
                TableDiff? tableDiff = CompareTables(currentTable, targetTable.Value);
                // Include TableDiff if it has changes OR if it exists (which means there's a difference, possibly column order)
                // CompareTables returns null only when there are no differences at all
                if (tableDiff != null)
                {
                    modifiedTables.Add(tableDiff);
                }
            }
        }

        // Compare sequences
        List<SqlSequence> newSequences = new List<SqlSequence>();
        List<string> droppedSequenceNames = new List<string>();
        List<SequenceChange> modifiedSequences = CompareSequences(currentSequences ?? new ConcurrentDictionary<string, SqlSequence>(), targetSequences ?? new ConcurrentDictionary<string, SqlSequence>(), newSequences, droppedSequenceNames);

        // Compare procedures
        List<SqlStoredProcedure> newProcedures = new List<SqlStoredProcedure>();
        List<string> droppedProcedureNames = new List<string>();
        List<ProcedureChange> modifiedProcedures = CompareProcedures(currentProcedures ?? new ConcurrentDictionary<string, SqlStoredProcedure>(), targetProcedures ?? new ConcurrentDictionary<string, SqlStoredProcedure>(), newProcedures, droppedProcedureNames);

        // Propagate column type changes to foreign key referencing columns
        // When a column that is referenced by foreign keys changes type, the referencing columns must also change
        PropagateColumnTypeChangesToForeignKeys(modifiedTables, currentSchema, targetSchema);

        return new SchemaDiff(newTables, droppedTableNames, modifiedTables, newSequences, droppedSequenceNames, modifiedSequences, newProcedures, droppedProcedureNames, modifiedProcedures);
    }

    /// <summary>
    /// Propagates column type changes to foreign key referencing columns.
    /// When a column that is referenced by foreign keys changes type (especially Rebuild changes),
    /// the referencing columns must also be updated to match the new type.
    /// </summary>
    private static void PropagateColumnTypeChangesToForeignKeys(
        List<TableDiff> modifiedTables,
        ConcurrentDictionary<string, SqlTable> currentSchema,
        ConcurrentDictionary<string, SqlTable> targetSchema)
    {
        // Build a map of (table, column) -> new column type for columns that are being changed
        // Use lowercase keys for case-insensitive matching
        Dictionary<(string Table, string Column), SqlTableColumn> changedColumns = new Dictionary<(string, string), SqlTableColumn>();
        
        MigrationLogger.Log("=== PropagateColumnTypeChangesToForeignKeys: Building changed columns map ===");
        foreach (TableDiff tableDiff in modifiedTables)
        {
            foreach (ColumnChange change in tableDiff.ColumnChanges)
            {
                if (change.NewColumn != null && 
                    (change.ChangeType == ColumnChangeType.Rebuild || change.ChangeType == ColumnChangeType.Modify))
                {
                    // Only propagate if the type actually changed
                    if (change.OldColumn != null && change.OldColumn.SqlType != change.NewColumn.SqlType)
                    {
                        string tableKey = tableDiff.TableName.ToLowerInvariant();
                        string columnKey = change.NewColumn.Name.ToLowerInvariant();
                        changedColumns[(tableKey, columnKey)] = change.NewColumn;
                        MigrationLogger.Log($"  [TYPE CHANGE] {tableDiff.TableName}.{change.NewColumn.Name}: {change.OldColumn.SqlType} -> {change.NewColumn.SqlType}");
                    }
                }
            }
        }

        MigrationLogger.Log($"=== PropagateColumnTypeChangesToForeignKeys: Found {changedColumns.Count} columns with type changes ===");

        // Find all foreign keys that reference changed columns and update the referencing columns
        foreach (KeyValuePair<string, SqlTable> tablePair in currentSchema)
        {
            foreach (SqlTableColumn column in tablePair.Value.Columns.Values)
            {
                foreach (SqlForeignKey fk in column.ForeignKeys)
                {
                    // Check if this FK references a column that changed type
                    string refTableKey = fk.RefTable.ToLowerInvariant();
                    string refColumnKey = fk.RefColumn.ToLowerInvariant();
                    if (changedColumns.TryGetValue((refTableKey, refColumnKey), out SqlTableColumn? newRefColumn))
                    {
                        // The referenced column changed type - we need to update this referencing column
                        string fkTableKey = fk.Table.ToLowerInvariant();
                        string fkColumnName = fk.Column;
                        
                        MigrationLogger.Log($"  [FK PROPAGATION] Found FK {fk.Name}: {fk.Table}.{fk.Column} references {fk.RefTable}.{fk.RefColumn} (type changed)");
                        
                        // Get the current referencing column
                        SqlTableColumn? currentFkColumn = tablePair.Value.Columns.TryGetValue(fkColumnName.ToLowerInvariant(), out SqlTableColumn? col) ? col : null;
                        
                        if (currentFkColumn != null)
                        {
                            // Check if the current referencing column type doesn't match the new referenced column type
                            // Foreign key columns MUST match the referenced column type
                            if (currentFkColumn.SqlType != newRefColumn.SqlType)
                            {
                                MigrationLogger.Log($"    [PROPAGATING] {fk.Table}.{fk.Column}: {currentFkColumn.SqlType} -> {newRefColumn.SqlType} (to match {fk.RefTable}.{fk.RefColumn})");
                                
                                // The referencing column needs to be updated to match the new referenced column type
                                // Find or create the TableDiff for this table
                                TableDiff? fkTableDiff = modifiedTables.FirstOrDefault(t => t.TableName.Equals(fk.Table, StringComparison.OrdinalIgnoreCase));
                                
                                if (fkTableDiff == null)
                                {
                                    // Create a new TableDiff for this table
                                    fkTableDiff = new TableDiff(fk.Table, new List<ColumnChange>(), new List<ForeignKeyChange>(), new List<IndexChange>());
                                    modifiedTables.Add(fkTableDiff);
                                    MigrationLogger.Log($"    [NEW TABLEDIFF] Created TableDiff for {fk.Table}");
                                }
                                
                                // Check if this column change already exists
                                ColumnChange? existingChange = fkTableDiff.ColumnChanges.FirstOrDefault(
                                    c => c.NewColumn != null && c.NewColumn.Name.Equals(fkColumnName, StringComparison.OrdinalIgnoreCase));
                                
                                if (existingChange == null)
                                {
                                    // Add a new column change to update the referencing column
                                    // Create a new column with the same type as the new referenced column
                                    // Preserve all other properties from the current column
                                    SqlTableColumn updatedColumn = currentFkColumn with { SqlType = newRefColumn.SqlType };
                                    
                                    // Determine change type - if it's a type change, it might need rebuild
                                    ColumnChangeType changeType = ColumnRebuildDetector.RequiresRebuild(
                                        currentFkColumn, updatedColumn, tablePair.Value)
                                        ? ColumnChangeType.Rebuild
                                        : ColumnChangeType.Modify;
                                    
                                    fkTableDiff.ColumnChanges.Add(new ColumnChange(changeType, currentFkColumn, updatedColumn));
                                    MigrationLogger.Log($"    [ADDED CHANGE] {fk.Table}.{fk.Column}: {changeType} (IsPrimaryKey={currentFkColumn.IsPrimaryKey})");
                                }
                                else if (existingChange.NewColumn != null && existingChange.NewColumn.SqlType != newRefColumn.SqlType)
                                {
                                    // Update existing change to use the correct target type (matching the new referenced column)
                                    // Replace the existing change with one that has the correct target column
                                    int index = fkTableDiff.ColumnChanges.IndexOf(existingChange);
                                    if (index >= 0 && existingChange.OldColumn != null)
                                    {
                                        // Update the new column to match the referenced column type
                                        SqlTableColumn updatedColumn = existingChange.NewColumn with { SqlType = newRefColumn.SqlType };
                                        
                                        ColumnChangeType changeType = ColumnRebuildDetector.RequiresRebuild(
                                            existingChange.OldColumn, updatedColumn, tablePair.Value)
                                            ? ColumnChangeType.Rebuild
                                            : ColumnChangeType.Modify;
                                        
                                        fkTableDiff.ColumnChanges[index] = new ColumnChange(changeType, existingChange.OldColumn, updatedColumn);
                                        MigrationLogger.Log($"    [UPDATED CHANGE] {fk.Table}.{fk.Column}: {changeType} (IsPrimaryKey={existingChange.OldColumn.IsPrimaryKey})");
                                    }
                                }
                            }
                            else
                            {
                                MigrationLogger.Log($"    [SKIP] {fk.Table}.{fk.Column} already matches type {newRefColumn.SqlType}");
                            }
                        }
                    }
                }
            }
        }
        
        MigrationLogger.Log("=== PropagateColumnTypeChangesToForeignKeys: Complete ===");
    }

    private static List<SequenceChange> CompareSequences(
        ConcurrentDictionary<string, SqlSequence> currentSequences,
        ConcurrentDictionary<string, SqlSequence> targetSequences,
        List<SqlSequence> newSequences,
        List<string> droppedSequenceNames)
    {
        List<SequenceChange> modifiedSequences = new List<SequenceChange>();

        // Find new sequences (in target but not in current)
        foreach (KeyValuePair<string, SqlSequence> targetSequence in targetSequences)
        {
            if (!currentSequences.ContainsKey(targetSequence.Key))
            {
                newSequences.Add(targetSequence.Value);
            }
            else if (currentSequences.TryGetValue(targetSequence.Key, out SqlSequence? currentSequence))
            {
                // Sequence exists in both - check if modified (compare definition or properties)
                if (!AreSequencesEqual(currentSequence, targetSequence.Value))
                {
                    modifiedSequences.Add(new SequenceChange(SequenceChangeType.Modify, currentSequence, targetSequence.Value));
                }
            }
        }

        // Find dropped sequences (in current but not in target)
        foreach (KeyValuePair<string, SqlSequence> currentSequence in currentSequences)
        {
            if (!targetSequences.ContainsKey(currentSequence.Key))
            {
                droppedSequenceNames.Add(currentSequence.Value.Name);
            }
        }

        return modifiedSequences;
    }

    private static List<ProcedureChange> CompareProcedures(
        ConcurrentDictionary<string, SqlStoredProcedure> currentProcedures,
        ConcurrentDictionary<string, SqlStoredProcedure> targetProcedures,
        List<SqlStoredProcedure> newProcedures,
        List<string> droppedProcedureNames)
    {
        List<ProcedureChange> modifiedProcedures = new List<ProcedureChange>();

        // Find new procedures (in target but not in current)
        foreach (KeyValuePair<string, SqlStoredProcedure> targetProcedure in targetProcedures)
        {
            if (!currentProcedures.ContainsKey(targetProcedure.Key))
            {
                newProcedures.Add(targetProcedure.Value);
            }
            else if (currentProcedures.TryGetValue(targetProcedure.Key, out SqlStoredProcedure? currentProcedure))
            {
                // Procedure exists in both - check if modified (compare definition text)
                if (!AreProceduresEqual(currentProcedure, targetProcedure.Value))
                {
                    modifiedProcedures.Add(new ProcedureChange(ProcedureChangeType.Modify, currentProcedure, targetProcedure.Value));
                }
            }
        }

        // Find dropped procedures (in current but not in target)
        foreach (KeyValuePair<string, SqlStoredProcedure> currentProcedure in currentProcedures)
        {
            if (!targetProcedures.ContainsKey(currentProcedure.Key))
            {
                droppedProcedureNames.Add(currentProcedure.Value.Name);
            }
        }

        return modifiedProcedures;
    }

    private static bool AreSequencesEqual(SqlSequence seq1, SqlSequence seq2)
    {
        // Compare all properties - if any differ, sequences are not equal
        return seq1.Name.Equals(seq2.Name, StringComparison.OrdinalIgnoreCase) &&
               seq1.DataType.Equals(seq2.DataType, StringComparison.OrdinalIgnoreCase) &&
               seq1.StartValue == seq2.StartValue &&
               seq1.Increment == seq2.Increment &&
               seq1.MinValue == seq2.MinValue &&
               seq1.MaxValue == seq2.MaxValue &&
               seq1.Cycle == seq2.Cycle &&
               seq1.CacheSize == seq2.CacheSize;
    }

    private static bool AreProceduresEqual(SqlStoredProcedure proc1, SqlStoredProcedure proc2)
    {
        // Compare definition text - normalize whitespace for comparison
        string def1 = NormalizeSqlDefinition(proc1.Definition);
        string def2 = NormalizeSqlDefinition(proc2.Definition);
        return def1.Equals(def2, StringComparison.OrdinalIgnoreCase);
    }

    private static string NormalizeSqlDefinition(string definition)
    {
        if (string.IsNullOrWhiteSpace(definition))
        {
            return string.Empty;
        }

        // Normalize whitespace: replace multiple spaces/newlines with single space, trim
        return System.Text.RegularExpressions.Regex.Replace(definition, @"\s+", " ").Trim();
    }

    private static TableDiff? CompareTables(SqlTable currentTable, SqlTable targetTable)
    {
        List<ColumnChange> columnChanges = new List<ColumnChange>();
        List<ForeignKeyChange> foreignKeyChanges = new List<ForeignKeyChange>();
        List<IndexChange> indexChanges = new List<IndexChange>();

        // Compare columns
        HashSet<string> processedColumns = new HashSet<string>();

        // Find added and modified columns
        foreach (KeyValuePair<string, SqlTableColumn> targetColumn in targetTable.Columns)
        {
            processedColumns.Add(targetColumn.Key);
            if (currentTable.Columns.TryGetValue(targetColumn.Key, out SqlTableColumn? currentColumn))
            {
                // Column exists in both - check if modified
                if (!AreColumnsEqual(currentColumn, targetColumn.Value))
                {
                    // Check if this change requires rebuild (DROP+ADD) vs simple ALTER COLUMN
                    ColumnChangeType changeType = ColumnRebuildDetector.RequiresRebuild(
                        currentColumn, targetColumn.Value, currentTable)
                        ? ColumnChangeType.Rebuild
                        : ColumnChangeType.Modify;
                    
                    columnChanges.Add(new ColumnChange(changeType, currentColumn, targetColumn.Value));
                }
            }
            else
            {
                // Column added
                columnChanges.Add(new ColumnChange(ColumnChangeType.Add, null, targetColumn.Value));
            }
        }

        // Find dropped columns
        foreach (KeyValuePair<string, SqlTableColumn> currentColumn in currentTable.Columns)
        {
            if (!processedColumns.Contains(currentColumn.Key))
            {
                columnChanges.Add(new ColumnChange(ColumnChangeType.Drop, currentColumn.Value, null));
            }
        }

        // Compare foreign keys
        // Collect all FKs from both tables
        Dictionary<string, SqlForeignKey> currentFks = new Dictionary<string, SqlForeignKey>();
        Dictionary<string, SqlForeignKey> targetFks = new Dictionary<string, SqlForeignKey>();

        foreach (SqlTableColumn column in currentTable.Columns.Values)
        {
            foreach (SqlForeignKey fk in column.ForeignKeys)
            {
                if (!currentFks.ContainsKey(fk.Name))
                {
                    currentFks[fk.Name] = fk;
                }
            }
        }

        foreach (SqlTableColumn column in targetTable.Columns.Values)
        {
            foreach (SqlForeignKey fk in column.ForeignKeys)
            {
                if (!targetFks.ContainsKey(fk.Name))
                {
                    targetFks[fk.Name] = fk;
                }
            }
        }

        // Find added and modified FKs
        foreach (KeyValuePair<string, SqlForeignKey> targetFk in targetFks)
        {
            if (currentFks.TryGetValue(targetFk.Key, out SqlForeignKey? currentFk))
            {
                // FK exists in both - check if modified
                if (!AreForeignKeysEqual(currentFk, targetFk.Value))
                {
                    foreignKeyChanges.Add(new ForeignKeyChange(ForeignKeyChangeType.Modify, currentFk, targetFk.Value));
                }
            }
            else
            {
                // FK added
                foreignKeyChanges.Add(new ForeignKeyChange(ForeignKeyChangeType.Add, null, targetFk.Value));
            }
        }

        // Find dropped FKs
        foreach (KeyValuePair<string, SqlForeignKey> currentFk in currentFks)
        {
            if (!targetFks.ContainsKey(currentFk.Key))
            {
                foreignKeyChanges.Add(new ForeignKeyChange(ForeignKeyChangeType.Drop, currentFk.Value, null));
            }
        }

        // Compare indexes
        Dictionary<string, SqlIndex> currentIndexes = currentTable.Indexes.ToDictionary(i => i.Name, StringComparer.OrdinalIgnoreCase);
        Dictionary<string, SqlIndex> targetIndexes = targetTable.Indexes.ToDictionary(i => i.Name, StringComparer.OrdinalIgnoreCase);

        // Find added and modified indexes
        foreach (KeyValuePair<string, SqlIndex> targetIndex in targetIndexes)
        {
            if (currentIndexes.TryGetValue(targetIndex.Key, out SqlIndex? currentIndex))
            {
                // Index exists in both - check if modified
                if (!AreIndexesEqual(currentIndex, targetIndex.Value))
                {
                    indexChanges.Add(new IndexChange(IndexChangeType.Modify, currentIndex, targetIndex.Value));
                }
            }
            else
            {
                // Index added
                indexChanges.Add(new IndexChange(IndexChangeType.Add, null, targetIndex.Value));
            }
        }

        // Find dropped indexes
        foreach (KeyValuePair<string, SqlIndex> currentIndex in currentIndexes)
        {
            if (!targetIndexes.ContainsKey(currentIndex.Key))
            {
                indexChanges.Add(new IndexChange(IndexChangeType.Drop, currentIndex.Value, null));
            }
        }

        // Check for column order differences even if all columns are "equal"
        // Column order is tracked via OrdinalPosition, but AreColumnsEqual doesn't compare it
        // So we need to check separately if columns are in different order
        bool hasColumnOrderDifference = false;
        if (columnChanges.Count == 0 && currentTable.Columns.Count == targetTable.Columns.Count)
        {
            // All columns are "equal" (same properties), but check if order differs
            List<SqlTableColumn> currentOrdered = currentTable.Columns.Values
                .OrderBy(c => c.OrdinalPosition)
                .ToList();
            List<SqlTableColumn> targetOrdered = targetTable.Columns.Values
                .OrderBy(c => c.OrdinalPosition)
                .ToList();
            
            if (currentOrdered.Count == targetOrdered.Count)
            {
                // Check if column names are in different order
                List<string> currentNames = currentOrdered.Select(c => c.Name).ToList();
                List<string> targetNames = targetOrdered.Select(c => c.Name).ToList();
                hasColumnOrderDifference = !currentNames.SequenceEqual(targetNames, StringComparer.OrdinalIgnoreCase);
                
                if (hasColumnOrderDifference)
                {
                    MigrationLogger.Log($"  [COLUMN ORDER DIFF] Table {currentTable.Name}:");
                    MigrationLogger.Log($"    Current order: [{string.Join(", ", currentNames)}]");
                    MigrationLogger.Log($"    Target order: [{string.Join(", ", targetNames)}]");
                }
            }
        }

        if (columnChanges.Count == 0 && foreignKeyChanges.Count == 0 && indexChanges.Count == 0 && !hasColumnOrderDifference)
        {
            return null;
        }

        return new TableDiff(targetTable.Name, columnChanges, foreignKeyChanges, indexChanges);
    }

    private static bool AreIndexesEqual(SqlIndex idx1, SqlIndex idx2)
    {
        return idx1.Name.Equals(idx2.Name, StringComparison.OrdinalIgnoreCase) &&
               idx1.IsUnique == idx2.IsUnique &&
               idx1.Columns.Count == idx2.Columns.Count &&
               idx1.Columns.SequenceEqual(idx2.Columns, StringComparer.OrdinalIgnoreCase);
    }

    private static bool AreColumnsEqual(SqlTableColumn col1, SqlTableColumn col2)
    {
        // Normalize default constraint values for comparison
        // SQL Server may store values with extra parentheses, so we normalize them
        string NormalizeDefaultValue(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return string.Empty;
            
            // Remove outer parentheses if they wrap the entire value
            string normalized = value.Trim();
            while (normalized.StartsWith('(') && normalized.EndsWith(')'))
            {
                // Check if it's balanced parentheses (simple check)
                int depth = 0;
                bool isBalanced = true;
                for (int i = 0; i < normalized.Length; i++)
                {
                    if (normalized[i] == '(') depth++;
                    else if (normalized[i] == ')') depth--;
                    if (depth == 0 && i < normalized.Length - 1)
                    {
                        isBalanced = false;
                        break;
                    }
                }
                if (isBalanced && depth == 0)
                {
                    normalized = normalized.Substring(1, normalized.Length - 2).Trim();
                }
                else
                {
                    break;
                }
            }
            return normalized;
        }
        
        return col1.Name.Equals(col2.Name, StringComparison.OrdinalIgnoreCase) &&
               col1.IsNullable == col2.IsNullable &&
               col1.IsIdentity == col2.IsIdentity &&
               col1.IsComputed == col2.IsComputed &&
               col1.IsPrimaryKey == col2.IsPrimaryKey &&
               col1.SqlType == col2.SqlType &&
               col1.Length == col2.Length &&
               col1.Precision == col2.Precision &&
               col1.Scale == col2.Scale &&
               (col1.ComputedSql ?? string.Empty) == (col2.ComputedSql ?? string.Empty) &&
               NormalizeDefaultValue(col1.DefaultConstraintValue) == NormalizeDefaultValue(col2.DefaultConstraintValue);
    }

    private static bool AreForeignKeysEqual(SqlForeignKey fk1, SqlForeignKey fk2)
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
}
