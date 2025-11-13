using System.Collections.Concurrent;
using System.IO.Compression;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Minfold;

public static class MigrationSchemaSnapshot
{
    private const int CurrentSchemaVersion = 1;

    public static async Task SaveSchemaSnapshot(
        ConcurrentDictionary<string, SqlTable> schema, 
        string migrationName, 
        string codePath,
        ConcurrentDictionary<string, SqlSequence>? sequences = null,
        ConcurrentDictionary<string, SqlStoredProcedure>? procedures = null)
    {
        string migrationsPath = MigrationUtilities.GetMigrationsPath(codePath);
        string migrationFolder = Path.Combine(migrationsPath, migrationName);
        string snapshotPath = Path.Combine(migrationFolder, "schema.bin");

        JsonSerializerOptions options = new JsonSerializerOptions
        {
            WriteIndented = false, // No indentation for binary format
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };

        // Create a snapshot object that includes version, tables, sequences, and procedures
        var snapshot = new
        {
            Version = CurrentSchemaVersion,
            Tables = schema,
            Sequences = sequences ?? new ConcurrentDictionary<string, SqlSequence>(),
            Procedures = procedures ?? new ConcurrentDictionary<string, SqlStoredProcedure>()
        };

        // Serialize to JSON bytes, then compress with GZip
        byte[] jsonBytes = JsonSerializer.SerializeToUtf8Bytes(snapshot, options);
        
        using (MemoryStream compressedStream = new MemoryStream())
        {
            using (GZipStream gzipStream = new GZipStream(compressedStream, CompressionLevel.Optimal))
            {
                await gzipStream.WriteAsync(jsonBytes);
            }
            byte[] compressedBytes = compressedStream.ToArray();
            await File.WriteAllBytesAsync(snapshotPath, compressedBytes);
        }
    }

    public static async Task<ResultOrException<(ConcurrentDictionary<string, SqlTable> Tables, ConcurrentDictionary<string, SqlSequence> Sequences, ConcurrentDictionary<string, SqlStoredProcedure> Procedures)>> LoadSchemaSnapshot(string migrationName, string codePath)
    {
        try
        {
            string migrationsPath = MigrationUtilities.GetMigrationsPath(codePath);
            string migrationFolder = Path.Combine(migrationsPath, migrationName);
            string snapshotPath = Path.Combine(migrationFolder, "schema.bin");

            if (!File.Exists(snapshotPath))
            {
                return new ResultOrException<(ConcurrentDictionary<string, SqlTable>, ConcurrentDictionary<string, SqlSequence>, ConcurrentDictionary<string, SqlStoredProcedure>)>(
                    (new ConcurrentDictionary<string, SqlTable>(), new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>()), 
                    new FileNotFoundException($"Schema snapshot not found: {snapshotPath}"));
            }

            // Decompress binary file
            byte[] compressedBytes = await File.ReadAllBytesAsync(snapshotPath);
            string json;
            
            using (MemoryStream compressedStream = new MemoryStream(compressedBytes))
            using (GZipStream gzipStream = new GZipStream(compressedStream, CompressionMode.Decompress))
            using (MemoryStream decompressedStream = new MemoryStream())
            {
                await gzipStream.CopyToAsync(decompressedStream);
                json = System.Text.Encoding.UTF8.GetString(decompressedStream.ToArray());
            }
            
            // Deserialize snapshot
            var snapshot = JsonSerializer.Deserialize<JsonElement>(json);
            
            // Check version for forward compatibility
            int version = 1; // Default to version 1 for backward compatibility
            if (snapshot.TryGetProperty("Version", out JsonElement versionElement) && versionElement.ValueKind == JsonValueKind.Number)
            {
                version = versionElement.GetInt32();
            }

            // Handle different versions if needed in the future
            if (version > CurrentSchemaVersion)
            {
                return new ResultOrException<(ConcurrentDictionary<string, SqlTable>, ConcurrentDictionary<string, SqlSequence>, ConcurrentDictionary<string, SqlStoredProcedure>)>(
                    (new ConcurrentDictionary<string, SqlTable>(), new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>()), 
                    new NotSupportedException($"Schema snapshot version {version} is not supported. Maximum supported version is {CurrentSchemaVersion}."));
            }

            ConcurrentDictionary<string, SqlTable> tables = new ConcurrentDictionary<string, SqlTable>();
            ConcurrentDictionary<string, SqlSequence> sequences = new ConcurrentDictionary<string, SqlSequence>(StringComparer.OrdinalIgnoreCase);
            ConcurrentDictionary<string, SqlStoredProcedure> procedures = new ConcurrentDictionary<string, SqlStoredProcedure>(StringComparer.OrdinalIgnoreCase);

            if (snapshot.TryGetProperty("Tables", out JsonElement tablesElement))
            {
                ConcurrentDictionary<string, SqlTable>? deserializedTables = JsonSerializer.Deserialize<ConcurrentDictionary<string, SqlTable>>(tablesElement.GetRawText());
                tables = deserializedTables is null
                    ? new ConcurrentDictionary<string, SqlTable>()
                    : new ConcurrentDictionary<string, SqlTable>(deserializedTables, StringComparer.OrdinalIgnoreCase);
            }

            if (snapshot.TryGetProperty("Sequences", out JsonElement sequencesElement))
            {
                Dictionary<string, SqlSequence>? deserializedSequences = JsonSerializer.Deserialize<Dictionary<string, SqlSequence>>(sequencesElement.GetRawText());
                if (deserializedSequences is not null)
                {
                    foreach (KeyValuePair<string, SqlSequence> kvp in deserializedSequences)
                    {
                        sequences[kvp.Key] = kvp.Value;
                    }
                }
            }

            if (snapshot.TryGetProperty("Procedures", out JsonElement proceduresElement))
            {
                Dictionary<string, SqlStoredProcedure>? deserializedProcedures = JsonSerializer.Deserialize<Dictionary<string, SqlStoredProcedure>>(proceduresElement.GetRawText());
                if (deserializedProcedures is not null)
                {
                    foreach (KeyValuePair<string, SqlStoredProcedure> kvp in deserializedProcedures)
                    {
                        procedures[kvp.Key] = kvp.Value;
                    }
                }
            }

            return new ResultOrException<(ConcurrentDictionary<string, SqlTable>, ConcurrentDictionary<string, SqlSequence>, ConcurrentDictionary<string, SqlStoredProcedure>)>(
                (tables, sequences, procedures), null);
        }
        catch (Exception ex)
        {
            return new ResultOrException<(ConcurrentDictionary<string, SqlTable>, ConcurrentDictionary<string, SqlSequence>, ConcurrentDictionary<string, SqlStoredProcedure>)>(
                (new ConcurrentDictionary<string, SqlTable>(), new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>()), 
                ex);
        }
    }

    public static async Task<ResultOrException<(ConcurrentDictionary<string, SqlTable> Tables, ConcurrentDictionary<string, SqlSequence> Sequences, ConcurrentDictionary<string, SqlStoredProcedure> Procedures)>> GetTargetSchemaFromMigrations(string codePath, List<string> appliedMigrations)
    {
        try
        {
            // If no migrations are recorded as applied, use the first migration's snapshot as the target
            // This handles the case where we're generating an incremental migration from a database
            // that has changes but no migrations recorded (e.g., manual changes before migrations were set up)
            if (appliedMigrations.Count == 0)
            {
                // Get all migration files (ordered by timestamp) and use the first one's snapshot
                List<MigrationInfo> allMigrations = MigrationApplier.GetMigrationFiles(codePath);
                
                if (allMigrations.Count == 0)
                {
                    return new ResultOrException<(ConcurrentDictionary<string, SqlTable>, ConcurrentDictionary<string, SqlSequence>, ConcurrentDictionary<string, SqlStoredProcedure>)>(
                        (new ConcurrentDictionary<string, SqlTable>(), new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>()), null);
                }

                // Use the first migration's snapshot (ordered by timestamp)
                string firstMigrationName = allMigrations[0].MigrationName;
                ResultOrException<(ConcurrentDictionary<string, SqlTable>, ConcurrentDictionary<string, SqlSequence>, ConcurrentDictionary<string, SqlStoredProcedure>)> snapshotResult = await LoadSchemaSnapshot(firstMigrationName, codePath);

                if (snapshotResult.Exception is not null)
                {
                    // If first migration snapshot doesn't exist, return empty schema
                    return new ResultOrException<(ConcurrentDictionary<string, SqlTable>, ConcurrentDictionary<string, SqlSequence>, ConcurrentDictionary<string, SqlStoredProcedure>)>(
                        (new ConcurrentDictionary<string, SqlTable>(), new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>()), null);
                }

                return snapshotResult;
            }

            // Load the schema snapshot from the last applied migration
            string lastMigration = appliedMigrations[appliedMigrations.Count - 1];
            ResultOrException<(ConcurrentDictionary<string, SqlTable>, ConcurrentDictionary<string, SqlSequence>, ConcurrentDictionary<string, SqlStoredProcedure>)> snapshotResult2 = await LoadSchemaSnapshot(lastMigration, codePath);

            if (snapshotResult2.Exception is not null)
            {
                return new ResultOrException<(ConcurrentDictionary<string, SqlTable>, ConcurrentDictionary<string, SqlSequence>, ConcurrentDictionary<string, SqlStoredProcedure>)>(
                    (new ConcurrentDictionary<string, SqlTable>(), new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>()), 
                    snapshotResult2.Exception);
            }

            return snapshotResult2;
        }
        catch (Exception ex)
        {
            return new ResultOrException<(ConcurrentDictionary<string, SqlTable>, ConcurrentDictionary<string, SqlSequence>, ConcurrentDictionary<string, SqlStoredProcedure>)>(
                (new ConcurrentDictionary<string, SqlTable>(), new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>()), ex);
        }
    }

    public static ConcurrentDictionary<string, SqlTable> ApplySchemaDiffToTarget(ConcurrentDictionary<string, SqlTable> targetSchema, SchemaDiff diff)
    {
        // Create a copy of the target schema
        ConcurrentDictionary<string, SqlTable> newTarget = new ConcurrentDictionary<string, SqlTable>();
        foreach (KeyValuePair<string, SqlTable> kvp in targetSchema)
        {
            // Deep copy the table
            Dictionary<string, SqlTableColumn> newColumns = new Dictionary<string, SqlTableColumn>();
            foreach (KeyValuePair<string, SqlTableColumn> colKvp in kvp.Value.Columns)
            {
                newColumns[colKvp.Key] = colKvp.Value with
                {
                    ForeignKeys = [..colKvp.Value.ForeignKeys]
                };
            }
            newTarget[kvp.Key] = new SqlTable(kvp.Value.Name, newColumns, kvp.Value.Indexes);
        }

        // Apply new tables
        foreach (SqlTable newTable in diff.NewTables)
        {
            newTarget[newTable.Name.ToLowerInvariant()] = newTable;
        }

        // Remove dropped tables
        foreach (string droppedTableName in diff.DroppedTableNames)
        {
            newTarget.TryRemove(droppedTableName.ToLowerInvariant(), out _);
        }

        // Apply table modifications
        foreach (TableDiff tableDiff in diff.ModifiedTables)
        {
            if (newTarget.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? table))
            {
                Dictionary<string, SqlTableColumn> newColumns = new Dictionary<string, SqlTableColumn>(table.Columns);

                // Track which columns will be dropped (to remove them and update positions)
                HashSet<string> droppedColumnNames = tableDiff.ColumnChanges
                    .Where(c => c.ChangeType == ColumnChangeType.Drop && c.OldColumn != null)
                    .Select(c => c.OldColumn!.Name.ToLowerInvariant())
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                // Apply column changes in order: Add first, then Modify/Rebuild, then Drop
                // This ensures that columns are added before they might be dropped (e.g., in down scripts)
                List<ColumnChange> addChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Add).ToList();
                List<ColumnChange> modifyChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Modify).ToList();
                List<ColumnChange> rebuildChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Rebuild).ToList();
                List<ColumnChange> dropChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Drop).ToList();
                
                // Process Add operations first
                foreach (ColumnChange change in addChanges)
                {
                    if (change.NewColumn != null)
                    {
                        newColumns[change.NewColumn.Name.ToLowerInvariant()] = change.NewColumn;
                    }
                }
                
                // Then process Modify operations
                foreach (ColumnChange change in modifyChanges)
                {
                    if (change.NewColumn != null)
                    {
                        // For Modify with DROP+ADD (identity/computed changes), the column is effectively
                        // dropped and re-added, so it goes to the end
                        newColumns[change.NewColumn.Name.ToLowerInvariant()] = change.NewColumn;
                    }
                }
                
                // Then process Rebuild operations (always DROP+ADD)
                foreach (ColumnChange change in rebuildChanges)
                {
                    if (change.NewColumn != null)
                    {
                        // Rebuild changes always DROP+ADD, so column goes to the end
                        newColumns[change.NewColumn.Name.ToLowerInvariant()] = change.NewColumn;
                    }
                }
                
                // Finally process Drop operations (after Add and Modify, so we don't drop columns that were just added)
                foreach (ColumnChange change in dropChanges)
                {
                    if (change.OldColumn != null)
                    {
                        bool removed = newColumns.Remove(change.OldColumn.Name.ToLowerInvariant());
                        MigrationLogger.Log($"    ApplySchemaDiffToTarget: Removed column '{change.OldColumn.Name}' from {tableDiff.TableName}: {removed}");
                    }
                }
                
                MigrationLogger.Log($"    ApplySchemaDiffToTarget: After processing changes, {tableDiff.TableName} has columns: [{string.Join(", ", newColumns.Values.OrderBy(c => c.OrdinalPosition).Select(c => c.Name))}]");

                // Update OrdinalPosition to reflect actual SQL Server physical order:
                // 1. Existing columns (not dropped, not modified with DROP+ADD) keep their relative positions
                // 2. Modified columns that require DROP+ADD go to the end (like new columns)
                // 3. New columns go to the end
                
                // Identify columns that require DROP+ADD (identity or computed changes, or Rebuild changes)
                HashSet<string> dropAddColumnNames = tableDiff.ColumnChanges
                    .Where(c => 
                        (c.ChangeType == ColumnChangeType.Modify && c.OldColumn != null && c.NewColumn != null &&
                         ((c.OldColumn.IsIdentity != c.NewColumn.IsIdentity) || (c.OldColumn.IsComputed != c.NewColumn.IsComputed))) ||
                        (c.ChangeType == ColumnChangeType.Rebuild && c.NewColumn != null))
                    .Select(c => c.NewColumn!.Name.ToLowerInvariant())
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                // Separate columns into: existing (keep order) and new/drop-add (go to end)
                // Process columns in the order they appear in ColumnChanges to match SQL Server's physical order
                List<SqlTableColumn> existingColumns = new List<SqlTableColumn>();
                List<SqlTableColumn> newOrDropAddColumns = new List<SqlTableColumn>();
                HashSet<string> processedColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                // Get original column order from target schema (before changes)
                List<SqlTableColumn> originalOrderedColumns = table.Columns.Values
                    .OrderBy(c => c.OrdinalPosition)
                    .ToList();

                // First, process existing columns that weren't drop-added (keep their relative positions)
                foreach (SqlTableColumn originalCol in originalOrderedColumns)
                {
                    if (droppedColumnNames.Contains(originalCol.Name.ToLowerInvariant()))
                    {
                        // Column was dropped, skip it
                        continue;
                    }

                    if (newColumns.TryGetValue(originalCol.Name.ToLowerInvariant(), out SqlTableColumn? updatedCol))
                    {
                        if (!dropAddColumnNames.Contains(updatedCol.Name.ToLowerInvariant()))
                        {
                            // Column exists and wasn't drop-added, keep its relative position
                            existingColumns.Add(updatedCol);
                            processedColumnNames.Add(updatedCol.Name.ToLowerInvariant());
                        }
                    }
                }

                // Then, process columns in the order they appear in ColumnChanges to match SQL Server's physical order
                // IMPORTANT: Process Add operations first, then Modify operations with DROP+ADD
                // This ensures that if we Add 'text' then Modify 'id' (DROP+ADD), the order is [text, id]
                // SQL Server processes operations in the order they appear in the migration script:
                // 1. Add operations add columns at the end
                // 2. Modify operations with DROP+ADD drop the column, then add it back at the end
                
                // First, process all Add operations
                foreach (ColumnChange change in tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Add))
                {
                    if (change.NewColumn != null && newColumns.TryGetValue(change.NewColumn.Name.ToLowerInvariant(), out SqlTableColumn? newCol))
                    {
                        if (!processedColumnNames.Contains(newCol.Name.ToLowerInvariant()))
                        {
                            newOrDropAddColumns.Add(newCol);
                            processedColumnNames.Add(newCol.Name.ToLowerInvariant());
                        }
                    }
                }
                
                // Then, process all Modify operations with DROP+ADD (these go after Add operations)
                foreach (ColumnChange change in tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Modify))
                {
                    if (change.NewColumn != null && dropAddColumnNames.Contains(change.NewColumn.Name.ToLowerInvariant()))
                    {
                        if (newColumns.TryGetValue(change.NewColumn.Name.ToLowerInvariant(), out SqlTableColumn? modifiedCol))
                        {
                            if (!processedColumnNames.Contains(modifiedCol.Name.ToLowerInvariant()))
                            {
                                newOrDropAddColumns.Add(modifiedCol);
                                processedColumnNames.Add(modifiedCol.Name.ToLowerInvariant());
                            }
                        }
                    }
                }
                
                // Then, process all Rebuild operations (these go after Modify operations with DROP+ADD)
                foreach (ColumnChange change in tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Rebuild))
                {
                    if (change.NewColumn != null && newColumns.TryGetValue(change.NewColumn.Name.ToLowerInvariant(), out SqlTableColumn? rebuiltCol))
                    {
                        if (!processedColumnNames.Contains(rebuiltCol.Name.ToLowerInvariant()))
                        {
                            newOrDropAddColumns.Add(rebuiltCol);
                            processedColumnNames.Add(rebuiltCol.Name.ToLowerInvariant());
                        }
                    }
                }

                // Reassign OrdinalPosition values: existing columns first, then new/drop-add columns
                int position = 1;
                Dictionary<string, SqlTableColumn> reorderedColumns = new Dictionary<string, SqlTableColumn>(StringComparer.OrdinalIgnoreCase);

                foreach (SqlTableColumn col in existingColumns)
                {
                    reorderedColumns[col.Name.ToLowerInvariant()] = col with { OrdinalPosition = position++ };
                }

                foreach (SqlTableColumn col in newOrDropAddColumns)
                {
                    reorderedColumns[col.Name.ToLowerInvariant()] = col with { OrdinalPosition = position++ };
                }

                // Replace newColumns with reordered columns
                newColumns = reorderedColumns;

                // Apply FK changes
                foreach (ForeignKeyChange fkChange in tableDiff.ForeignKeyChanges)
                {
                    if (fkChange.ChangeType == ForeignKeyChangeType.Add && fkChange.NewForeignKey != null)
                    {
                        // Add FK to the column
                        if (newColumns.TryGetValue(fkChange.NewForeignKey.Column.ToLowerInvariant(), out SqlTableColumn? column))
                        {
                            column.ForeignKeys.Add(fkChange.NewForeignKey);
                        }
                    }
                    else if (fkChange.ChangeType == ForeignKeyChangeType.Drop && fkChange.OldForeignKey != null)
                    {
                        // Remove FK from the column
                        if (newColumns.TryGetValue(fkChange.OldForeignKey.Column.ToLowerInvariant(), out SqlTableColumn? column))
                        {
                            column.ForeignKeys.RemoveAll(fk => fk.Name == fkChange.OldForeignKey!.Name);
                        }
                    }
                    else if (fkChange.ChangeType == ForeignKeyChangeType.Modify && fkChange.NewForeignKey != null && fkChange.OldForeignKey != null)
                    {
                        // Replace FK
                        if (newColumns.TryGetValue(fkChange.NewForeignKey.Column.ToLowerInvariant(), out SqlTableColumn? column))
                        {
                            int index = column.ForeignKeys.FindIndex(fk => fk.Name == fkChange.OldForeignKey!.Name);
                            if (index >= 0)
                            {
                                column.ForeignKeys[index] = fkChange.NewForeignKey;
                            }
                        }
                    }
                }

                // Apply index changes
                List<SqlIndex> newIndexes = new List<SqlIndex>(table.Indexes);
                foreach (IndexChange indexChange in tableDiff.IndexChanges)
                {
                    if (indexChange.ChangeType == IndexChangeType.Add && indexChange.NewIndex != null)
                    {
                        // Add new index
                        newIndexes.Add(indexChange.NewIndex);
                    }
                    else if (indexChange.ChangeType == IndexChangeType.Drop && indexChange.OldIndex != null)
                    {
                        // Remove dropped index
                        newIndexes.RemoveAll(idx => idx.Name.Equals(indexChange.OldIndex.Name, StringComparison.OrdinalIgnoreCase));
                    }
                    else if (indexChange.ChangeType == IndexChangeType.Modify && indexChange.NewIndex != null && indexChange.OldIndex != null)
                    {
                        // Replace modified index
                        int index = newIndexes.FindIndex(idx => idx.Name.Equals(indexChange.OldIndex.Name, StringComparison.OrdinalIgnoreCase));
                        if (index >= 0)
                        {
                            newIndexes[index] = indexChange.NewIndex;
                        }
                    }
                }

                newTarget[tableDiff.TableName.ToLowerInvariant()] = new SqlTable(table.Name, newColumns, newIndexes);
            }
        }

        return newTarget;
    }

    public static ConcurrentDictionary<string, SqlSequence> ApplySequenceDiffToTarget(ConcurrentDictionary<string, SqlSequence> targetSequences, SchemaDiff diff)
    {
        ConcurrentDictionary<string, SqlSequence> newTarget = new ConcurrentDictionary<string, SqlSequence>(targetSequences, StringComparer.OrdinalIgnoreCase);

        // Add new sequences
        foreach (SqlSequence newSequence in diff.NewSequences)
        {
            newTarget[newSequence.Name.ToLowerInvariant()] = newSequence;
        }

        // Remove dropped sequences
        foreach (string droppedSequenceName in diff.DroppedSequenceNames)
        {
            newTarget.TryRemove(droppedSequenceName.ToLowerInvariant(), out _);
        }

        // Apply sequence modifications (drop old, add new)
        foreach (SequenceChange sequenceChange in diff.ModifiedSequences)
        {
            if (sequenceChange.ChangeType == SequenceChangeType.Modify && sequenceChange.OldSequence != null && sequenceChange.NewSequence != null)
            {
                newTarget.TryRemove(sequenceChange.OldSequence.Name.ToLowerInvariant(), out _);
                newTarget[sequenceChange.NewSequence.Name.ToLowerInvariant()] = sequenceChange.NewSequence;
            }
        }

        return newTarget;
    }

    public static ConcurrentDictionary<string, SqlStoredProcedure> ApplyProcedureDiffToTarget(ConcurrentDictionary<string, SqlStoredProcedure> targetProcedures, SchemaDiff diff)
    {
        ConcurrentDictionary<string, SqlStoredProcedure> newTarget = new ConcurrentDictionary<string, SqlStoredProcedure>(targetProcedures, StringComparer.OrdinalIgnoreCase);

        // Add new procedures
        foreach (SqlStoredProcedure newProcedure in diff.NewProcedures)
        {
            newTarget[newProcedure.Name.ToLowerInvariant()] = newProcedure;
        }

        // Remove dropped procedures
        foreach (string droppedProcedureName in diff.DroppedProcedureNames)
        {
            newTarget.TryRemove(droppedProcedureName.ToLowerInvariant(), out _);
        }

        // Apply procedure modifications (drop old, add new)
        foreach (ProcedureChange procedureChange in diff.ModifiedProcedures)
        {
            if (procedureChange.ChangeType == ProcedureChangeType.Modify && procedureChange.OldProcedure != null && procedureChange.NewProcedure != null)
            {
                newTarget.TryRemove(procedureChange.OldProcedure.Name.ToLowerInvariant(), out _);
                newTarget[procedureChange.NewProcedure.Name.ToLowerInvariant()] = procedureChange.NewProcedure;
            }
        }

        return newTarget;
    }
}

