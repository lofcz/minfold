using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Minfold;

public static class MigrationSchemaSnapshot
{
    public static async Task SaveSchemaSnapshot(
        ConcurrentDictionary<string, SqlTable> schema, 
        string migrationName, 
        string codePath,
        ConcurrentDictionary<string, SqlSequence>? sequences = null,
        ConcurrentDictionary<string, SqlStoredProcedure>? procedures = null)
    {
        string migrationsPath = MigrationUtilities.GetMigrationsPath(codePath);
        string snapshotPath = Path.Combine(migrationsPath, $"{migrationName}.schema.json");

        JsonSerializerOptions options = new JsonSerializerOptions
        {
            WriteIndented = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };

        // Create a snapshot object that includes tables, sequences, and procedures
        var snapshot = new
        {
            Tables = schema,
            Sequences = sequences ?? new ConcurrentDictionary<string, SqlSequence>(),
            Procedures = procedures ?? new ConcurrentDictionary<string, SqlStoredProcedure>()
        };

        string json = JsonSerializer.Serialize(snapshot, options);
        await File.WriteAllTextAsync(snapshotPath, json);
    }

    public static async Task<ResultOrException<(ConcurrentDictionary<string, SqlTable> Tables, ConcurrentDictionary<string, SqlSequence> Sequences, ConcurrentDictionary<string, SqlStoredProcedure> Procedures)>> LoadSchemaSnapshot(string migrationName, string codePath)
    {
        try
        {
            string migrationsPath = MigrationUtilities.GetMigrationsPath(codePath);
            string snapshotPath = Path.Combine(migrationsPath, $"{migrationName}.schema.json");

            if (!File.Exists(snapshotPath))
            {
                return new ResultOrException<(ConcurrentDictionary<string, SqlTable>, ConcurrentDictionary<string, SqlSequence>, ConcurrentDictionary<string, SqlStoredProcedure>)>(
                    (new ConcurrentDictionary<string, SqlTable>(), new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>()), 
                    new FileNotFoundException($"Schema snapshot not found: {snapshotPath}"));
            }

            string json = await File.ReadAllTextAsync(snapshotPath);
            
            // Try to deserialize as new format (with sequences/procedures) first
            try
            {
                var snapshot = JsonSerializer.Deserialize<JsonElement>(json);
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
                else
                {
                    // Old format - just tables
                    ConcurrentDictionary<string, SqlTable>? deserializedTables = JsonSerializer.Deserialize<ConcurrentDictionary<string, SqlTable>>(json);
                    tables = deserializedTables is null
                        ? new ConcurrentDictionary<string, SqlTable>(StringComparer.OrdinalIgnoreCase)
                        : new ConcurrentDictionary<string, SqlTable>(deserializedTables, StringComparer.OrdinalIgnoreCase);
                }

                if (snapshot.TryGetProperty("Sequences", out JsonElement sequencesElement))
                {
                    Dictionary<string, SqlSequence>? deserializedSequences = JsonSerializer.Deserialize<Dictionary<string, SqlSequence>>(sequencesElement.GetRawText());
                    sequences = new ConcurrentDictionary<string, SqlSequence>(StringComparer.OrdinalIgnoreCase);
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
                    procedures = new ConcurrentDictionary<string, SqlStoredProcedure>(StringComparer.OrdinalIgnoreCase);
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
            catch
            {
                // Fallback to old format (just tables)
                ConcurrentDictionary<string, SqlTable>? schema = JsonSerializer.Deserialize<ConcurrentDictionary<string, SqlTable>>(json);
                if (schema is null)
                {
                    return new ResultOrException<(ConcurrentDictionary<string, SqlTable>, ConcurrentDictionary<string, SqlSequence>, ConcurrentDictionary<string, SqlStoredProcedure>)>(
                        (new ConcurrentDictionary<string, SqlTable>(), new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>()), 
                        new Exception("Failed to deserialize schema snapshot"));
                }

                return new ResultOrException<(ConcurrentDictionary<string, SqlTable>, ConcurrentDictionary<string, SqlSequence>, ConcurrentDictionary<string, SqlStoredProcedure>)>(
                    (new ConcurrentDictionary<string, SqlTable>(schema, StringComparer.OrdinalIgnoreCase),
                     new ConcurrentDictionary<string, SqlSequence>(StringComparer.OrdinalIgnoreCase),
                     new ConcurrentDictionary<string, SqlStoredProcedure>(StringComparer.OrdinalIgnoreCase)), null);
            }
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

                // Apply column changes
                foreach (ColumnChange change in tableDiff.ColumnChanges)
                {
                    if (change.ChangeType == ColumnChangeType.Add && change.NewColumn != null)
                    {
                        newColumns[change.NewColumn.Name.ToLowerInvariant()] = change.NewColumn;
                    }
                    else if (change.ChangeType == ColumnChangeType.Drop && change.OldColumn != null)
                    {
                        newColumns.Remove(change.OldColumn.Name.ToLowerInvariant());
                    }
                    else if (change.ChangeType == ColumnChangeType.Modify && change.NewColumn != null)
                    {
                        newColumns[change.NewColumn.Name.ToLowerInvariant()] = change.NewColumn;
                    }
                }

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

