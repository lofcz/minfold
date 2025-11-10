using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Minfold;

public static class MigrationSchemaSnapshot
{
    public static async Task SaveSchemaSnapshot(ConcurrentDictionary<string, SqlTable> schema, string migrationName, string codePath)
    {
        string migrationsPath = MigrationUtilities.GetMigrationsPath(codePath);
        string snapshotPath = Path.Combine(migrationsPath, $"{migrationName}.schema.json");

        JsonSerializerOptions options = new JsonSerializerOptions
        {
            WriteIndented = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };

        string json = JsonSerializer.Serialize(schema, options);
        await File.WriteAllTextAsync(snapshotPath, json);
    }

    public static async Task<ResultOrException<ConcurrentDictionary<string, SqlTable>>> LoadSchemaSnapshot(string migrationName, string codePath)
    {
        try
        {
            string migrationsPath = MigrationUtilities.GetMigrationsPath(codePath);
            string snapshotPath = Path.Combine(migrationsPath, $"{migrationName}.schema.json");

            if (!File.Exists(snapshotPath))
            {
                return new ResultOrException<ConcurrentDictionary<string, SqlTable>>(null, new FileNotFoundException($"Schema snapshot not found: {snapshotPath}"));
            }

            string json = await File.ReadAllTextAsync(snapshotPath);
            ConcurrentDictionary<string, SqlTable>? schema = JsonSerializer.Deserialize<ConcurrentDictionary<string, SqlTable>>(json);

            if (schema is null)
            {
                return new ResultOrException<ConcurrentDictionary<string, SqlTable>>(null, new Exception("Failed to deserialize schema snapshot"));
            }

            return new ResultOrException<ConcurrentDictionary<string, SqlTable>>(schema, null);
        }
        catch (Exception ex)
        {
            return new ResultOrException<ConcurrentDictionary<string, SqlTable>>(null, ex);
        }
    }

    public static async Task<ResultOrException<ConcurrentDictionary<string, SqlTable>>> GetTargetSchemaFromMigrations(string codePath, List<string> appliedMigrations)
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
                    return new ResultOrException<ConcurrentDictionary<string, SqlTable>>(new ConcurrentDictionary<string, SqlTable>(), null);
                }

                // Use the first migration's snapshot (ordered by timestamp)
                string firstMigrationName = allMigrations[0].MigrationName;
                ResultOrException<ConcurrentDictionary<string, SqlTable>> snapshotResult = await LoadSchemaSnapshot(firstMigrationName, codePath);

                if (snapshotResult.Exception is not null || snapshotResult.Result is null)
                {
                    // If first migration snapshot doesn't exist, return empty schema
                    return new ResultOrException<ConcurrentDictionary<string, SqlTable>>(new ConcurrentDictionary<string, SqlTable>(), null);
                }

                return snapshotResult;
            }

            // Load the schema snapshot from the last applied migration
            string lastMigration = appliedMigrations[appliedMigrations.Count - 1];
            ResultOrException<ConcurrentDictionary<string, SqlTable>> snapshotResult2 = await LoadSchemaSnapshot(lastMigration, codePath);

            if (snapshotResult2.Exception is not null || snapshotResult2.Result is null)
            {
                return new ResultOrException<ConcurrentDictionary<string, SqlTable>>(null, snapshotResult2.Exception ?? new Exception("Failed to load schema snapshot"));
            }

            return snapshotResult2;
        }
        catch (Exception ex)
        {
            return new ResultOrException<ConcurrentDictionary<string, SqlTable>>(null, ex);
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

                newTarget[tableDiff.TableName.ToLowerInvariant()] = new SqlTable(table.Name, newColumns, table.Indexes);
            }
        }

        return newTarget;
    }
}

