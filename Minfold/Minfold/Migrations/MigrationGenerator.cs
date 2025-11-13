using System.Collections.Concurrent;
using System.Text;

namespace Minfold;

public class MinfoldMigrationDbUpToDateException : Exception
{
    public MinfoldMigrationDbUpToDateException() : base("Database is already up to date.")
    {
    }
}

public static class MigrationGenerator
{
    public static string GetMigrationsPath(string codePath)
    {
        return MigrationUtilities.GetMigrationsPath(codePath);
    }

    public static string GetNextMigrationTimestamp(string codePath)
    {
        return MigrationUtilities.GetNextMigrationTimestamp(codePath);
    }

    public static string FormatMigrationScript(string script)
    {
        return MigrationUtilities.FormatMigrationScript(script);
    }

    public static async Task<ResultOrException<MigrationGenerationResult>> GenerateInitialMigration(string sqlConn, string dbName, string codePath, string description, List<string>? schemas = null)
    {
        try
        {
            string migrationsPath = MigrationUtilities.GetMigrationsPath(codePath);

            // Check if migrations folder exists and has folders
            if (Directory.Exists(migrationsPath))
            {
                string[] existingMigrations = Directory.GetDirectories(migrationsPath);
                if (existingMigrations.Length > 0)
                {
                    return new ResultOrException<MigrationGenerationResult>(null, new InvalidOperationException("Migrations already exist. Cannot generate initial migration."));
                }
            }

            Directory.CreateDirectory(migrationsPath);

            List<string> allowedSchemas = schemas ?? ["dbo"];
            SqlService sqlService = new SqlService(sqlConn);
            ResultOrException<ConcurrentDictionary<string, SqlTable>> schemaResult = await sqlService.GetSchema(dbName, null, ["__MinfoldMigrations"], allowedSchemas);

            if (schemaResult.Exception is not null || schemaResult.Result is null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, schemaResult.Exception ?? new Exception("Failed to get database schema"));
            }

            // Get sequences and procedures
            ResultOrException<ConcurrentDictionary<string, SqlSequence>> sequencesResult = await sqlService.GetSequences(dbName, allowedSchemas);
            if (sequencesResult.Exception is not null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, sequencesResult.Exception);
            }

            ResultOrException<ConcurrentDictionary<string, SqlStoredProcedure>> proceduresResult = await sqlService.GetStoredProcedures(dbName, allowedSchemas);
            if (proceduresResult.Exception is not null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, proceduresResult.Exception);
            }

            ConcurrentDictionary<string, SqlSequence> sequences = sequencesResult.Result ?? new ConcurrentDictionary<string, SqlSequence>(StringComparer.OrdinalIgnoreCase);
            ConcurrentDictionary<string, SqlStoredProcedure> procedures = proceduresResult.Result ?? new ConcurrentDictionary<string, SqlStoredProcedure>(StringComparer.OrdinalIgnoreCase);

            // Get foreign keys with full metadata
            ResultOrException<Dictionary<string, List<SqlForeignKey>>> fksResult = await sqlService.GetForeignKeys(schemaResult.Result.Keys.ToList());
            if (fksResult.Exception is not null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, fksResult.Exception);
            }

            // Attach foreign keys to tables
            foreach (KeyValuePair<string, List<SqlForeignKey>> fkList in fksResult.Result ?? new Dictionary<string, List<SqlForeignKey>>())
            {
                if (schemaResult.Result.TryGetValue(fkList.Key, out SqlTable? table))
                {
                    foreach (SqlForeignKey fk in fkList.Value)
                    {
                        if (table.Columns.TryGetValue(fk.Column.ToLowerInvariant(), out SqlTableColumn? column))
                        {
                            column.ForeignKeys.Add(fk);
                        }
                    }
                }
            }

            // Generate up and down scripts using dedicated generators
            string upScript = GenerateInitialUpScript.Generate(schemaResult.Result, sequences, procedures);
            string downScript = GenerateInitialDownScript.Generate(schemaResult.Result, sequences, procedures);

            string timestamp = MigrationUtilities.GetNextMigrationTimestamp(codePath);
            string migrationName = string.IsNullOrWhiteSpace(description) ? timestamp : $"{timestamp}_{description}";
            string migrationFolder = Path.Combine(migrationsPath, migrationName);
            Directory.CreateDirectory(migrationFolder);
            
            string upScriptPath = Path.Combine(migrationFolder, "up.sql");
            string downScriptPath = Path.Combine(migrationFolder, "down.sql");

            await File.WriteAllTextAsync(upScriptPath, upScript);
            await File.WriteAllTextAsync(downScriptPath, downScript);

            // Save schema snapshot for incremental migrations (including sequences and procedures)
            await MigrationSchemaSnapshot.SaveSchemaSnapshot(schemaResult.Result, migrationName, codePath, sequences, procedures);

            return new ResultOrException<MigrationGenerationResult>(
                new MigrationGenerationResult(migrationName, upScriptPath, downScriptPath, description),
                null
            );
        }
        catch (Exception ex)
        {
            return new ResultOrException<MigrationGenerationResult>(null, ex);
        }
    }

    public static async Task<ResultOrException<MigrationGenerationResult>> GenerateIncrementalMigration(string sqlConn, string dbName, string codePath, string description, List<string>? schemas = null)
    {
        try
        {
            string migrationsPath = MigrationUtilities.GetMigrationsPath(codePath);
            Directory.CreateDirectory(migrationsPath);

            List<string> allowedSchemas = schemas ?? ["dbo"];
            // Get current database schema
            SqlService sqlService = new SqlService(sqlConn);
            ResultOrException<ConcurrentDictionary<string, SqlTable>> currentSchemaResult = await sqlService.GetSchema(dbName, null, ["__MinfoldMigrations"], allowedSchemas);

            if (currentSchemaResult.Exception is not null || currentSchemaResult.Result is null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, currentSchemaResult.Exception ?? new Exception("Failed to get current database schema"));
            }

            // Get current sequences and procedures
            ResultOrException<ConcurrentDictionary<string, SqlSequence>> currentSequencesResult = await sqlService.GetSequences(dbName, allowedSchemas);
            if (currentSequencesResult.Exception is not null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, currentSequencesResult.Exception);
            }

            ResultOrException<ConcurrentDictionary<string, SqlStoredProcedure>> currentProceduresResult = await sqlService.GetStoredProcedures(dbName, allowedSchemas);
            if (currentProceduresResult.Exception is not null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, currentProceduresResult.Exception);
            }

            ConcurrentDictionary<string, SqlSequence> currentSequences = currentSequencesResult.Result ?? new ConcurrentDictionary<string, SqlSequence>(StringComparer.OrdinalIgnoreCase);
            ConcurrentDictionary<string, SqlStoredProcedure> currentProcedures = currentProceduresResult.Result ?? new ConcurrentDictionary<string, SqlStoredProcedure>(StringComparer.OrdinalIgnoreCase);

            // Get applied migrations
            ResultOrException<List<string>> appliedMigrationsResult = await MigrationApplier.GetAppliedMigrations(sqlConn, dbName);
            if (appliedMigrationsResult.Exception is not null || appliedMigrationsResult.Result is null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, appliedMigrationsResult.Exception ?? new Exception("Failed to get applied migrations"));
            }

            // Get target schema from last migration snapshot (including sequences and procedures)
            ResultOrException<(ConcurrentDictionary<string, SqlTable> Tables, ConcurrentDictionary<string, SqlSequence> Sequences, ConcurrentDictionary<string, SqlStoredProcedure> Procedures)> targetSchemaResult = await MigrationSchemaSnapshot.GetTargetSchemaFromMigrations(codePath, appliedMigrationsResult.Result);
            if (targetSchemaResult.Exception is not null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, targetSchemaResult.Exception);
            }

            ConcurrentDictionary<string, SqlTable> targetSchema = targetSchemaResult.Result.Tables;
            ConcurrentDictionary<string, SqlSequence> targetSequences = targetSchemaResult.Result.Sequences;
            ConcurrentDictionary<string, SqlStoredProcedure> targetProcedures = targetSchemaResult.Result.Procedures;

            // Attach foreign keys to current schema for comparison
            ResultOrException<Dictionary<string, List<SqlForeignKey>>> currentFksResult = await sqlService.GetForeignKeys(currentSchemaResult.Result.Keys.ToList());
            if (currentFksResult.Exception is not null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, currentFksResult.Exception);
            }

            foreach (KeyValuePair<string, List<SqlForeignKey>> fkList in currentFksResult.Result ?? new Dictionary<string, List<SqlForeignKey>>())
            {
                if (currentSchemaResult.Result.TryGetValue(fkList.Key, out SqlTable? table))
                {
                    foreach (SqlForeignKey fk in fkList.Value)
                    {
                        if (table.Columns.TryGetValue(fk.Column.ToLowerInvariant(), out SqlTableColumn? column))
                        {
                            column.ForeignKeys.Add(fk);
                        }
                    }
                }
            }

            // Compare schemas: we need TWO diffs - one for up script, one for down script
            // For incremental migrations:
            //   - targetSchema = state AFTER all applied migrations (from snapshot)
            //   - currentSchema = current database state (may have manual changes)
            //   - We want to generate a migration: targetSchema → currentSchema
            //
            // For UP script: CompareSchemas(target=BEFORE, current=AFTER) finds what to add/change
            //   - Add changes = columns in current but not in target → ADD them
            //   - Drop changes = columns in target but not in current → DROP them
            //
            // For DOWN script: CompareSchemas(current=AFTER, target=BEFORE) finds what to reverse
            //   - Add changes = columns in target but not in current → ADD them back
            //   - Drop changes = columns in current but not in target → DROP them back
            
            // Up script diff: what changed from target (BEFORE) to current (AFTER)
            SchemaDiff upDiff = MigrationSchemaComparer.CompareSchemas(targetSchema, currentSchemaResult.Result, targetSequences, currentSequences, targetProcedures, currentProcedures);
            
            // Down script diff: what changed from current (AFTER) back to target (BEFORE) - reverse of up diff
            SchemaDiff downDiff = MigrationSchemaComparer.CompareSchemas(currentSchemaResult.Result, targetSchema, currentSequences, targetSequences, currentProcedures, targetProcedures);
            
            // Debug logging for sequences and procedures
            MigrationLogger.Log($"\n=== Sequence comparison for down script ===");
            MigrationLogger.Log($"Current sequences: {string.Join(", ", currentSequences.Keys)}");
            MigrationLogger.Log($"Target sequences: {string.Join(", ", targetSequences.Keys)}");
            MigrationLogger.Log($"DownDiff.NewSequences: {string.Join(", ", downDiff.NewSequences.Select(s => s.Name))}");
            MigrationLogger.Log($"DownDiff.DroppedSequenceNames: {string.Join(", ", downDiff.DroppedSequenceNames)}");
            MigrationLogger.Log($"DownDiff.ModifiedSequences: {downDiff.ModifiedSequences.Count}");
            if (downDiff.DroppedSequenceNames.Count > 0)
            {
                MigrationLogger.Log("Detailed dropped sequence classification:");
                foreach (string sequenceName in downDiff.DroppedSequenceNames)
                {
                    bool inCurrent = currentSequences.Keys.Any(k => k.Equals(sequenceName, StringComparison.OrdinalIgnoreCase));
                    bool inTarget = targetSequences.Keys.Any(k => k.Equals(sequenceName, StringComparison.OrdinalIgnoreCase));
                    MigrationLogger.Log($"  {sequenceName} -> inCurrent={inCurrent}, inTarget={inTarget}");
                }
            }
            if (downDiff.NewSequences.Count > 0)
            {
                MigrationLogger.Log("Detailed new sequence classification:");
                foreach (SqlSequence sequence in downDiff.NewSequences)
                {
                    bool inCurrent = currentSequences.Keys.Any(k => k.Equals(sequence.Name, StringComparison.OrdinalIgnoreCase));
                    bool inTarget = targetSequences.Keys.Any(k => k.Equals(sequence.Name, StringComparison.OrdinalIgnoreCase));
                    MigrationLogger.Log($"  {sequence.Name} -> inCurrent={inCurrent}, inTarget={inTarget}");
                }
            }
            
            MigrationLogger.Log($"\n=== Procedure comparison for down script ===");
            MigrationLogger.Log($"Current procedures: {string.Join(", ", currentProcedures.Keys)}");
            MigrationLogger.Log($"Target procedures: {string.Join(", ", targetProcedures.Keys)}");
            MigrationLogger.Log($"DownDiff.NewProcedures: {string.Join(", ", downDiff.NewProcedures.Select(p => p.Name))}");
            MigrationLogger.Log($"DownDiff.DroppedProcedureNames: {string.Join(", ", downDiff.DroppedProcedureNames)}");
            MigrationLogger.Log($"DownDiff.ModifiedProcedures: {downDiff.ModifiedProcedures.Count}");
            
            // Use upDiff for up script generation, downDiff for down script generation
            SchemaDiff diff = upDiff;

            // Check if there are any changes
            if (diff.NewTables.Count == 0 && diff.DroppedTableNames.Count == 0 && diff.ModifiedTables.Count == 0 &&
                diff.NewSequences.Count == 0 && diff.DroppedSequenceNames.Count == 0 && diff.ModifiedSequences.Count == 0 &&
                diff.NewProcedures.Count == 0 && diff.DroppedProcedureNames.Count == 0 && diff.ModifiedProcedures.Count == 0)
            {
                return new ResultOrException<MigrationGenerationResult>(null, new MinfoldMigrationDbUpToDateException());
            }

            // Generate migration scripts
            // Generate up script using dedicated generator
            string upScript = GenerateIncrementalUpScript.Generate(
                diff,
                currentSchemaResult.Result ?? new ConcurrentDictionary<string, SqlTable>(StringComparer.OrdinalIgnoreCase),
                targetSequences,
                targetProcedures,
                targetSchema,
                upDiff);

            // Generate down script using dedicated generator
            string downScript = GenerateIncrementalDownScript.Generate(
                downDiff,
                currentSchemaResult.Result ?? new ConcurrentDictionary<string, SqlTable>(StringComparer.OrdinalIgnoreCase),
                currentSequences,
                currentProcedures,
                targetSchema,
                targetSequences,
                targetProcedures);

            SchemaDiff originalDiff = diff;

            string timestamp = MigrationUtilities.GetNextMigrationTimestamp(codePath);
            string migrationName = string.IsNullOrWhiteSpace(description) ? timestamp : $"{timestamp}_{description}";
            string migrationFolder = Path.Combine(migrationsPath, migrationName);
            Directory.CreateDirectory(migrationFolder);
            
            string upScriptPath = Path.Combine(migrationFolder, "up.sql");
            string downScriptPath = Path.Combine(migrationFolder, "down.sql");

            await File.WriteAllTextAsync(upScriptPath, upScript);
            await File.WriteAllTextAsync(downScriptPath, downScript);

            // Restore diff to upDiff for snapshot saving (we save the state after applying the UP migration)
            diff = originalDiff;

            // Save schema snapshot (target schema represents the state after this migration is applied)
            // We need to apply the changes to target schema to get the new target
            ConcurrentDictionary<string, SqlTable> newTargetSchema = MigrationSchemaSnapshot.ApplySchemaDiffToTarget(targetSchema, diff);
            ConcurrentDictionary<string, SqlSequence> newTargetSequences = MigrationSchemaSnapshot.ApplySequenceDiffToTarget(targetSequences, diff);
            ConcurrentDictionary<string, SqlStoredProcedure> newTargetProcedures = MigrationSchemaSnapshot.ApplyProcedureDiffToTarget(targetProcedures, diff);
            await MigrationSchemaSnapshot.SaveSchemaSnapshot(newTargetSchema, migrationName, codePath, newTargetSequences, newTargetProcedures);

            return new ResultOrException<MigrationGenerationResult>(
                new MigrationGenerationResult(migrationName, upScriptPath, downScriptPath, description),
                null
            );
        }
        catch (Exception ex)
        {
            return new ResultOrException<MigrationGenerationResult>(null, ex);
        }
    }

    public static async Task<ResultOrException<MigrationGenerationResult>> CreateEmptyMigration(string codePath, string description)
    {
        try
        {
            string migrationsPath = MigrationUtilities.GetMigrationsPath(codePath);
            Directory.CreateDirectory(migrationsPath);

            string timestamp = MigrationUtilities.GetNextMigrationTimestamp(codePath);
            string migrationName = string.IsNullOrWhiteSpace(description) ? timestamp : $"{timestamp}_{description}";
            string migrationFolder = Path.Combine(migrationsPath, migrationName);
            Directory.CreateDirectory(migrationFolder);
            
            string upScriptPath = Path.Combine(migrationFolder, "up.sql");
            string downScriptPath = Path.Combine(migrationFolder, "down.sql");

            // Create empty migration files
            await File.WriteAllTextAsync(upScriptPath, "-- Generated using Minfold, do not edit manually\n-- Add your migration SQL here");
            await File.WriteAllTextAsync(downScriptPath, "-- Generated using Minfold, do not edit manually\n-- Add your rollback SQL here");

            return new ResultOrException<MigrationGenerationResult>(
                new MigrationGenerationResult(migrationName, upScriptPath, downScriptPath, description),
                null
            );
        }
        catch (Exception ex)
        {
            return new ResultOrException<MigrationGenerationResult>(null, ex);
        }
    }
    
    public static string GenerateSectionHeader(int phaseNumber, string phaseDescription)
    {
        StringBuilder sb = new StringBuilder();
        sb.AppendLine("-- =============================================");
        sb.AppendLine($"-- Phase {phaseNumber}: {phaseDescription}");
        sb.AppendLine("-- =============================================");
        sb.AppendLine();
        return sb.ToString();
    }
}