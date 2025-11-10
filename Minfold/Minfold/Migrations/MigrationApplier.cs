using System.Collections.Concurrent;
using System.Text;
using Microsoft.Data.SqlClient;

namespace Minfold;

public class MigrationApplier
{
    private const string MigrationsTableName = "__MinfoldMigrations";
    
    private static string GetCreateMigrationsTableSql()
    {
        // Default CREATE statement - used only if table doesn't exist
        // The actual values should come from scripted SQL if table exists
        return $"""
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{MigrationsTableName}]') AND type in (N'U'))
            BEGIN
                CREATE TABLE [dbo].[{MigrationsTableName}] (
                    [Id] INT IDENTITY(1,1) PRIMARY KEY,
                    [MigrationName] NVARCHAR(255) NOT NULL UNIQUE,
                    [AppliedAt] DATETIME2(7) NOT NULL DEFAULT GETUTCDATE()
                );
            END
            """;
    }

    public static async Task<ResultOrException<bool>> EnsureMigrationsTableExists(string sqlConn, string dbName)
    {
        try
        {
            SqlService sqlService = new SqlService(sqlConn);
            
            // First, try to script the table to get its actual definition from the database
            // This ensures we use the correct IDENTITY values if the table already exists
            ResultOrException<string> scriptResult = await sqlService.SqlTableCreateScript($"dbo.{MigrationsTableName}");
            
            // If table exists, scriptResult will contain the CREATE TABLE statement with correct values
            // If table doesn't exist, we'll create it with the default definition
            if (scriptResult.Exception is null && !string.IsNullOrWhiteSpace(scriptResult.Result))
            {
                // Table exists - the scripted SQL contains the correct definition from the database
                // No need to create it, just verify it exists
                return new ResultOrException<bool>(true, null);
            }
            
            // Table doesn't exist, create it with default definition
            ResultOrException<int> result = await sqlService.Execute(GetCreateMigrationsTableSql());

            if (result.Exception is not null)
            {
                return new ResultOrException<bool>(false, result.Exception);
            }

            return new ResultOrException<bool>(true, null);
        }
        catch (Exception ex)
        {
            return new ResultOrException<bool>(false, ex);
        }
    }

    public static async Task<ResultOrException<List<string>>> GetAppliedMigrations(string sqlConn, string dbName)
    {
        try
        {
            await EnsureMigrationsTableExists(sqlConn, dbName);

            await using SqlConnection conn = new SqlConnection(sqlConn);
            await conn.OpenAsync();

            string sql = $"SELECT [MigrationName] FROM [dbo].[{MigrationsTableName}] ORDER BY [AppliedAt]";
            SqlCommand command = new SqlCommand(sql, conn);

            List<string> appliedMigrations = new List<string>();
            await using SqlDataReader reader = await command.ExecuteReaderAsync();

            while (reader.Read())
            {
                string migrationName = reader.GetString(0);
                appliedMigrations.Add(migrationName);
            }

            return new ResultOrException<List<string>>(appliedMigrations, null);
        }
        catch (Exception ex)
        {
            return new ResultOrException<List<string>>(null, ex);
        }
    }

    public static List<MigrationInfo> GetMigrationFiles(string codePath)
    {
        string migrationsPath = MigrationUtilities.GetMigrationsPath(codePath);

        if (!Directory.Exists(migrationsPath))
        {
            return new List<MigrationInfo>();
        }

        string[] migrationFolders = Directory.GetDirectories(migrationsPath);

        List<MigrationInfo> migrations = new List<MigrationInfo>();

        foreach (string folderPath in migrationFolders)
        {
            string folderName = Path.GetFileName(folderPath);
            (string timestamp, string description) = ParseMigrationName(folderName);

            string upScriptPath = Path.Combine(folderPath, "up.sql");
            string downScriptPath = Path.Combine(folderPath, "down.sql");
            string? downScriptPathIfExists = File.Exists(downScriptPath) ? downScriptPath : null;

            migrations.Add(new MigrationInfo(
                folderName,
                timestamp,
                description,
                upScriptPath,
                downScriptPathIfExists,
                null
            ));
        }

        return migrations.OrderBy(m => m.Timestamp).ToList();
    }

    public static (string Timestamp, string Description) ParseMigrationName(string folderName)
    {
        // Format: YYYYMMDDHHMMSS_Description
        int underscoreIndex = folderName.IndexOf('_');

        if (underscoreIndex == -1 || underscoreIndex < 14)
        {
            return (folderName.Length >= 14 ? folderName[..14] : folderName, 
                    underscoreIndex == -1 ? "" : folderName[(underscoreIndex + 1)..]);
        }

        string timestamp = folderName[..14];
        string description = underscoreIndex < folderName.Length - 1 
            ? folderName[(underscoreIndex + 1)..] 
            : "";

        return (timestamp, description);
    }

    public static async Task<ResultOrException<int>> ExecuteMigrationScript(string sqlConn, string script)
    {
        try
        {
            // Log the script being executed
            MigrationLogger.Log("=== Executing Migration Script ===");
            MigrationLogger.Log(script);
            MigrationLogger.Log("=== End of Script ===");
            MigrationLogger.Log("");
            
            // Split script into batches on GO statements (case-insensitive, handles GO on its own line)
            // GO is a batch separator in SQL Server - it's not a SQL statement, so we need to split manually
            // Migration scripts don't include BEGIN TRANSACTION/COMMIT TRANSACTION - we manage transactions via ADO.NET
            string[] batches = System.Text.RegularExpressions.Regex.Split(
                script,
                @"^\s*GO\s*$",
                System.Text.RegularExpressions.RegexOptions.Multiline | System.Text.RegularExpressions.RegexOptions.IgnoreCase
            );
            
            // Execute all batches within a single ADO.NET transaction
            await using Microsoft.Data.SqlClient.SqlConnection connection = new Microsoft.Data.SqlClient.SqlConnection(sqlConn);
            await connection.OpenAsync();
            
            // Set XACT_ABORT ON for proper error handling
            Microsoft.Data.SqlClient.SqlCommand setXactAbortCommand = new Microsoft.Data.SqlClient.SqlCommand("SET XACT_ABORT ON", connection);
            await setXactAbortCommand.ExecuteNonQueryAsync();
            
            await using Microsoft.Data.SqlClient.SqlTransaction transaction = connection.BeginTransaction();
            
            try
            {
                // Execute each batch separately within the transaction
                foreach (string batch in batches)
                {
                    string trimmedBatch = batch.Trim();
                    if (string.IsNullOrWhiteSpace(trimmedBatch))
                    {
                        continue; // Skip empty batches (e.g., multiple consecutive GO statements)
                    }
                    
                    Microsoft.Data.SqlClient.SqlCommand command = new Microsoft.Data.SqlClient.SqlCommand(trimmedBatch, connection, transaction);
                    await using Microsoft.Data.SqlClient.SqlDataReader reader = await command.ExecuteReaderAsync();
                    // Consume the reader to ensure the command completes
                    while (await reader.ReadAsync()) { }
                }
                
                // Commit the transaction if all batches succeeded
                await transaction.CommitAsync();
                return new ResultOrException<int>(1, null);
            }
            catch (Exception ex)
            {
                // Rollback the transaction on any error
                await transaction.RollbackAsync();
                return new ResultOrException<int>(0, ex);
            }
        }
        catch (Exception ex)
        {
            return new ResultOrException<int>(0, ex);
        }
    }

    public static async Task<ResultOrException<MigrationApplyResult>> ApplyMigrations(string sqlConn, string dbName, string codePath, bool dryRun = false)
    {
        try
        {
            ResultOrException<bool> ensureTableResult = await EnsureMigrationsTableExists(sqlConn, dbName);
            if (ensureTableResult.Exception is not null)
            {
                return new ResultOrException<MigrationApplyResult>(null, ensureTableResult.Exception);
            }

            ResultOrException<List<string>> appliedMigrationsResult = await GetAppliedMigrations(sqlConn, dbName);
            if (appliedMigrationsResult.Exception is not null)
            {
                return new ResultOrException<MigrationApplyResult>(null, appliedMigrationsResult.Exception);
            }

            HashSet<string> appliedSet = appliedMigrationsResult.Result?.ToHashSet() ?? new HashSet<string>();
            List<MigrationInfo> allMigrations = GetMigrationFiles(codePath);

            List<string> migrationsToApply = allMigrations
                .Where(m => !appliedSet.Contains(m.MigrationName))
                .Select(m => m.MigrationName)
                .ToList();

            if (migrationsToApply.Count == 0)
            {
                return new ResultOrException<MigrationApplyResult>(new MigrationApplyResult(new List<string>()), null);
            }

            if (dryRun)
            {
                return new ResultOrException<MigrationApplyResult>(new MigrationApplyResult(migrationsToApply), null);
            }

            List<string> successfullyApplied = new List<string>();

            foreach (string migrationName in migrationsToApply)
            {
                MigrationInfo? migration = allMigrations.FirstOrDefault(m => m.MigrationName == migrationName);
                if (migration is null)
                {
                    continue;
                }

                MigrationLogger.Log($">>> Applying migration: {migrationName}");
                string upScript = await File.ReadAllTextAsync(migration.UpScriptPath);
                ResultOrException<int> executeResult = await ExecuteMigrationScript(sqlConn, upScript);

                if (executeResult.Exception is not null)
                {
                    return new ResultOrException<MigrationApplyResult>(null, 
                        new Exception($"Failed to apply migration {migrationName}: {executeResult.Exception.Message}", executeResult.Exception));
                }

                // Record migration as applied
                await RecordMigrationApplied(sqlConn, migrationName, dbName);

                successfullyApplied.Add(migrationName);
            }

            return new ResultOrException<MigrationApplyResult>(new MigrationApplyResult(successfullyApplied), null);
        }
        catch (Exception ex)
        {
            return new ResultOrException<MigrationApplyResult>(null, ex);
        }
    }

    public static async Task<ResultOrException<MigrationRollbackResult>> RollbackMigration(string sqlConn, string dbName, string codePath, string migrationName, bool dryRun = false)
    {
        try
        {
            List<MigrationInfo> allMigrations = GetMigrationFiles(codePath);
            MigrationInfo? migration = allMigrations.FirstOrDefault(m => m.MigrationName == migrationName);

            if (migration is null)
            {
                return new ResultOrException<MigrationRollbackResult>(null, new Exception($"Migration {migrationName} not found"));
            }

            if (migration.DownScriptPath is null)
            {
                return new ResultOrException<MigrationRollbackResult>(null, new Exception($"Down script not found for migration {migrationName}"));
            }

            if (dryRun)
            {
                return new ResultOrException<MigrationRollbackResult>(new MigrationRollbackResult(migrationName), null);
            }

            MigrationLogger.Log($">>> Rolling back migration: {migrationName}");
            string downScript = await File.ReadAllTextAsync(migration.DownScriptPath);
            ResultOrException<int> executeResult = await ExecuteMigrationScript(sqlConn, downScript);

            if (executeResult.Exception is not null)
            {
                return new ResultOrException<MigrationRollbackResult>(null,
                    new Exception($"Failed to rollback migration {migrationName}: {executeResult.Exception.Message}", executeResult.Exception));
            }

            // Remove migration from tracking table
            await RemoveMigrationRecord(sqlConn, migrationName);

            return new ResultOrException<MigrationRollbackResult>(new MigrationRollbackResult(migrationName), null);
        }
        catch (Exception ex)
        {
            return new ResultOrException<MigrationRollbackResult>(null, ex);
        }
    }

    public static async Task<ResultOrException<MigrationGotoResult>> GotoMigration(string sqlConn, string dbName, string codePath, string targetMigrationName, bool dryRun = false)
    {
        try
        {
            List<MigrationInfo> allMigrations = GetMigrationFiles(codePath);
            MigrationInfo? targetMigration = allMigrations.FirstOrDefault(m => m.MigrationName == targetMigrationName);

            if (targetMigration is null)
            {
                return new ResultOrException<MigrationGotoResult>(null, new Exception($"Migration {targetMigrationName} not found"));
            }

            ResultOrException<List<string>> appliedMigrationsResult = await GetAppliedMigrations(sqlConn, dbName);
            if (appliedMigrationsResult.Exception is not null)
            {
                return new ResultOrException<MigrationGotoResult>(null, appliedMigrationsResult.Exception);
            }

            List<string> appliedMigrations = appliedMigrationsResult.Result ?? new List<string>();
            HashSet<string> appliedSet = appliedMigrations.ToHashSet();

            // Find target migration index
            int targetIndex = allMigrations.FindIndex(m => m.MigrationName == targetMigrationName);
            if (targetIndex == -1)
            {
                return new ResultOrException<MigrationGotoResult>(null, new Exception($"Migration {targetMigrationName} not found"));
            }

            List<string> migrationsToApply = new List<string>();
            List<string> migrationsToRollback = new List<string>();

            // Determine what needs to happen
            for (int i = 0; i < allMigrations.Count; i++)
            {
                string migrationName = allMigrations[i].MigrationName;
                bool isApplied = appliedSet.Contains(migrationName);

                if (i <= targetIndex && !isApplied)
                {
                    migrationsToApply.Add(migrationName);
                }
                else if (i > targetIndex && isApplied)
                {
                    migrationsToRollback.Add(migrationName);
                }
            }

            if (dryRun)
            {
                return new ResultOrException<MigrationGotoResult>(
                    new MigrationGotoResult(migrationsToApply, migrationsToRollback),
                    null
                );
            }

            List<string> applied = new List<string>();
            List<string> rolledBack = new List<string>();

            // Rollback migrations first (in reverse order)
            for (int i = migrationsToRollback.Count - 1; i >= 0; i--)
            {
                ResultOrException<MigrationRollbackResult> rollbackResult = await RollbackMigration(sqlConn, dbName, codePath, migrationsToRollback[i], false);
                if (rollbackResult.Exception is not null)
                {
                    return new ResultOrException<MigrationGotoResult>(null, rollbackResult.Exception);
                }
                rolledBack.Add(migrationsToRollback[i]);
            }

            // Apply migrations one by one up to target
            foreach (string migrationName in migrationsToApply)
            {
                MigrationInfo? migration = allMigrations.FirstOrDefault(m => m.MigrationName == migrationName);
                if (migration is null)
                {
                    continue;
                }

                MigrationLogger.Log($">>> Applying migration: {migrationName}");
                string upScript = await File.ReadAllTextAsync(migration.UpScriptPath);
                ResultOrException<int> executeResult = await ExecuteMigrationScript(sqlConn, upScript);

                if (executeResult.Exception is not null)
                {
                    return new ResultOrException<MigrationGotoResult>(null,
                        new Exception($"Failed to apply migration {migrationName}: {executeResult.Exception.Message}", executeResult.Exception));
                }

                // Record migration as applied
                await RecordMigrationApplied(sqlConn, migrationName, dbName);
                applied.Add(migrationName);
            }

            return new ResultOrException<MigrationGotoResult>(
                new MigrationGotoResult(applied, rolledBack),
                null
            );
        }
        catch (Exception ex)
        {
            return new ResultOrException<MigrationGotoResult>(null, ex);
        }
    }

    public static async Task RecordMigrationApplied(string sqlConn, string migrationName, string? dbName = null)
    {
        if (dbName is not null)
        {
            await EnsureMigrationsTableExists(sqlConn, dbName);
        }
        await using SqlConnection conn = new SqlConnection(sqlConn);
        await conn.OpenAsync();

        string sql = $"INSERT INTO [dbo].[{MigrationsTableName}] ([MigrationName], [AppliedAt]) VALUES (@MigrationName, GETUTCDATE())";
        SqlCommand command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@MigrationName", migrationName);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task RemoveMigrationRecord(string sqlConn, string migrationName)
    {
        await using SqlConnection conn = new SqlConnection(sqlConn);
        await conn.OpenAsync();

        string sql = $"DELETE FROM [dbo].[{MigrationsTableName}] WHERE [MigrationName] = @MigrationName";
        SqlCommand command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@MigrationName", migrationName);
        await command.ExecuteNonQueryAsync();
    }

    public static async Task<ResultOrException<MigrationClaimResult>> ClaimMigration(string sqlConn, string dbName, string codePath, string migrationNameOrLatest, bool force = false)
    {
        try
        {
            // Ensure migrations table exists
            ResultOrException<bool> ensureTableResult = await EnsureMigrationsTableExists(sqlConn, dbName);
            if (ensureTableResult.Exception is not null)
            {
                return new ResultOrException<MigrationClaimResult>(null, ensureTableResult.Exception);
            }

            // Get all migrations
            List<MigrationInfo> allMigrations = GetMigrationFiles(codePath);
            if (allMigrations.Count == 0)
            {
                return new ResultOrException<MigrationClaimResult>(null, new Exception("No migrations found"));
            }

            // Resolve migration name (handle "latest")
            string targetMigrationName;
            if (migrationNameOrLatest.Equals("latest", StringComparison.OrdinalIgnoreCase))
            {
                // Get the latest migration by timestamp
                targetMigrationName = allMigrations[allMigrations.Count - 1].MigrationName;
            }
            else
            {
                targetMigrationName = migrationNameOrLatest;
            }

            // Validate migration exists
            MigrationInfo? targetMigration = allMigrations.FirstOrDefault(m => m.MigrationName == targetMigrationName);
            if (targetMigration is null)
            {
                return new ResultOrException<MigrationClaimResult>(null, new Exception($"Migration {targetMigrationName} not found"));
            }

            // Load expected schema from migration snapshot
            ResultOrException<(ConcurrentDictionary<string, SqlTable> Tables, ConcurrentDictionary<string, SqlSequence> Sequences, ConcurrentDictionary<string, SqlStoredProcedure> Procedures)> snapshotResult = 
                await MigrationSchemaSnapshot.LoadSchemaSnapshot(targetMigrationName, codePath);

            if (snapshotResult.Exception is not null)
            {
                return new ResultOrException<MigrationClaimResult>(null, 
                    new Exception($"Failed to load schema snapshot for migration {targetMigrationName}: {snapshotResult.Exception.Message}", snapshotResult.Exception));
            }

            ConcurrentDictionary<string, SqlTable> expectedTables = snapshotResult.Result.Tables;
            ConcurrentDictionary<string, SqlSequence> expectedSequences = snapshotResult.Result.Sequences;
            ConcurrentDictionary<string, SqlStoredProcedure> expectedProcedures = snapshotResult.Result.Procedures;

            // Get current database schema
            SqlService sqlService = new SqlService(sqlConn);
            
            // Get current tables
            ResultOrException<ConcurrentDictionary<string, SqlTable>> currentSchemaResult = await sqlService.GetSchema(dbName, null, ["__MinfoldMigrations"]);
            if (currentSchemaResult.Exception is not null || currentSchemaResult.Result is null)
            {
                return new ResultOrException<MigrationClaimResult>(null, 
                    currentSchemaResult.Exception ?? new Exception("Failed to get current database schema"));
            }

            // Get foreign keys and attach to tables
            ResultOrException<Dictionary<string, List<SqlForeignKey>>> fksResult = await sqlService.GetForeignKeys(currentSchemaResult.Result.Keys.ToList());
            if (fksResult.Exception is not null)
            {
                return new ResultOrException<MigrationClaimResult>(null, fksResult.Exception);
            }

            foreach (KeyValuePair<string, List<SqlForeignKey>> fkList in fksResult.Result ?? new Dictionary<string, List<SqlForeignKey>>())
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

            // Get current sequences
            ResultOrException<ConcurrentDictionary<string, SqlSequence>> currentSequencesResult = await sqlService.GetSequences(dbName);
            if (currentSequencesResult.Exception is not null)
            {
                return new ResultOrException<MigrationClaimResult>(null, currentSequencesResult.Exception);
            }

            // Get current procedures
            ResultOrException<ConcurrentDictionary<string, SqlStoredProcedure>> currentProceduresResult = await sqlService.GetStoredProcedures(dbName);
            if (currentProceduresResult.Exception is not null)
            {
                return new ResultOrException<MigrationClaimResult>(null, currentProceduresResult.Exception);
            }

            ConcurrentDictionary<string, SqlTable> currentTables = currentSchemaResult.Result;
            ConcurrentDictionary<string, SqlSequence> currentSequences = currentSequencesResult.Result ?? new ConcurrentDictionary<string, SqlSequence>();
            ConcurrentDictionary<string, SqlStoredProcedure> currentProcedures = currentProceduresResult.Result ?? new ConcurrentDictionary<string, SqlStoredProcedure>();

            // Compare schemas
            SchemaDiff diff = MigrationSchemaComparer.CompareSchemas(
                currentTables,
                expectedTables,
                currentSequences,
                expectedSequences,
                currentProcedures,
                expectedProcedures);

            // Check if there are any differences
            bool hasDifferences = diff.NewTables.Count > 0 ||
                                  diff.DroppedTableNames.Count > 0 ||
                                  diff.ModifiedTables.Count > 0 ||
                                  diff.NewSequences.Count > 0 ||
                                  diff.DroppedSequenceNames.Count > 0 ||
                                  diff.ModifiedSequences.Count > 0 ||
                                  diff.NewProcedures.Count > 0 ||
                                  diff.DroppedProcedureNames.Count > 0 ||
                                  diff.ModifiedProcedures.Count > 0;

            if (hasDifferences && !force)
            {
                // Build error message with diff details
                StringBuilder errorMsg = new StringBuilder();
                errorMsg.AppendLine($"Schema verification failed for migration {targetMigrationName}. The database schema does not match the expected schema.");
                errorMsg.AppendLine();
                
                if (diff.NewTables.Count > 0)
                {
                    errorMsg.AppendLine($"Found {diff.NewTables.Count} unexpected new table(s):");
                    foreach (SqlTable table in diff.NewTables)
                    {
                        errorMsg.AppendLine($"  - {table.Name}");
                    }
                    errorMsg.AppendLine();
                }

                if (diff.DroppedTableNames.Count > 0)
                {
                    errorMsg.AppendLine($"Found {diff.DroppedTableNames.Count} missing table(s):");
                    foreach (string tableName in diff.DroppedTableNames)
                    {
                        errorMsg.AppendLine($"  - {tableName}");
                    }
                    errorMsg.AppendLine();
                }

                if (diff.ModifiedTables.Count > 0)
                {
                    errorMsg.AppendLine($"Found {diff.ModifiedTables.Count} modified table(s):");
                    foreach (TableDiff tableDiff in diff.ModifiedTables)
                    {
                        errorMsg.AppendLine($"  - {tableDiff.TableName} ({tableDiff.ColumnChanges.Count} column changes, {tableDiff.ForeignKeyChanges.Count} FK changes, {tableDiff.IndexChanges.Count} index changes)");
                    }
                    errorMsg.AppendLine();
                }

                if (diff.NewSequences.Count > 0 || diff.DroppedSequenceNames.Count > 0 || diff.ModifiedSequences.Count > 0)
                {
                    errorMsg.AppendLine("Sequence differences found.");
                    errorMsg.AppendLine();
                }

                if (diff.NewProcedures.Count > 0 || diff.DroppedProcedureNames.Count > 0 || diff.ModifiedProcedures.Count > 0)
                {
                    errorMsg.AppendLine("Stored procedure differences found.");
                    errorMsg.AppendLine();
                }

                errorMsg.AppendLine("Use --force to override this verification and claim the migration anyway.");

                return new ResultOrException<MigrationClaimResult>(null, new Exception(errorMsg.ToString()));
            }

            // Get already applied migrations
            ResultOrException<List<string>> appliedMigrationsResult = await GetAppliedMigrations(sqlConn, dbName);
            if (appliedMigrationsResult.Exception is not null)
            {
                return new ResultOrException<MigrationClaimResult>(null, appliedMigrationsResult.Exception);
            }

            HashSet<string> appliedSet = appliedMigrationsResult.Result?.ToHashSet() ?? new HashSet<string>();

            // Find target migration index
            int targetIndex = allMigrations.FindIndex(m => m.MigrationName == targetMigrationName);
            if (targetIndex == -1)
            {
                return new ResultOrException<MigrationClaimResult>(null, new Exception($"Migration {targetMigrationName} not found"));
            }

            // Mark all migrations up to and including the target as applied
            List<string> claimedMigrations = new List<string>();
            for (int i = 0; i <= targetIndex; i++)
            {
                string migrationName = allMigrations[i].MigrationName;
                if (!appliedSet.Contains(migrationName))
                {
                    await RecordMigrationApplied(sqlConn, migrationName, dbName);
                    claimedMigrations.Add(migrationName);
                }
            }

            // Remove migrations that come after the target (they shouldn't be applied)
            for (int i = targetIndex + 1; i < allMigrations.Count; i++)
            {
                string migrationName = allMigrations[i].MigrationName;
                if (appliedSet.Contains(migrationName))
                {
                    await RemoveMigrationRecord(sqlConn, migrationName);
                }
            }

            SchemaDiff? differencesForResult = hasDifferences ? diff : null;
            return new ResultOrException<MigrationClaimResult>(
                new MigrationClaimResult(targetMigrationName, !hasDifferences, differencesForResult),
                null);
        }
        catch (Exception ex)
        {
            return new ResultOrException<MigrationClaimResult>(null, ex);
        }
    }
}

