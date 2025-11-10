using Microsoft.Data.SqlClient;

namespace Minfold;

public class MigrationApplier
{
    private const string MigrationsTableName = "__MinfoldMigrations";
    private const string CreateMigrationsTableSql = $"""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{MigrationsTableName}]') AND type in (N'U'))
        BEGIN
            CREATE TABLE [dbo].[{MigrationsTableName}] (
                [Id] INT IDENTITY(1,1) PRIMARY KEY,
                [MigrationName] NVARCHAR(255) NOT NULL UNIQUE,
                [AppliedAt] DATETIME2(7) NOT NULL DEFAULT GETUTCDATE()
            );
        END
        """;

    public static async Task<ResultOrException<bool>> EnsureMigrationsTableExists(string sqlConn, string dbName)
    {
        try
        {
            SqlService sqlService = new SqlService(sqlConn);
            ResultOrException<int> result = await sqlService.Execute(CreateMigrationsTableSql);

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

        string[] migrationFiles = Directory.GetFiles(migrationsPath, "*.sql", SearchOption.TopDirectoryOnly)
            .Where(f => !f.EndsWith(".down.sql"))
            .ToArray();

        List<MigrationInfo> migrations = new List<MigrationInfo>();

        foreach (string filePath in migrationFiles)
        {
            string fileName = Path.GetFileName(filePath);
            (string timestamp, string description) = ParseMigrationName(fileName);

            string downScriptPath = filePath.Replace(".sql", ".down.sql");
            string? downScriptPathIfExists = File.Exists(downScriptPath) ? downScriptPath : null;

            migrations.Add(new MigrationInfo(
                fileName.Replace(".sql", ""),
                timestamp,
                description,
                filePath,
                downScriptPathIfExists,
                null
            ));
        }

        return migrations.OrderBy(m => m.Timestamp).ToList();
    }

    public static (string Timestamp, string Description) ParseMigrationName(string filename)
    {
        // Format: YYYYMMDDHHMMSS_Description.sql
        string nameWithoutExtension = Path.GetFileNameWithoutExtension(filename);
        int underscoreIndex = nameWithoutExtension.IndexOf('_');

        if (underscoreIndex == -1 || underscoreIndex < 14)
        {
            return (nameWithoutExtension.Length >= 14 ? nameWithoutExtension[..14] : nameWithoutExtension, 
                    underscoreIndex == -1 ? "" : nameWithoutExtension[(underscoreIndex + 1)..]);
        }

        string timestamp = nameWithoutExtension[..14];
        string description = underscoreIndex < nameWithoutExtension.Length - 1 
            ? nameWithoutExtension[(underscoreIndex + 1)..] 
            : "";

        return (timestamp, description);
    }

    public static async Task<ResultOrException<int>> ExecuteMigrationScript(string sqlConn, string script)
    {
        try
        {
            SqlService sqlService = new SqlService(sqlConn);
            ResultOrException<int> result = await sqlService.Execute(script);

            return result;
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

                string upScript = await File.ReadAllTextAsync(migration.UpScriptPath);
                ResultOrException<int> executeResult = await ExecuteMigrationScript(sqlConn, upScript);

                if (executeResult.Exception is not null)
                {
                    return new ResultOrException<MigrationApplyResult>(null, 
                        new Exception($"Failed to apply migration {migrationName}: {executeResult.Exception.Message}", executeResult.Exception));
                }

                // Record migration as applied
                await RecordMigrationApplied(sqlConn, migrationName);

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

                string upScript = await File.ReadAllTextAsync(migration.UpScriptPath);
                ResultOrException<int> executeResult = await ExecuteMigrationScript(sqlConn, upScript);

                if (executeResult.Exception is not null)
                {
                    return new ResultOrException<MigrationGotoResult>(null,
                        new Exception($"Failed to apply migration {migrationName}: {executeResult.Exception.Message}", executeResult.Exception));
                }

                // Record migration as applied
                await RecordMigrationApplied(sqlConn, migrationName);
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

    private static async Task RecordMigrationApplied(string sqlConn, string migrationName)
    {
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
}

