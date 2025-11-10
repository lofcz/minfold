using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using Minfold;
using Newtonsoft.Json;

namespace MinFold.Test;

public class MigrationIntegrationTests
{
    private static SqlSettings connSettings;
    private SqlService SqlService => new SqlService(connSettings.Connection);

    public record SqlSettings(string Connection, string Database);
    public record MigrationTestState(string StepName, ConcurrentDictionary<string, SqlTable> Schema, List<string> AppliedMigrations, string? MigrationName);

    [SetUp]
    public async Task Setup()
    {
        if (!File.Exists("conn.txt"))
        {
            throw new Exception("Please make a copy of conn_proto.txt and replace it's content with the connection string");
        }

        connSettings = JsonConvert.DeserializeObject<SqlSettings>(await File.ReadAllTextAsync("conn.txt")) ?? throw new Exception("Failed to deserialize content of conn.txt as valid JSON");
    }

    [Test]
    public async Task TestMigrationLifecycle()
    {
        string tempDbName = $"MinfoldTest_Migration_{Guid.NewGuid():N}";
        string tempProjectPath = Path.Combine(Path.GetTempPath(), $"MinfoldTestProject_{Guid.NewGuid():N}");
        string? tempDbConnectionString = null;
        Dictionary<string, MigrationTestState> stateHistory = new Dictionary<string, MigrationTestState>();

        try
        {
            // Setup: Create temporary database and project
            tempDbConnectionString = await CreateTempDatabase(connSettings.Connection, tempDbName);
            await CreateTempProject(tempProjectPath);
            await ApplyInitialSchema(tempDbConnectionString, tempDbName);

            // Step 1: Generate initial migration
            MigrationTestState step1State = await Step1_GenerateInitialMigration(tempProjectPath, tempDbConnectionString, tempDbName);
            stateHistory["Step1"] = step1State;

            // Step 2: Create fresh database and apply initial migration, verify schema matches
            string freshDbName = $"MinfoldTest_Migration_Fresh_{Guid.NewGuid():N}";
            string freshDbConnectionString = await CreateTempDatabase(connSettings.Connection, freshDbName);
            try
            {
                MigrationTestState step2State = await Step2_ApplyInitialMigrationAndVerify(tempProjectPath, freshDbConnectionString, freshDbName, step1State.Schema);
                stateHistory["Step2"] = step2State;
            }
            finally
            {
                await DropTempDatabase(freshDbConnectionString, freshDbName);
            }

            // Step 3: Make schema changes (add column, add table, add FK)
            await Step3_MakeSchemaChanges(tempDbConnectionString, tempDbName);
            MigrationTestState step3State = await GetCurrentState(tempDbConnectionString, tempDbName, tempProjectPath, "Step3", null);
            stateHistory["Step3"] = step3State;

            // Step 4: Generate incremental migration #1
            MigrationTestState step4State = await Step4_GenerateIncrementalMigration1(tempProjectPath, tempDbConnectionString, tempDbName);
            stateHistory["Step4"] = step4State;

            // Record migrations as applied to original database (since changes already exist)
            // Record initial migration first (the database already has the initial schema from Setup)
            Assert.That(step1State.MigrationName, Is.Not.Null, "Step 4: Initial migration name is null");
            await RecordMigrationApplied(tempDbConnectionString, tempDbName, step1State.MigrationName!);
            
            // Then record migration #1 (this allows us to rollback later in Step 9)
            Assert.That(step4State.MigrationName, Is.Not.Null, "Step 4: Migration name is null");
            await RecordMigrationApplied(tempDbConnectionString, tempDbName, step4State.MigrationName!);

            // Step 5: Apply migrations to a fresh database, verify schema matches Step 3 state
            string freshDb2Name = $"MinfoldTest_Migration_Fresh2_{Guid.NewGuid():N}";
            string freshDb2ConnectionString = await CreateTempDatabase(connSettings.Connection, freshDb2Name);
            try
            {
                // Apply all pending migrations (initial + migration #1)
                ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(freshDb2ConnectionString, freshDb2Name, tempProjectPath, false);
                Assert.That(applyResult.Exception, Is.Null, $"Step 5 failed: {applyResult.Exception?.Message}");
                Assert.That(applyResult.Result, Is.Not.Null, "Step 5: Apply migrations returned null result");
                Assert.That(applyResult.Result!.AppliedMigrations.Count, Is.EqualTo(2), "Step 5: Expected exactly 2 migrations to be applied (initial + migration #1)");
                
                // Verify schema matches Step 3 state
                await VerifySchemaMatches(freshDb2ConnectionString, freshDb2Name, step3State.Schema);
                
                // Get migration #1 name for state tracking
                Assert.That(stateHistory["Step4"].MigrationName, Is.Not.Null, "Step 5: Step4 migration name is null");
                MigrationTestState step5State = await GetCurrentState(freshDb2ConnectionString, freshDb2Name, tempProjectPath, "Step5", stateHistory["Step4"].MigrationName!);
                stateHistory["Step5"] = step5State;
            }
            finally
            {
                await DropTempDatabase(freshDb2ConnectionString, freshDb2Name);
            }

            // Step 6: Make more schema changes (modify column, drop column, modify FK)
            await Step6_MakeMoreSchemaChanges(tempDbConnectionString, tempDbName);
            MigrationTestState step6State = await GetCurrentState(tempDbConnectionString, tempDbName, tempProjectPath, "Step6", null);
            stateHistory["Step6"] = step6State;

            // Step 7: Generate incremental migration #2
            MigrationTestState step7State = await Step7_GenerateIncrementalMigration2(tempProjectPath, tempDbConnectionString, tempDbName);
            stateHistory["Step7"] = step7State;

            // Step 8: Apply migration #2, verify schema
            MigrationTestState step8State = await Step8_ApplyMigration2AndVerify(tempProjectPath, tempDbConnectionString, tempDbName, step6State.Schema);
            stateHistory["Step8"] = step8State;

            // Step 9: Rollback to migration #1, verify schema matches Step 5 state
            Assert.That(stateHistory["Step5"].MigrationName, Is.Not.Null, "Step 9: Step5 migration name is null");
            await Step9_RollbackToMigration1(tempProjectPath, tempDbConnectionString, tempDbName, stateHistory["Step5"].Schema, stateHistory["Step5"].MigrationName!);

            // Step 10: Reapply migration #2, verify schema matches Step 8 state
            await Step10_ReapplyMigration2(tempProjectPath, tempDbConnectionString, tempDbName, stateHistory["Step8"].Schema);

            // Step 11: Rollback to initial migration, verify schema matches Step 2 state
            Assert.That(stateHistory["Step2"].MigrationName, Is.Not.Null, "Step 11: Step2 migration name is null");
            await Step11_RollbackToInitial(tempProjectPath, tempDbConnectionString, tempDbName, stateHistory["Step2"].Schema, stateHistory["Step2"].MigrationName!);

            // Step 12: Reapply all migrations, verify final schema matches Step 8 state
            await Step12_ReapplyAllMigrations(tempProjectPath, tempDbConnectionString, tempDbName, stateHistory["Step8"].Schema);
        }
        finally
        {
            // Cleanup
            if (!string.IsNullOrEmpty(tempDbConnectionString))
            {
                await DropTempDatabase(tempDbConnectionString, tempDbName);
            }
            if (Directory.Exists(tempProjectPath))
            {
                Directory.Delete(tempProjectPath, true);
            }
        }
    }

    private async Task<string> CreateTempDatabase(string baseConnectionString, string dbName)
    {
        // Extract server connection string (without Initial Catalog)
        SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(baseConnectionString);
        builder.InitialCatalog = "master"; // Connect to master to create database
        
        SqlService masterService = new SqlService(builder.ConnectionString);
        ResultOrException<int> createResult = await masterService.Execute($"""
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{dbName}')
            BEGIN
                CREATE DATABASE [{dbName}]
            END
            """);

        if (createResult.Exception is not null)
        {
            throw new Exception($"Failed to create temporary database: {createResult.Exception.Message}", createResult.Exception);
        }

        // Return connection string for the new database
        builder.InitialCatalog = dbName;
        return builder.ConnectionString;
    }

    private async Task DropTempDatabase(string connectionString, string dbName)
    {
        try
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
            builder.InitialCatalog = "master";
            
            SqlService masterService = new SqlService(builder.ConnectionString);
            await masterService.Execute($"""
                IF EXISTS (SELECT * FROM sys.databases WHERE name = '{dbName}')
                BEGIN
                    ALTER DATABASE [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                    DROP DATABASE [{dbName}]
                END
                """);
        }
        catch (Exception ex)
        {
            // Log but don't throw - cleanup failures shouldn't fail tests
            Console.WriteLine($"Warning: Failed to drop temporary database {dbName}: {ex.Message}");
        }
    }

    private async Task CreateTempProject(string basePath)
    {
        string migrationsPath = Path.Combine(basePath, "Dao", "Migrations");
        Directory.CreateDirectory(migrationsPath);
        await Task.CompletedTask;
    }

    private async Task ApplyInitialSchema(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        ResultOrException<int> result = await sqlService.Execute("""
            CREATE TABLE [dbo].[Users] (
                [id] INT PRIMARY KEY IDENTITY(1,1),
                [name] NVARCHAR(100) NOT NULL,
                [email] NVARCHAR(255) NOT NULL
            )

            CREATE TABLE [dbo].[Posts] (
                [id] INT PRIMARY KEY IDENTITY(1,1),
                [title] NVARCHAR(200) NOT NULL,
                [content] NVARCHAR(MAX),
                [userId] INT NOT NULL,
                CONSTRAINT [FK_Posts_Users] FOREIGN KEY ([userId]) REFERENCES [dbo].[Users]([id])
            )

            CREATE TABLE [dbo].[Categories] (
                [id] INT PRIMARY KEY IDENTITY(1,1),
                [name] NVARCHAR(100) NOT NULL
            )
            """);

        if (result.Exception is not null)
        {
            throw new Exception($"Failed to apply initial schema: {result.Exception.Message}", result.Exception);
        }
    }

    private async Task<ConcurrentDictionary<string, SqlTable>> GetCurrentSchema(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        ResultOrException<ConcurrentDictionary<string, SqlTable>> schemaResult = await sqlService.GetSchema(dbName, null, ["__MinfoldMigrations"]);

        if (schemaResult.Exception is not null || schemaResult.Result is null)
        {
            throw new Exception($"Failed to get current schema: {schemaResult.Exception?.Message ?? "Unknown error"}");
        }

        // Attach foreign keys
        ResultOrException<Dictionary<string, List<SqlForeignKey>>> fksResult = await sqlService.GetForeignKeys(schemaResult.Result.Keys.ToList());
        if (fksResult.Exception is not null)
        {
            throw new Exception($"Failed to get foreign keys: {fksResult.Exception.Message}");
        }

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

        return schemaResult.Result;
    }

    private async Task VerifySchemaMatches(string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema)
    {
        ConcurrentDictionary<string, SqlTable> currentSchema = await GetCurrentSchema(connectionString, dbName);
        SchemaDiff diff = MigrationSchemaComparer.CompareSchemas(currentSchema, expectedSchema);

        Assert.That(diff.NewTables, Is.Empty, "Schema mismatch: Found unexpected new tables");
        Assert.That(diff.DroppedTableNames, Is.Empty, "Schema mismatch: Found unexpected dropped tables");
        Assert.That(diff.ModifiedTables, Is.Empty, "Schema mismatch: Found unexpected table modifications");
    }

    private async Task<MigrationTestState> GetCurrentState(string connectionString, string dbName, string projectPath, string stepName, string? migrationName)
    {
        ConcurrentDictionary<string, SqlTable> schema = await GetCurrentSchema(connectionString, dbName);
        ResultOrException<List<string>> appliedMigrationsResult = await MigrationApplier.GetAppliedMigrations(connectionString, dbName);
        List<string> appliedMigrations = appliedMigrationsResult.Result ?? new List<string>();

        return new MigrationTestState(stepName, schema, appliedMigrations, migrationName);
    }

    private async Task RecordMigrationApplied(string connectionString, string dbName, string migrationName)
    {
        await MigrationApplier.EnsureMigrationsTableExists(connectionString, dbName);
        await using SqlConnection conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        string sql = "INSERT INTO [dbo].[__MinfoldMigrations] ([MigrationName], [AppliedAt]) VALUES (@MigrationName, GETUTCDATE())";
        SqlCommand command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@MigrationName", migrationName);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<MigrationTestState> Step1_GenerateInitialMigration(string projectPath, string connectionString, string dbName)
    {
        ResultOrException<MigrationGenerationResult> result = await MigrationGenerator.GenerateInitialMigration(
            connectionString, dbName, projectPath, "InitialSchema");

        Assert.That(result.Exception, Is.Null, $"Step 1 failed: {result.Exception?.Message}");
        Assert.That(result.Result, Is.Not.Null, "Step 1: Migration generation returned null result");
        Assert.That(File.Exists(result.Result!.UpScriptPath), Is.True, "Step 1: Up script file not created");
        Assert.That(File.Exists(result.Result.DownScriptPath), Is.True, "Step 1: Down script file not created");

        ConcurrentDictionary<string, SqlTable> schema = await GetCurrentSchema(connectionString, dbName);
        return new MigrationTestState("Step1", schema, new List<string>(), result.Result.MigrationName);
    }

    private async Task<MigrationTestState> Step2_ApplyInitialMigrationAndVerify(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema)
    {
        ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(connectionString, dbName, projectPath, false);

        Assert.That(applyResult.Exception, Is.Null, $"Step 2 failed: {applyResult.Exception?.Message}");
        Assert.That(applyResult.Result, Is.Not.Null, "Step 2: Apply migrations returned null result");
        Assert.That(applyResult.Result!.AppliedMigrations.Count, Is.EqualTo(1), "Step 2: Expected exactly one migration to be applied");

        await VerifySchemaMatches(connectionString, dbName, expectedSchema);

        return await GetCurrentState(connectionString, dbName, projectPath, "Step2", applyResult.Result.AppliedMigrations[0]);
    }

    private async Task Step3_MakeSchemaChanges(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        ResultOrException<int> result = await sqlService.Execute("""
            ALTER TABLE [dbo].[Users] ADD [createdAt] DATETIME2(7) NOT NULL DEFAULT GETUTCDATE()

            CREATE TABLE [dbo].[Tags] (
                [id] INT PRIMARY KEY IDENTITY(1,1),
                [name] NVARCHAR(100) NOT NULL
            )

            ALTER TABLE [dbo].[Posts] ADD [tagId] INT NULL
            ALTER TABLE [dbo].[Posts] ADD CONSTRAINT [FK_Posts_Tags] FOREIGN KEY ([tagId]) REFERENCES [dbo].[Tags]([id])
            """);

        if (result.Exception is not null)
        {
            throw new Exception($"Step 3 failed: {result.Exception.Message}", result.Exception);
        }
    }

    private async Task<MigrationTestState> Step4_GenerateIncrementalMigration1(string projectPath, string connectionString, string dbName)
    {
        ResultOrException<MigrationGenerationResult> result = await MigrationGenerator.GenerateIncrementalMigration(
            connectionString, dbName, projectPath, "AddCreatedAtAndTags");

        Assert.That(result.Exception, Is.Null, $"Step 4 failed: {result.Exception?.Message}");
        Assert.That(result.Result, Is.Not.Null, "Step 4: Migration generation returned null result");
        Assert.That(File.Exists(result.Result!.UpScriptPath), Is.True, "Step 4: Up script file not created");
        Assert.That(File.Exists(result.Result.DownScriptPath), Is.True, "Step 4: Down script file not created");

        ConcurrentDictionary<string, SqlTable> schema = await GetCurrentSchema(connectionString, dbName);
        ResultOrException<List<string>> appliedMigrationsResult = await MigrationApplier.GetAppliedMigrations(connectionString, dbName);
        List<string> appliedMigrations = appliedMigrationsResult.Result ?? new List<string>();

        return new MigrationTestState("Step4", schema, appliedMigrations, result.Result.MigrationName);
    }

    private async Task<MigrationTestState> Step5_ApplyMigration1AndVerify(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema)
    {
        ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(connectionString, dbName, projectPath, false);

        Assert.That(applyResult.Exception, Is.Null, $"Step 5 failed: {applyResult.Exception?.Message}");
        Assert.That(applyResult.Result, Is.Not.Null, "Step 5: Apply migrations returned null result");
        Assert.That(applyResult.Result!.AppliedMigrations.Count, Is.EqualTo(1), "Step 5: Expected exactly one migration to be applied");

        await VerifySchemaMatches(connectionString, dbName, expectedSchema);

        return await GetCurrentState(connectionString, dbName, projectPath, "Step5", applyResult.Result.AppliedMigrations[0]);
    }

    private async Task Step6_MakeMoreSchemaChanges(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        ResultOrException<int> result = await sqlService.Execute("""
            ALTER TABLE [dbo].[Users] ALTER COLUMN [email] NVARCHAR(500) NOT NULL

            DROP TABLE [dbo].[Categories]

            -- Drop and recreate FK with different enforcement
            ALTER TABLE [dbo].[Posts] DROP CONSTRAINT [FK_Posts_Users]
            ALTER TABLE [dbo].[Posts] WITH NOCHECK ADD CONSTRAINT [FK_Posts_Users] FOREIGN KEY ([userId]) REFERENCES [dbo].[Users]([id])
            """);

        if (result.Exception is not null)
        {
            throw new Exception($"Step 6 failed: {result.Exception.Message}", result.Exception);
        }
    }

    private async Task<MigrationTestState> Step7_GenerateIncrementalMigration2(string projectPath, string connectionString, string dbName)
    {
        ResultOrException<MigrationGenerationResult> result = await MigrationGenerator.GenerateIncrementalMigration(
            connectionString, dbName, projectPath, "ModifyEmailAndDropCategories");

        Assert.That(result.Exception, Is.Null, $"Step 7 failed: {result.Exception?.Message}");
        Assert.That(result.Result, Is.Not.Null, "Step 7: Migration generation returned null result");
        Assert.That(File.Exists(result.Result!.UpScriptPath), Is.True, "Step 7: Up script file not created");
        Assert.That(File.Exists(result.Result.DownScriptPath), Is.True, "Step 7: Down script file not created");

        ConcurrentDictionary<string, SqlTable> schema = await GetCurrentSchema(connectionString, dbName);
        ResultOrException<List<string>> appliedMigrationsResult = await MigrationApplier.GetAppliedMigrations(connectionString, dbName);
        List<string> appliedMigrations = appliedMigrationsResult.Result ?? new List<string>();

        return new MigrationTestState("Step7", schema, appliedMigrations, result.Result.MigrationName);
    }

    private async Task<MigrationTestState> Step8_ApplyMigration2AndVerify(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema)
    {
        ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(connectionString, dbName, projectPath, false);

        Assert.That(applyResult.Exception, Is.Null, $"Step 8 failed: {applyResult.Exception?.Message}");
        Assert.That(applyResult.Result, Is.Not.Null, "Step 8: Apply migrations returned null result");
        Assert.That(applyResult.Result!.AppliedMigrations.Count, Is.EqualTo(1), "Step 8: Expected exactly one migration to be applied");

        await VerifySchemaMatches(connectionString, dbName, expectedSchema);

        return await GetCurrentState(connectionString, dbName, projectPath, "Step8", applyResult.Result.AppliedMigrations[0]);
    }

    private async Task Step9_RollbackToMigration1(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema, string migration1Name)
    {
        ResultOrException<MigrationGotoResult> gotoResult = await MigrationApplier.GotoMigration(connectionString, dbName, projectPath, migration1Name, false);

        Assert.That(gotoResult.Exception, Is.Null, $"Step 9 failed: {gotoResult.Exception?.Message}");
        Assert.That(gotoResult.Result, Is.Not.Null, "Step 9: Goto migration returned null result");

        await VerifySchemaMatches(connectionString, dbName, expectedSchema);
    }

    private async Task Step10_ReapplyMigration2(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema)
    {
        ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(connectionString, dbName, projectPath, false);

        Assert.That(applyResult.Exception, Is.Null, $"Step 10 failed: {applyResult.Exception?.Message}");
        Assert.That(applyResult.Result, Is.Not.Null, "Step 10: Apply migrations returned null result");
        Assert.That(applyResult.Result!.AppliedMigrations.Count, Is.EqualTo(1), "Step 10: Expected exactly one migration to be applied");

        await VerifySchemaMatches(connectionString, dbName, expectedSchema);
    }

    private async Task Step11_RollbackToInitial(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema, string initialMigrationName)
    {
        ResultOrException<MigrationGotoResult> gotoResult = await MigrationApplier.GotoMigration(connectionString, dbName, projectPath, initialMigrationName, false);

        Assert.That(gotoResult.Exception, Is.Null, $"Step 11 failed: {gotoResult.Exception?.Message}");
        Assert.That(gotoResult.Result, Is.Not.Null, "Step 11: Goto migration returned null result");

        await VerifySchemaMatches(connectionString, dbName, expectedSchema);
    }

    private async Task Step12_ReapplyAllMigrations(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema)
    {
        ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(connectionString, dbName, projectPath, false);

        Assert.That(applyResult.Exception, Is.Null, $"Step 12 failed: {applyResult.Exception?.Message}");
        Assert.That(applyResult.Result, Is.Not.Null, "Step 12: Apply migrations returned null result");

        await VerifySchemaMatches(connectionString, dbName, expectedSchema);
    }
}

