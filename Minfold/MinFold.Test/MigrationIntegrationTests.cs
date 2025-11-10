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

            // Step 2.5: Make complex schema changes (identity, PK changes)
            await Step2_5_MakeComplexSchemaChanges(tempDbConnectionString, tempDbName);
            MigrationTestState step2_5State = await GetCurrentState(tempDbConnectionString, tempDbName, tempProjectPath, "Step2.5", null);
            stateHistory["Step2.5"] = step2_5State;

            // Step 2.6: Generate migration for complex changes
            MigrationTestState step2_6State = await Step2_6_GenerateComplexMigration(tempProjectPath, tempDbConnectionString, tempDbName);
            stateHistory["Step2.6"] = step2_6State;

            // Record complex migration as applied to original database
            Assert.That(step2_6State.MigrationName, Is.Not.Null, "Step 2.6: Migration name is null");
            await RecordMigrationApplied(tempDbConnectionString, tempDbName, step2_6State.MigrationName!);

            // Step 2.7: Apply all pending migrations (initial + complex) to fresh database and verify
            string freshDbComplexName = $"MinfoldTest_Migration_Complex_{Guid.NewGuid():N}";
            string freshDbComplexConnectionString = await CreateTempDatabase(connSettings.Connection, freshDbComplexName);
            try
            {
                // Apply all pending migrations (initial + complex) - ApplyMigrations applies all pending migrations
                MigrationTestState step2_7State = await Step2_7_ApplyComplexMigrationAndVerify(tempProjectPath, freshDbComplexConnectionString, freshDbComplexName, step2_5State.Schema);
                stateHistory["Step2.7"] = step2_7State;
            }
            finally
            {
                await DropTempDatabase(freshDbComplexConnectionString, freshDbComplexName);
            }

            // Step 2.8: Rollback complex migration and verify schema restored
            await Step2_8_RollbackComplexMigration(tempProjectPath, tempDbConnectionString, tempDbName, step1State.Schema, step2_6State.MigrationName!);

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
                // The complex migration was rolled back and its files deleted, so it won't be applied
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

            // Step 5.5: Verify that generating a migration when there are no changes doesn't create any migration files
            // Use the temp database which already has migrations applied and recorded
            Assert.That(stateHistory["Step1"].MigrationName, Is.Not.Null, "Step 5.5: Step1 migration name is null");
            Assert.That(stateHistory["Step4"].MigrationName, Is.Not.Null, "Step 5.5: Step4 migration name is null");
            await Step5_5_VerifyNoChangesMigration(tempProjectPath, tempDbConnectionString, tempDbName, 
                stateHistory["Step1"].MigrationName!, stateHistory["Step4"].MigrationName!);

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

    private async Task DropPrimaryKeyConstraint(SqlService sqlService, string tableName)
    {
        ResultOrException<int> result = await sqlService.Execute($"""
            DECLARE @pkConstraintName NVARCHAR(128);
            SELECT @pkConstraintName = name FROM sys.key_constraints 
            WHERE parent_object_id = OBJECT_ID('[dbo].[{tableName}]') 
            AND type = 'PK';
            IF @pkConstraintName IS NOT NULL
                EXEC('ALTER TABLE [dbo].[{tableName}] DROP CONSTRAINT [' + @pkConstraintName + ']');
            """);
        if (result.Exception is not null)
        {
            throw new Exception($"Failed to drop PK constraint on {tableName}: {result.Exception.Message}", result.Exception);
        }
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

    private async Task Step2_5_MakeComplexSchemaChanges(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        
        // 1. Add identity to non-identity column: 
        // First, remove identity from existing id column (SQL Server only allows one identity per table)
        // Then add sequenceNumber with DEFAULT, then change to identity
        // The migration generator should automatically handle dropping the default constraint
        
        // Step 1a: Remove identity from id column (drop and recreate as non-identity)
        // First, drop the PK constraint (SQL Server auto-generates constraint names)
        await DropPrimaryKeyConstraint(sqlService, "Categories");

        // Now drop and recreate the id column without identity
        string dropIdColumnSql = MigrationSqlGenerator.GenerateDropColumnStatement("id", "Categories");
        string addIdColumnSql = MigrationSqlGenerator.GenerateAddColumnStatement(
            new SqlTableColumn(
                Name: "id",
                OrdinalPosition: 0,
                IsNullable: false,
                IsIdentity: false,
                SqlType: SqlDbTypeExt.Int,
                ForeignKeys: [],
                IsComputed: false,
                IsPrimaryKey: true,
                ComputedSql: null,
                LengthOrPrecision: null
            ), 
            "Categories");
        
        ResultOrException<int> result1b = await sqlService.Execute($"""
            {dropIdColumnSql}
            {addIdColumnSql}
            ALTER TABLE [dbo].[Categories] ADD CONSTRAINT [PK_Categories] PRIMARY KEY ([id])
            """);
        if (result1b.Exception is not null)
        {
            throw new Exception($"Step 2.5 failed (remove identity from id): {result1b.Exception.Message}", result1b.Exception);
        }

        // Step 1c: Add sequenceNumber with DEFAULT
        ResultOrException<int> result1c = await sqlService.Execute("""
            ALTER TABLE [dbo].[Categories] ADD [sequenceNumber] INT NOT NULL DEFAULT 0
            """);
        if (result1c.Exception is not null)
        {
            throw new Exception($"Step 2.5 failed (add sequenceNumber): {result1c.Exception.Message}", result1c.Exception);
        }

        // Step 1d: Change sequenceNumber to identity - use the same SQL that the migration generator would produce
        // This tests that the migration generator correctly handles default constraints
        string dropSequenceNumberSql = MigrationSqlGenerator.GenerateDropColumnStatement("sequenceNumber", "Categories");
        string addSequenceNumberSql = MigrationSqlGenerator.GenerateAddColumnStatement(
            new SqlTableColumn(
                Name: "sequenceNumber",
                OrdinalPosition: 0,
                IsNullable: false,
                IsIdentity: true,
                SqlType: SqlDbTypeExt.Int,
                ForeignKeys: [],
                IsComputed: false,
                IsPrimaryKey: false,
                ComputedSql: null,
                LengthOrPrecision: null
            ), 
            "Categories");
        
        ResultOrException<int> result1d = await sqlService.Execute($"""
            {dropSequenceNumberSql}
            {addSequenceNumberSql}
            """);
        if (result1d.Exception is not null)
        {
            throw new Exception($"Step 2.5 failed (recreate as identity): {result1d.Exception.Message}", result1d.Exception);
        }

        // 2. Make non-PK column a PK: Add code column, drop PK on id, add PK on code
        ResultOrException<int> result3 = await sqlService.Execute("""
            ALTER TABLE [dbo].[Categories] ADD [code] NVARCHAR(50) NOT NULL DEFAULT 'TEMP'
            CREATE UNIQUE INDEX [IX_Categories_code] ON [dbo].[Categories]([code])
            """);
        if (result3.Exception is not null)
        {
            throw new Exception($"Step 2.5 failed (add code column): {result3.Exception.Message}", result3.Exception);
        }

        // Drop existing PK
        await DropPrimaryKeyConstraint(sqlService, "Categories");

        // Add new PK on code
        ResultOrException<int> result5 = await sqlService.Execute("""
            ALTER TABLE [dbo].[Categories] ADD CONSTRAINT [PK_Categories] PRIMARY KEY ([code])
            """);
        if (result5.Exception is not null)
        {
            throw new Exception($"Step 2.5 failed (add PK on code): {result5.Exception.Message}", result5.Exception);
        }

        // 3. Make PK column non-PK: Drop PK on Posts.id, add new postId as PK
        // First, drop FK constraint that references Posts.id
        ResultOrException<int> result6 = await sqlService.Execute("""
            ALTER TABLE [dbo].[Posts] DROP CONSTRAINT [FK_Posts_Users]
            """);
        if (result6.Exception is not null)
        {
            throw new Exception($"Step 2.5 failed (drop FK): {result6.Exception.Message}", result6.Exception);
        }

        // Drop PK on Posts.id
        await DropPrimaryKeyConstraint(sqlService, "Posts");

        // Remove identity from id column (SQL Server only allows one identity per table)
        string dropPostsIdColumnSql = MigrationSqlGenerator.GenerateDropColumnStatement("id", "Posts");
        string addPostsIdColumnSql = MigrationSqlGenerator.GenerateAddColumnStatement(
            new SqlTableColumn(
                Name: "id",
                OrdinalPosition: 0,
                IsNullable: false,
                IsIdentity: false,
                SqlType: SqlDbTypeExt.Int,
                ForeignKeys: [],
                IsComputed: false,
                IsPrimaryKey: false,
                ComputedSql: null,
                LengthOrPrecision: null
            ), 
            "Posts");
        
        ResultOrException<int> result7 = await sqlService.Execute($"""
            {dropPostsIdColumnSql}
            {addPostsIdColumnSql}
            """);
        if (result7.Exception is not null)
        {
            throw new Exception($"Step 2.5 failed (remove identity from Posts.id): {result7.Exception.Message}", result7.Exception);
        }

        // Add new postId column as PK with identity
        ResultOrException<int> result8 = await sqlService.Execute("""
            ALTER TABLE [dbo].[Posts] ADD [postId] INT IDENTITY(1,1) NOT NULL
            ALTER TABLE [dbo].[Posts] ADD CONSTRAINT [PK_Posts] PRIMARY KEY ([postId])
            """);
        if (result8.Exception is not null)
        {
            throw new Exception($"Step 2.5 failed (add postId PK): {result8.Exception.Message}", result8.Exception);
        }

        // Recreate FK constraint (now referencing Users.id, which still exists)
        ResultOrException<int> result9 = await sqlService.Execute("""
            ALTER TABLE [dbo].[Posts] ADD CONSTRAINT [FK_Posts_Users] FOREIGN KEY ([userId]) REFERENCES [dbo].[Users]([id])
            """);
        if (result9.Exception is not null)
        {
            throw new Exception($"Step 2.5 failed (recreate FK): {result9.Exception.Message}", result9.Exception);
        }
    }

    private async Task<MigrationTestState> Step2_6_GenerateComplexMigration(string projectPath, string connectionString, string dbName)
    {
        ResultOrException<MigrationGenerationResult> result = await MigrationGenerator.GenerateIncrementalMigration(
            connectionString, dbName, projectPath, "ComplexSchemaChanges");

        Assert.That(result.Exception, Is.Null, $"Step 2.6 failed: {result.Exception?.Message}");
        Assert.That(result.Result, Is.Not.Null, "Step 2.6: Migration generation returned null result");
        Assert.That(File.Exists(result.Result!.UpScriptPath), Is.True, "Step 2.6: Up script file not created");
        Assert.That(File.Exists(result.Result.DownScriptPath), Is.True, "Step 2.6: Down script file not created");

        ConcurrentDictionary<string, SqlTable> schema = await GetCurrentSchema(connectionString, dbName);
        ResultOrException<List<string>> appliedMigrationsResult = await MigrationApplier.GetAppliedMigrations(connectionString, dbName);
        List<string> appliedMigrations = appliedMigrationsResult.Result ?? new List<string>();

        return new MigrationTestState("Step2.6", schema, appliedMigrations, result.Result.MigrationName);
    }

    private async Task<MigrationTestState> Step2_7_ApplyComplexMigrationAndVerify(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema)
    {
        ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(connectionString, dbName, projectPath, false);

        Assert.That(applyResult.Exception, Is.Null, $"Step 2.7 failed: {applyResult.Exception?.Message}");
        Assert.That(applyResult.Result, Is.Not.Null, "Step 2.7: Apply migrations returned null result");
        Assert.That(applyResult.Result!.AppliedMigrations.Count, Is.EqualTo(2), "Step 2.7: Expected two migrations to be applied (initial + complex)");

        await VerifySchemaMatches(connectionString, dbName, expectedSchema);

        // Return state with the last applied migration (the complex one)
        return await GetCurrentState(connectionString, dbName, projectPath, "Step2.7", applyResult.Result.AppliedMigrations[^1]);
    }

    private async Task Step2_8_RollbackComplexMigration(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema, string migrationName)
    {
        ResultOrException<MigrationRollbackResult> rollbackResult = await MigrationApplier.RollbackMigration(connectionString, dbName, projectPath, migrationName, false);

        Assert.That(rollbackResult.Exception, Is.Null, $"Step 2.8 failed: {rollbackResult.Exception?.Message}");
        Assert.That(rollbackResult.Result, Is.Not.Null, "Step 2.8: Rollback migration returned null result");

        // Delete the rolled-back migration files since they're no longer part of the migration history
        // This ensures that subsequent migrations don't try to apply it
        string migrationsPath = MigrationUtilities.GetMigrationsPath(projectPath);
        string upScriptPath = Path.Combine(migrationsPath, $"{migrationName}.sql");
        string downScriptPath = Path.Combine(migrationsPath, $"{migrationName}.down.sql");
        
        if (File.Exists(upScriptPath))
        {
            File.Delete(upScriptPath);
        }
        if (File.Exists(downScriptPath))
        {
            File.Delete(downScriptPath);
        }

        await VerifySchemaMatches(connectionString, dbName, expectedSchema);
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
            
            -- Create indexes (CRUD: Create)
            CREATE NONCLUSTERED INDEX [IX_Users_createdAt] ON [dbo].[Users]([createdAt])
            CREATE UNIQUE NONCLUSTERED INDEX [IX_Tags_name] ON [dbo].[Tags]([name])
            CREATE NONCLUSTERED INDEX [IX_Posts_tagId] ON [dbo].[Posts]([tagId])
            """);

        if (result.Exception is not null)
        {
            throw new Exception($"Step 3 failed: {result.Exception.Message}", result.Exception);
        }
    }

    private async Task Step5_5_VerifyNoChangesMigration(string projectPath, string connectionString, string dbName, string initialMigrationName, string migration1Name)
    {
        // Ensure migrations are recorded (they may already be recorded from Step 4)
        // We need to record initial + migration #1 so that GenerateIncrementalMigration knows what the target state is
        ResultOrException<List<string>> appliedMigrationsResult = await MigrationApplier.GetAppliedMigrations(connectionString, dbName);
        Assert.That(appliedMigrationsResult.Exception, Is.Null, $"Step 5.5: Failed to get applied migrations: {appliedMigrationsResult.Exception?.Message}");
        Assert.That(appliedMigrationsResult.Result, Is.Not.Null, "Step 5.5: GetAppliedMigrations returned null result");
        
        HashSet<string> appliedSet = appliedMigrationsResult.Result!.ToHashSet();
        
        if (!appliedSet.Contains(initialMigrationName))
        {
            await RecordMigrationApplied(connectionString, dbName, initialMigrationName);
        }
        
        if (!appliedSet.Contains(migration1Name))
        {
            await RecordMigrationApplied(connectionString, dbName, migration1Name);
        }

        // Get count of migration files before attempting to generate
        string migrationsPath = MigrationUtilities.GetMigrationsPath(projectPath);
        int migrationFilesBefore = Directory.Exists(migrationsPath) 
            ? Directory.GetFiles(migrationsPath, "*.sql").Length 
            : 0;

        // Attempt to generate a migration when there are no changes
        ResultOrException<MigrationGenerationResult> result = await MigrationGenerator.GenerateIncrementalMigration(
            connectionString, dbName, projectPath, "NoChangesTest");

        // Should return an exception indicating no changes
        Assert.That(result.Exception, Is.Not.Null, "Step 5.5: Expected exception when generating migration with no changes");
        Assert.That(result.Exception!.Message, Does.Contain("No schema changes detected"), 
            $"Step 5.5: Expected 'No schema changes detected' message, but got: {result.Exception.Message}");
        Assert.That(result.Result, Is.Null, "Step 5.5: Expected null result when no changes detected");

        // Verify no migration files were created
        int migrationFilesAfter = Directory.Exists(migrationsPath) 
            ? Directory.GetFiles(migrationsPath, "*.sql").Length 
            : 0;
        Assert.That(migrationFilesAfter, Is.EqualTo(migrationFilesBefore), 
            "Step 5.5: No migration files should be created when there are no changes");
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
            
            -- Index operations (CRUD: Update/Modify and Delete)
            -- Modify index: Drop IX_Users_createdAt and recreate with different columns (add email)
            DROP INDEX [IX_Users_createdAt] ON [dbo].[Users]
            CREATE NONCLUSTERED INDEX [IX_Users_createdAt_email] ON [dbo].[Users]([createdAt], [email])
            
            -- Drop index: Remove IX_Posts_tagId
            DROP INDEX [IX_Posts_tagId] ON [dbo].[Posts]
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

