using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using Minfold;
using Newtonsoft.Json;

namespace MinFold.Test;

public class NotNullColumnTests
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
    public async Task TestNotNullColumnWithDefault()
    {
        MigrationLogger.SetLogger(Console.WriteLine);
        
        string tempDbName = $"MinfoldTest_NotNullColumn_{Guid.NewGuid():N}";
        string tempProjectPath = Path.Combine(Path.GetTempPath(), $"MinfoldTestNotNullColumnProject_{Guid.NewGuid():N}");
        string? tempDbConnectionString = null;

        try
        {
            // Setup: Create temporary database and project
            tempDbConnectionString = await CreateTempDatabase(connSettings.Connection, tempDbName);
            await CreateTempProject(tempProjectPath);
            
            // Step 1: Create initial table with data
            await Step1_CreateTableWithData(tempDbConnectionString, tempDbName);
            
            // Step 2: Generate initial migration
            MigrationTestState step2State = await Step2_GenerateInitialMigration(tempProjectPath, tempDbConnectionString, tempDbName);
            
            // Record initial migration as applied
            Assert.That(step2State.MigrationName, Is.Not.Null, "Step 2: Initial migration name is null");
            await RecordMigrationApplied(tempDbConnectionString, tempDbName, step2State.MigrationName!);
            
            // Step 3: Add NOT NULL column to table with data
            await Step3_AddNotNullColumn(tempDbConnectionString, tempDbName);
            MigrationTestState step3State = await GetCurrentState(tempDbConnectionString, tempDbName, tempProjectPath, "Step3", null);
            
            // Step 4: Generate incremental migration
            MigrationTestState step4State = await Step4_GenerateIncrementalMigration(tempProjectPath, tempDbConnectionString, tempDbName);
            
            // Record migration as applied
            Assert.That(step4State.MigrationName, Is.Not.Null, "Step 4: Migration name is null");
            await RecordMigrationApplied(tempDbConnectionString, tempDbName, step4State.MigrationName!);
            
            // Step 5: Apply migration to fresh database and verify
            string freshDbName = $"MinfoldTest_NotNullColumn_Fresh_{Guid.NewGuid():N}";
            string freshDbConnectionString = await CreateTempDatabase(connSettings.Connection, freshDbName);
            try
            {
                await Step5_ApplyMigrationAndVerify(tempProjectPath, freshDbConnectionString, freshDbName, step3State.Schema);
            }
            finally
            {
                await DropTempDatabase(freshDbConnectionString, freshDbName);
            }
            
            // Step 6: Rollback migration and verify
            await Step6_RollbackMigration(tempProjectPath, tempDbConnectionString, tempDbName, step2State.Schema, step4State.MigrationName!);
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
        SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(baseConnectionString);
        builder.InitialCatalog = "master";
        
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
            Console.WriteLine($"Warning: Failed to drop temporary database {dbName}: {ex.Message}");
        }
    }

    private async Task CreateTempProject(string basePath)
    {
        string migrationsPath = Path.Combine(basePath, "Dao", "Migrations");
        Directory.CreateDirectory(migrationsPath);
        await Task.CompletedTask;
    }

    private async Task Step1_CreateTableWithData(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        
        // Create table
        ResultOrException<int> createResult = await sqlService.Execute("""
            CREATE TABLE [dbo].[SampleTable](
                [id] INT NOT NULL IDENTITY(1,1),
                [text] NVARCHAR(MAX) NULL
            )
            """);

        if (createResult.Exception is not null)
        {
            throw new Exception($"Step 1 failed (create table): {createResult.Exception.Message}", createResult.Exception);
        }
        
        // Add primary key
        ResultOrException<int> pkResult = await sqlService.Execute("""
            ALTER TABLE [dbo].[SampleTable] ADD CONSTRAINT [PK_SampleTable] PRIMARY KEY ([id])
            """);

        if (pkResult.Exception is not null)
        {
            throw new Exception($"Step 1 failed (add PK): {pkResult.Exception.Message}", pkResult.Exception);
        }
        
        // Insert some data
        ResultOrException<int> insertResult = await sqlService.Execute("""
            INSERT INTO [dbo].[SampleTable] ([text]) VALUES ('Test Data 1');
            INSERT INTO [dbo].[SampleTable] ([text]) VALUES ('Test Data 2');
            INSERT INTO [dbo].[SampleTable] ([text]) VALUES ('Test Data 3');
            """);

        if (insertResult.Exception is not null)
        {
            throw new Exception($"Step 1 failed (insert data): {insertResult.Exception.Message}", insertResult.Exception);
        }
    }

    private async Task<MigrationTestState> Step2_GenerateInitialMigration(string projectPath, string connectionString, string dbName)
    {
        ResultOrException<MigrationGenerationResult> result = await MigrationGenerator.GenerateInitialMigration(
            connectionString, dbName, projectPath, "InitialSchema");

        Assert.That(result.Exception, Is.Null, $"Step 2 failed: {result.Exception?.Message}");
        Assert.That(result.Result, Is.Not.Null, "Step 2: Migration generation returned null result");
        Assert.That(File.Exists(result.Result!.UpScriptPath), Is.True, "Step 2: Up script file not created");
        Assert.That(File.Exists(result.Result.DownScriptPath), Is.True, "Step 2: Down script file not created");

        ConcurrentDictionary<string, SqlTable> schema = await GetCurrentSchema(connectionString, dbName);
        return new MigrationTestState("Step2", schema, new List<string>(), result.Result.MigrationName);
    }

    private async Task Step3_AddNotNullColumn(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        
        // Add NOT NULL column - this should use DEFAULT when table has data
        // The migration generator should handle this automatically
        ResultOrException<int> result = await sqlService.Execute("""
            ALTER TABLE [dbo].[SampleTable] ADD [myColumn] INT NOT NULL CONSTRAINT [DF_SampleTable_myColumn_temp] DEFAULT 0;
            """);

        if (result.Exception is not null)
        {
            throw new Exception($"Step 3 failed (add NOT NULL column): {result.Exception.Message}", result.Exception);
        }
        
        // Optionally drop the default constraint after adding (to test that the migration handles it correctly)
        // But for this test, we'll leave it and let the migration generator handle it
    }

    private async Task<MigrationTestState> Step4_GenerateIncrementalMigration(string projectPath, string connectionString, string dbName)
    {
        ResultOrException<MigrationGenerationResult> result = await MigrationGenerator.GenerateIncrementalMigration(
            connectionString, dbName, projectPath, "AddNotNullColumn");

        Assert.That(result.Exception, Is.Null, $"Step 4 failed: {result.Exception?.Message}");
        Assert.That(result.Result, Is.Not.Null, "Step 4: Migration generation returned null result");
        Assert.That(File.Exists(result.Result!.UpScriptPath), Is.True, "Step 4: Up script file not created");
        Assert.That(File.Exists(result.Result.DownScriptPath), Is.True, "Step 4: Down script file not created");

        // Verify that the up script includes a DEFAULT constraint for the NOT NULL column
        // This is critical: when adding a NOT NULL column to a table that exists in targetSchema
        // (meaning it was created by a previous migration and may have data), the migration
        // generator must automatically include a DEFAULT constraint.
        // The fix ensures we check targetSchema (not currentSchema) to determine if table may have data.
        string upScript = await File.ReadAllTextAsync(result.Result.UpScriptPath);
        
        // Verify the migration includes DEFAULT constraint
        Assert.That(upScript.Contains("DEFAULT"), Is.True, 
            "Step 4: Up script should include DEFAULT constraint for NOT NULL column when table exists in targetSchema");
        
        // Verify it's NOT NULL (not just nullable with default)
        Assert.That(upScript.Contains("NOT NULL"), Is.True, 
            "Step 4: Up script should include NOT NULL for the column");
        
        // Verify the pattern: NOT NULL CONSTRAINT ... DEFAULT (the constraint name pattern)
        // The migration generator creates constraint names like: DF_TableName_ColumnName_GUID
        Assert.That(upScript.Contains("CONSTRAINT [DF_"), Is.True, 
            "Step 4: Up script should include a named DEFAULT constraint (DF_TableName_ColumnName_...)");
        
        // Verify the DEFAULT value is 0 (for INT type)
        Assert.That(upScript.Contains("DEFAULT 0"), Is.True, 
            "Step 4: Up script should include DEFAULT 0 for INT NOT NULL column");
        
        // Verify the complete pattern: NOT NULL CONSTRAINT ... DEFAULT 0 appears together
        // This ensures the migration generator correctly handles NOT NULL columns for existing tables
        int notNullIndex = upScript.IndexOf("NOT NULL", StringComparison.OrdinalIgnoreCase);
        int defaultIndex = upScript.IndexOf("DEFAULT 0", StringComparison.OrdinalIgnoreCase);
        Assert.That(notNullIndex, Is.GreaterThan(-1), "Step 4: NOT NULL should be present");
        Assert.That(defaultIndex, Is.GreaterThan(-1), "Step 4: DEFAULT 0 should be present");
        Assert.That(defaultIndex, Is.GreaterThan(notNullIndex), 
            "Step 4: DEFAULT should appear after NOT NULL in the same ALTER TABLE statement");
        
        // Verify it's in an ALTER TABLE ADD statement (not CREATE TABLE)
        Assert.That(upScript.Contains("ALTER TABLE"), Is.True, 
            "Step 4: Should use ALTER TABLE ADD (not CREATE TABLE) for incremental migration");

        ConcurrentDictionary<string, SqlTable> schema = await GetCurrentSchema(connectionString, dbName);
        ResultOrException<List<string>> appliedMigrationsResult = await MigrationApplier.GetAppliedMigrations(connectionString, dbName);
        List<string> appliedMigrations = appliedMigrationsResult.Result ?? new List<string>();

        return new MigrationTestState("Step4", schema, appliedMigrations, result.Result.MigrationName);
    }

    private async Task Step5_ApplyMigrationAndVerify(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema)
    {
        // Get all migrations to apply them in stages
        List<MigrationInfo> allMigrations = MigrationApplier.GetMigrationFiles(projectPath);
        
        // Apply initial migration first
        MigrationInfo? initialMigration = allMigrations.FirstOrDefault(m => m.MigrationName.Contains("InitialSchema"));
        Assert.That(initialMigration, Is.Not.Null, "Step 5: Initial migration not found");
        
        ResultOrException<int> initialResult = await MigrationApplier.ExecuteMigrationScript(connectionString, await File.ReadAllTextAsync(initialMigration!.UpScriptPath));
        Assert.That(initialResult.Exception, Is.Null, $"Step 5 failed (initial migration): {initialResult.Exception?.Message}");
        await MigrationApplier.RecordMigrationApplied(connectionString, initialMigration.MigrationName, dbName);
        
        // Insert test data BEFORE applying the incremental migration that adds the NOT NULL column
        // This tests that existing rows get the default value when a NOT NULL column with DEFAULT is added
        SqlService sqlService = new SqlService(connectionString);
        
        // Insert rows one by one to ensure they all get inserted
        ResultOrException<int> insert1 = await sqlService.Execute("INSERT INTO [dbo].[SampleTable] ([text]) VALUES ('Test Data 1')");
        Assert.That(insert1.Exception, Is.Null, "Step 5: Failed to insert row 1");
        ResultOrException<int> insert2 = await sqlService.Execute("INSERT INTO [dbo].[SampleTable] ([text]) VALUES ('Test Data 2')");
        Assert.That(insert2.Exception, Is.Null, "Step 5: Failed to insert row 2");
        ResultOrException<int> insert3 = await sqlService.Execute("INSERT INTO [dbo].[SampleTable] ([text]) VALUES ('Test Data 3')");
        Assert.That(insert3.Exception, Is.Null, "Step 5: Failed to insert row 3");
        
        // Verify that 3 rows exist before adding the column
        int countBefore = await GetRowCount(connectionString, "SELECT COUNT(*) FROM [dbo].[SampleTable]");
        Assert.That(countBefore, Is.EqualTo(3), "Step 5: Expected 3 rows before adding column");
        
        // Now apply the incremental migration that adds the NOT NULL column with DEFAULT
        // CRITICAL TEST: This verifies the fix works correctly. The migration was generated
        // with the table existing in targetSchema (from the initial migration). When applied
        // to a fresh database with existing data, the migration MUST include a DEFAULT constraint
        // or SQL Server will fail with: "Column cannot be added to non-empty table because it
        // does not satisfy these conditions" (no NULL, no DEFAULT, not IDENTITY, table not empty).
        // 
        // Without the fix (checking currentSchema instead of targetSchema), this would fail
        // because the migration generator wouldn't know the table may have data when applied.
        MigrationInfo? incrementalMigration = allMigrations.FirstOrDefault(m => m.MigrationName.Contains("AddNotNullColumn"));
        Assert.That(incrementalMigration, Is.Not.Null, "Step 5: Incremental migration not found");
        
        // Read the migration script to verify it includes DEFAULT before applying
        string incrementalScript = await File.ReadAllTextAsync(incrementalMigration!.UpScriptPath);
        Assert.That(incrementalScript.Contains("NOT NULL"), Is.True, 
            "Step 5: Migration script must include NOT NULL");
        Assert.That(incrementalScript.Contains("DEFAULT 0"), Is.True, 
            "Step 5: Migration script must include DEFAULT 0 for NOT NULL column when table exists in targetSchema");
        Assert.That(incrementalScript.Contains("CONSTRAINT [DF_"), Is.True, 
            "Step 5: Migration script must include named DEFAULT constraint");
        
        ResultOrException<int> incrementalResult = await MigrationApplier.ExecuteMigrationScript(connectionString, incrementalScript);
        Assert.That(incrementalResult.Exception, Is.Null, 
            $"Step 5 failed (incremental migration): {incrementalResult.Exception?.Message}. " +
            "If this fails with 'Column cannot be added to non-empty table', the migration generator " +
            "is not correctly detecting that the table exists in targetSchema and may have data.");
        await MigrationApplier.RecordMigrationApplied(connectionString, incrementalMigration.MigrationName, dbName);

        await VerifySchemaMatches(connectionString, dbName, expectedSchema, new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>());
        
        // Verify total row count is still 3
        int totalCount = await GetRowCount(connectionString, "SELECT COUNT(*) FROM [dbo].[SampleTable]");
        Assert.That(totalCount, Is.EqualTo(3), "Step 5: Expected 3 total rows after adding column");
        
        // Verify that existing rows got the default value when the NOT NULL column was added
        int countResult = await GetRowCount(connectionString, "SELECT COUNT(*) FROM [dbo].[SampleTable] WHERE [myColumn] = 0");
        // All rows should have myColumn = 0 (the default value)
        Assert.That(countResult, Is.EqualTo(3), "Step 5: All rows should have myColumn = 0 (default value)");
    }

    private async Task Step6_RollbackMigration(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema, string migrationName)
    {
        ResultOrException<MigrationRollbackResult> rollbackResult = await MigrationApplier.RollbackMigration(connectionString, dbName, projectPath, migrationName, false);

        Assert.That(rollbackResult.Exception, Is.Null, $"Step 6 failed: {rollbackResult.Exception?.Message}");
        Assert.That(rollbackResult.Result, Is.Not.Null, "Step 6: Rollback migration returned null result");

        // Delete the rolled-back migration folder
        string migrationsPath = MigrationUtilities.GetMigrationsPath(projectPath);
        string migrationFolder = Path.Combine(migrationsPath, migrationName);
        
        if (Directory.Exists(migrationFolder))
        {
            Directory.Delete(migrationFolder, true);
        }

        await VerifySchemaMatches(connectionString, dbName, expectedSchema, new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>());
    }

    private async Task<ConcurrentDictionary<string, SqlTable>> GetCurrentSchema(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        ResultOrException<ConcurrentDictionary<string, SqlTable>> schemaResult = await sqlService.GetSchema(dbName, null, ["__MinfoldMigrations"]);

        if (schemaResult.Exception is not null || schemaResult.Result is null)
        {
            throw new Exception($"Failed to get current schema: {schemaResult.Exception?.Message ?? "Unknown error"}");
        }

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

    private async Task VerifySchemaMatches(string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema, 
        ConcurrentDictionary<string, SqlSequence> expectedSequences, 
        ConcurrentDictionary<string, SqlStoredProcedure> expectedProcedures)
    {
        ConcurrentDictionary<string, SqlTable> currentSchema = await GetCurrentSchema(connectionString, dbName);
        (ConcurrentDictionary<string, SqlSequence> currentSequences, ConcurrentDictionary<string, SqlStoredProcedure> currentProcedures) = await GetCurrentSequencesAndProcedures(connectionString, dbName);
        
        SchemaDiff diff = MigrationSchemaComparer.CompareSchemas(
            currentSchema, 
            expectedSchema,
            currentSequences,
            expectedSequences,
            currentProcedures,
            expectedProcedures);

        Assert.That(diff.NewTables, Is.Empty, "Schema mismatch: Found unexpected new tables");
        Assert.That(diff.DroppedTableNames, Is.Empty, "Schema mismatch: Found unexpected dropped tables");
        Assert.That(diff.ModifiedTables, Is.Empty, "Schema mismatch: Found unexpected table modifications");
        Assert.That(diff.NewSequences, Is.Empty, "Schema mismatch: Found unexpected new sequences");
        Assert.That(diff.DroppedSequenceNames, Is.Empty, "Schema mismatch: Found unexpected dropped sequences");
        Assert.That(diff.ModifiedSequences, Is.Empty, "Schema mismatch: Found unexpected sequence modifications");
        Assert.That(diff.NewProcedures, Is.Empty, "Schema mismatch: Found unexpected new procedures");
        Assert.That(diff.DroppedProcedureNames, Is.Empty, "Schema mismatch: Found unexpected dropped procedures");
        Assert.That(diff.ModifiedProcedures, Is.Empty, "Schema mismatch: Found unexpected procedure modifications");
    }

    private async Task<MigrationTestState> GetCurrentState(string connectionString, string dbName, string projectPath, string stepName, string? migrationName)
    {
        ConcurrentDictionary<string, SqlTable> schema = await GetCurrentSchema(connectionString, dbName);
        ResultOrException<List<string>> appliedMigrationsResult = await MigrationApplier.GetAppliedMigrations(connectionString, dbName);
        List<string> appliedMigrations = appliedMigrationsResult.Result ?? new List<string>();

        return new MigrationTestState(stepName, schema, appliedMigrations, migrationName);
    }

    private async Task<(ConcurrentDictionary<string, SqlSequence> Sequences, ConcurrentDictionary<string, SqlStoredProcedure> Procedures)> GetCurrentSequencesAndProcedures(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        ResultOrException<ConcurrentDictionary<string, SqlSequence>> sequencesResult = await sqlService.GetSequences(dbName);
        if (sequencesResult.Exception is not null)
        {
            throw new Exception($"Failed to get sequences: {sequencesResult.Exception.Message}");
        }

        ResultOrException<ConcurrentDictionary<string, SqlStoredProcedure>> proceduresResult = await sqlService.GetStoredProcedures(dbName);
        if (proceduresResult.Exception is not null)
        {
            throw new Exception($"Failed to get procedures: {proceduresResult.Exception.Message}");
        }

        return (sequencesResult.Result ?? new ConcurrentDictionary<string, SqlSequence>(), 
                proceduresResult.Result ?? new ConcurrentDictionary<string, SqlStoredProcedure>());
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

    private async Task<int> GetRowCount(string connectionString, string countQuery)
    {
        await using SqlConnection conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        
        SqlCommand command = new SqlCommand(countQuery, conn);
        object? result = await command.ExecuteScalarAsync();
        return result != null ? Convert.ToInt32(result) : 0;
    }
}

