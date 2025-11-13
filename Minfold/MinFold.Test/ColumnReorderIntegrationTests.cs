using System.Collections.Concurrent;
using System.Data;
using Microsoft.Data.SqlClient;
using Minfold;
using Newtonsoft.Json;

namespace MinFold.Test;

[TestFixture]
public class ColumnReorderIntegrationTests
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
    public async Task TestColumnReordering()
    {
        MigrationLogger.SetLogger(Console.WriteLine);
        
        string tempDbName = $"MinfoldTest_ColumnReorder_{Guid.NewGuid():N}";
        string tempProjectPath = Path.Combine(Path.GetTempPath(), $"MinfoldTestColumnReorder_{Guid.NewGuid():N}");
        string? tempDbConnectionString = null;

        try
        {
            // Setup: Create temporary database and project
            tempDbConnectionString = await CreateTempDatabase(connSettings.Connection, tempDbName);
            await CreateTempProject(tempProjectPath);
            
            // Step 1: Create initial schema with columns in specific order
            await Step1_CreateInitialSchema(tempDbConnectionString, tempDbName);
            
            // Step 2: Generate initial migration
            MigrationTestState step2State = await Step2_GenerateInitialMigration(tempProjectPath, tempDbConnectionString, tempDbName);
            
            // Verify initial column order
            VerifyColumnOrder(step2State.Schema, "Customers", new List<string> { "id", "firstName", "lastName", "email", "phone", "createdDate" }, "Step 2: Initial migration");
            
            // Record initial migration as applied
            Assert.That(step2State.MigrationName, Is.Not.Null, "Step 2: Initial migration name is null");
            await RecordMigrationApplied(tempDbConnectionString, tempDbName, step2State.MigrationName!);
            
            // Step 3: Reorder columns in the database
            // Change order to: id, lastName, firstName, email, phone, createdDate
            await Step3_ReorderColumns(tempDbConnectionString, tempDbName);
            
            // Step 4: Generate incremental migration for column reordering
            MigrationTestState step4State = await Step4_GenerateIncrementalMigration(tempProjectPath, tempDbConnectionString, tempDbName);
            
            // Verify column order after reordering
            VerifyColumnOrder(step4State.Schema, "Customers", new List<string> { "id", "lastName", "firstName", "email", "phone", "createdDate" }, "Step 4: After reordering");
            
            // Record incremental migration as applied
            Assert.That(step4State.MigrationName, Is.Not.Null, "Step 4: Incremental migration name is null");
            await RecordMigrationApplied(tempDbConnectionString, tempDbName, step4State.MigrationName!);
            
            // Step 5: Apply migration to fresh database and verify column order
            string freshDbName = $"MinfoldTest_ColumnReorder_Fresh_{Guid.NewGuid():N}";
            string freshDbConnectionString = await CreateTempDatabase(connSettings.Connection, freshDbName);
            try
            {
                await Step5_ApplyMigrationsToFreshDatabase(tempProjectPath, freshDbConnectionString, freshDbName, step4State.Schema);
            }
            finally
            {
                await DropTempDatabase(freshDbConnectionString, freshDbName);
            }
            
            // Step 6: Rollback migration and verify column order is restored
            Assert.That(step4State.MigrationName, Is.Not.Null, "Step 6: Incremental migration name is null");
            await Step6_RollbackMigration(tempProjectPath, tempDbConnectionString, tempDbName, step2State.Schema, step4State.MigrationName!);
            
            // Step 7: Reapply migration and verify column order again
            await Step7_ReapplyMigration(tempProjectPath, tempDbConnectionString, tempDbName, step4State.Schema);
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
        SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
        builder.InitialCatalog = "master";
        
        SqlService masterService = new SqlService(builder.ConnectionString);
        ResultOrException<int> dropResult = await masterService.Execute($"""
            IF EXISTS (SELECT * FROM sys.databases WHERE name = '{dbName}')
            BEGIN
                ALTER DATABASE [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{dbName}]
            END
            """);

        if (dropResult.Exception is not null)
        {
            throw new Exception($"Failed to drop temporary database: {dropResult.Exception.Message}", dropResult.Exception);
        }
    }

    private async Task CreateTempProject(string projectPath)
    {
        if (Directory.Exists(projectPath))
        {
            Directory.Delete(projectPath, true);
        }
        Directory.CreateDirectory(projectPath);
        
        // Create migrations directory
        string migrationsPath = Path.Combine(projectPath, "Migrations");
        Directory.CreateDirectory(migrationsPath);
    }

    private async Task Step1_CreateInitialSchema(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        ResultOrException<int> result = await sqlService.Execute("""
            CREATE TABLE [dbo].[Customers] (
                [id] INT PRIMARY KEY IDENTITY(1,1),
                [firstName] NVARCHAR(50) NOT NULL,
                [lastName] NVARCHAR(50) NOT NULL,
                [email] NVARCHAR(100) NOT NULL,
                [phone] NVARCHAR(20) NULL,
                [createdDate] DATETIME2(7) NOT NULL DEFAULT GETUTCDATE()
            )
            """);

        if (result.Exception is not null)
        {
            throw new Exception($"Step 1 failed: {result.Exception.Message}", result.Exception);
        }
    }

    private async Task<MigrationTestState> Step2_GenerateInitialMigration(string projectPath, string connectionString, string dbName)
    {
        ResultOrException<MigrationGenerationResult> result = await MigrationGenerator.GenerateInitialMigration(
            connectionString,
            dbName,
            projectPath,
            "InitialSchema");

        if (result.Exception is not null)
        {
            throw new Exception($"Step 2 failed: {result.Exception.Message}", result.Exception);
        }

        Assert.That(result.Result, Is.Not.Null, "Step 2: Generate initial migration returned null result");
        Assert.That(result.Result!.MigrationName, Is.Not.Null, "Step 2: Migration name is null");
        Assert.That(result.Result.MigrationName, Is.Not.Empty, "Step 2: Migration name is empty");

        ConcurrentDictionary<string, SqlTable> schema = await GetCurrentSchema(connectionString, dbName);
        return new MigrationTestState("Step2", schema, [], result.Result.MigrationName);
    }

    private async Task Step3_ReorderColumns(string connectionString, string dbName)
    {
        // Reorder columns using the same approach the migration generator uses
        // New order: id, lastName, firstName, email, phone, createdDate
        // This simulates what a user would do: manually reorder columns in the database
        SqlService sqlService = new SqlService(connectionString);
        
        // Use the same reordering logic as GenerateColumnReorderStatement
        // Create temp table with new column order
        string tempTableName = "Customers_reorder_test";
        
        ResultOrException<int> result = await sqlService.Execute($"""
            -- Create temporary table with reordered columns (without PK constraint - will add after rename)
            CREATE TABLE [dbo].[{tempTableName}] (
                [id] INT IDENTITY(1,1) NOT NULL,
                [lastName] NVARCHAR(50) NOT NULL,
                [firstName] NVARCHAR(50) NOT NULL,
                [email] NVARCHAR(100) NOT NULL,
                [phone] NVARCHAR(20) NULL,
                [createdDate] DATETIME2(7) NOT NULL DEFAULT GETUTCDATE()
            );
            
            -- Copy data from original table to temporary table
            SET IDENTITY_INSERT [dbo].[{tempTableName}] ON;
            INSERT INTO [dbo].[{tempTableName}] ([id], [lastName], [firstName], [email], [phone], [createdDate])
            SELECT [id], [lastName], [firstName], [email], [phone], [createdDate]
            FROM [dbo].[Customers];
            SET IDENTITY_INSERT [dbo].[{tempTableName}] OFF;
            
            -- Drop original table
            DROP TABLE [dbo].[Customers];
            
            -- Rename temporary table to original name
            EXEC sp_rename '[dbo].[{tempTableName}]', 'Customers', 'OBJECT';
            
            -- Add primary key constraint
            ALTER TABLE [dbo].[Customers] ADD CONSTRAINT [PK_Customers] PRIMARY KEY ([id]);
            """);

        if (result.Exception is not null)
        {
            throw new Exception($"Step 3 failed: {result.Exception.Message}", result.Exception);
        }
    }

    private async Task<MigrationTestState> Step4_GenerateIncrementalMigration(string projectPath, string connectionString, string dbName)
    {
        ResultOrException<MigrationGenerationResult> result = await MigrationGenerator.GenerateIncrementalMigration(
            connectionString,
            dbName,
            projectPath,
            "ReorderColumns");

        if (result.Exception is not null)
        {
            throw new Exception($"Step 4 failed: {result.Exception.Message}", result.Exception);
        }

        Assert.That(result.Result, Is.Not.Null, "Step 4: Generate incremental migration returned null result");
        Assert.That(result.Result!.MigrationName, Is.Not.Null, "Step 4: Migration name is null");
        Assert.That(result.Result.MigrationName, Is.Not.Empty, "Step 4: Migration name is empty");

        ConcurrentDictionary<string, SqlTable> schema = await GetCurrentSchema(connectionString, dbName);
        return new MigrationTestState("Step4", schema, [], result.Result.MigrationName);
    }

    private async Task Step5_ApplyMigrationsToFreshDatabase(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema)
    {
        ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(connectionString, dbName, projectPath, false);

        Assert.That(applyResult.Exception, Is.Null, $"Step 5 failed: {applyResult.Exception?.Message}");
        Assert.That(applyResult.Result, Is.Not.Null, "Step 5: Apply migrations returned null result");
        Assert.That(applyResult.Result!.AppliedMigrations.Count, Is.GreaterThan(0), "Step 5: Expected at least one migration to be applied");

        // Verify schema matches expected schema
        await VerifySchemaMatches(connectionString, dbName, expectedSchema, new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>());
    }

    private async Task Step6_RollbackMigration(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema, string migrationName)
    {
        // Rollback the migration
        ResultOrException<MigrationRollbackResult> rollbackResult = await MigrationApplier.RollbackMigration(connectionString, dbName, projectPath, migrationName, false);

        Assert.That(rollbackResult.Exception, Is.Null, $"Step 6 failed: {rollbackResult.Exception?.Message}");
        Assert.That(rollbackResult.Result, Is.Not.Null, "Step 6: Rollback migration returned null result");
        Assert.That(rollbackResult.Result!.RolledBackMigration, Is.EqualTo(migrationName), "Step 6: Rolled back wrong migration");

        // Verify schema matches expected schema (original order)
        await VerifySchemaMatches(connectionString, dbName, expectedSchema, new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>());
    }

    private async Task Step7_ReapplyMigration(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema)
    {
        ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(connectionString, dbName, projectPath, false);

        Assert.That(applyResult.Exception, Is.Null, $"Step 7 failed: {applyResult.Exception?.Message}");
        Assert.That(applyResult.Result, Is.Not.Null, "Step 7: Apply migrations returned null result");

        // Verify schema matches expected schema (reordered)
        await VerifySchemaMatches(connectionString, dbName, expectedSchema, new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>());
    }

    private async Task RecordMigrationApplied(string connectionString, string dbName, string migrationName)
    {
        SqlService sqlService = new SqlService(connectionString);
        ResultOrException<int> result = await sqlService.Execute($"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '__MinfoldMigrations')
            BEGIN
                CREATE TABLE [dbo].[__MinfoldMigrations] (
                    [MigrationName] NVARCHAR(255) PRIMARY KEY,
                    [AppliedAt] DATETIME2 NOT NULL DEFAULT GETUTCDATE()
                )
            END
            
            IF NOT EXISTS (SELECT * FROM [dbo].[__MinfoldMigrations] WHERE [MigrationName] = '{migrationName}')
            BEGIN
                INSERT INTO [dbo].[__MinfoldMigrations] ([MigrationName]) VALUES ('{migrationName}')
            END
            """);

        if (result.Exception is not null)
        {
            throw new Exception($"Failed to record migration: {result.Exception.Message}", result.Exception);
        }
    }

    private async Task<ConcurrentDictionary<string, SqlTable>> GetCurrentSchema(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        // Exclude __MinfoldMigrations table from schema comparison (it's a system table)
        ResultOrException<ConcurrentDictionary<string, SqlTable>> result = await sqlService.GetSchema(dbName, null, ["__MinfoldMigrations"]);

        if (result.Exception is not null)
        {
            throw new Exception($"Failed to get schema: {result.Exception.Message}", result.Exception);
        }

        return result.Result ?? new ConcurrentDictionary<string, SqlTable>();
    }

    private async Task<(ConcurrentDictionary<string, SqlSequence>, ConcurrentDictionary<string, SqlStoredProcedure>)> GetCurrentSequencesAndProcedures(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        ResultOrException<ConcurrentDictionary<string, SqlSequence>> sequencesResult = await sqlService.GetSequences(dbName);
        if (sequencesResult.Exception is not null)
        {
            throw new Exception($"Failed to get sequences: {sequencesResult.Exception.Message}", sequencesResult.Exception);
        }

        ResultOrException<ConcurrentDictionary<string, SqlStoredProcedure>> proceduresResult = await sqlService.GetStoredProcedures(dbName);
        if (proceduresResult.Exception is not null)
        {
            throw new Exception($"Failed to get procedures: {proceduresResult.Exception.Message}", proceduresResult.Exception);
        }

        return (sequencesResult.Result ?? new ConcurrentDictionary<string, SqlSequence>(), 
                proceduresResult.Result ?? new ConcurrentDictionary<string, SqlStoredProcedure>());
    }

    private async Task VerifySchemaMatches(
        string connectionString,
        string dbName,
        ConcurrentDictionary<string, SqlTable> expectedSchema,
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

    private void VerifyColumnOrder(ConcurrentDictionary<string, SqlTable> schema, string tableName, List<string> expectedOrder, string context)
    {
        if (!schema.TryGetValue(tableName.ToLowerInvariant(), out SqlTable? table))
        {
            Assert.Fail($"{context}: Table '{tableName}' not found in schema");
            return;
        }

        List<string> actualOrder = table.Columns.Values
            .OrderBy(c => c.OrdinalPosition)
            .Select(c => c.Name)
            .ToList();

        Assert.That(actualOrder, Is.EqualTo(expectedOrder), 
            $"{context}: Column order mismatch for table '{tableName}'. Expected: [{string.Join(", ", expectedOrder)}], Actual: [{string.Join(", ", actualOrder)}]");
    }
}

