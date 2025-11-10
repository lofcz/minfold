using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using Minfold;
using Newtonsoft.Json;

namespace MinFold.Test;

public class DefaultConstraintTests
{
    private static SqlSettings connSettings;
    private SqlService SqlService => new SqlService(connSettings.Connection);

    public record SqlSettings(string Connection, string Database);

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
    public async Task TestDefaultConstraintPreservation()
    {
        MigrationLogger.SetLogger(Console.WriteLine);
        
        string tempDbName = $"MinfoldTest_DefaultConstraints_{Guid.NewGuid():N}";
        string tempProjectPath = Path.Combine(Path.GetTempPath(), $"MinfoldTestDefaultConstraintsProject_{Guid.NewGuid():N}");
        string? tempDbConnectionString = null;

        try
        {
            // Setup: Create temporary database and project
            tempDbConnectionString = await CreateTempDatabase(connSettings.Connection, tempDbName);
            await CreateTempProject(tempProjectPath);
            
            // Step 1: Create table with various default constraint types
            await Step1_CreateTableWithDefaultConstraints(tempDbConnectionString, tempDbName);
            
            // Step 2: Generate initial migration and verify default constraints are captured
            await Step2_GenerateInitialMigrationAndVerify(tempProjectPath, tempDbConnectionString, tempDbName);
            
            // Step 3: Add a new column with a default constraint
            await Step3_AddColumnWithDefaultConstraint(tempDbConnectionString, tempDbName);
            
            // Step 4: Generate incremental migration and verify default constraint is preserved
            await Step4_GenerateIncrementalMigrationAndVerify(tempProjectPath, tempDbConnectionString, tempDbName);
            
            // Step 5: Apply migrations to fresh database and verify all default constraints are preserved
            await Step5_ApplyToFreshDatabaseAndVerify(tempProjectPath, tempDbConnectionString, tempDbName);
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

    private async Task Step1_CreateTableWithDefaultConstraints(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        
        // Create table with various default constraint types:
        // - INT with DEFAULT 0
        // - NVARCHAR with DEFAULT 'test'
        // - DATETIME2 with DEFAULT GETDATE()
        // - BIT with DEFAULT 1
        ResultOrException<int> result = await sqlService.Execute("""
            CREATE TABLE [dbo].[DefaultTestTable] (
                [id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
                [intValue] INT NOT NULL CONSTRAINT [DF_DefaultTestTable_intValue] DEFAULT 0,
                [stringValue] NVARCHAR(100) NOT NULL CONSTRAINT [DF_DefaultTestTable_stringValue] DEFAULT 'test',
                [dateValue] DATETIME2(7) NOT NULL CONSTRAINT [DF_DefaultTestTable_dateValue] DEFAULT GETDATE(),
                [boolValue] BIT NOT NULL CONSTRAINT [DF_DefaultTestTable_boolValue] DEFAULT 1
            )
            """);

        if (result.Exception is not null)
        {
            throw new Exception($"Step 1 failed: {result.Exception.Message}", result.Exception);
        }
    }

    private async Task Step2_GenerateInitialMigrationAndVerify(string projectPath, string connectionString, string dbName)
    {
        // First, check what default constraints are captured from the database
        ConcurrentDictionary<string, SqlTable> currentSchema = await GetCurrentSchema(connectionString, dbName);
        if (currentSchema.TryGetValue("defaulttesttable", out SqlTable? table))
        {
            Console.WriteLine("=== Default Constraints Captured from Database ===");
            foreach (var col in table.Columns.Values.OrderBy(c => c.OrdinalPosition))
            {
                Console.WriteLine($"Column: {col.Name}");
                Console.WriteLine($"  DefaultConstraintName: {col.DefaultConstraintName ?? "(null)"}");
                Console.WriteLine($"  DefaultConstraintValue: {col.DefaultConstraintValue ?? "(null)"}");
                Console.WriteLine($"  IsNullable: {col.IsNullable}");
                Console.WriteLine($"  IsIdentity: {col.IsIdentity}");
            }
        }
        
        ResultOrException<MigrationGenerationResult> result = await MigrationGenerator.GenerateInitialMigration(
            connectionString, dbName, projectPath, "InitialSchema");

        Assert.That(result.Exception, Is.Null, $"Step 2 failed: {result.Exception?.Message}");
        Assert.That(result.Result, Is.Not.Null, "Step 2: Migration generation returned null result");
        
        // Verify the up script includes all default constraints
        string upScript = await File.ReadAllTextAsync(result.Result!.UpScriptPath);
        
        Console.WriteLine("\n=== Generated Migration Script (first 2000 chars) ===");
        Console.WriteLine(upScript.Substring(0, Math.Min(2000, upScript.Length)));
        Console.WriteLine("\n=== Searching for DEFAULT values ===");
        Console.WriteLine($"Contains 'DEFAULT 0': {upScript.Contains("DEFAULT 0")}");
        Console.WriteLine($"Contains 'DEFAULT 'test'': {upScript.Contains("DEFAULT 'test'")}");
        Console.WriteLine($"Contains 'DEFAULT GETDATE()': {upScript.Contains("DEFAULT GETDATE()")}");
        Console.WriteLine($"Contains 'DEFAULT 1': {upScript.Contains("DEFAULT 1")}");
        Console.WriteLine($"Contains 'GETDATE': {upScript.Contains("GETDATE")}");
        Console.WriteLine($"Contains 'getdate': {upScript.Contains("getdate", StringComparison.OrdinalIgnoreCase)}");
        
        // Find all DEFAULT occurrences
        int defaultIndex = 0;
        while ((defaultIndex = upScript.IndexOf("DEFAULT", defaultIndex)) != -1)
        {
            int endIndex = Math.Min(defaultIndex + 100, upScript.Length);
            Console.WriteLine($"Found DEFAULT at position {defaultIndex}: {upScript.Substring(defaultIndex, endIndex - defaultIndex)}");
            defaultIndex++;
        }
        
        Assert.That(upScript.Contains("DEFAULT 0"), Is.True, "Step 2: Up script should include DEFAULT 0 for intValue");
        Assert.That(upScript.Contains("DEFAULT 'test'"), Is.True, "Step 2: Up script should include DEFAULT 'test' for stringValue");
        // SQL Server stores GETDATE() as lowercase getdate() in default constraints, so check case-insensitively
        Assert.That(upScript.Contains("DEFAULT getdate()", StringComparison.OrdinalIgnoreCase), Is.True, 
            "Step 2: Up script should include DEFAULT getdate() (or GETDATE()) for dateValue");
        Assert.That(upScript.Contains("DEFAULT 1"), Is.True, "Step 2: Up script should include DEFAULT 1 for boolValue");
        
        // Verify constraint names are preserved or generated
        Assert.That(upScript.Contains("DF_DefaultTestTable_intValue"), Is.True, "Step 2: Up script should include constraint name for intValue");
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

    private async Task Step3_AddColumnWithDefaultConstraint(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        
        // Add a new column with a default constraint
        ResultOrException<int> result = await sqlService.Execute("""
            ALTER TABLE [dbo].[DefaultTestTable] 
            ADD [newColumn] INT NOT NULL CONSTRAINT [DF_DefaultTestTable_newColumn] DEFAULT 42
            """);

        if (result.Exception is not null)
        {
            throw new Exception($"Step 3 failed: {result.Exception.Message}", result.Exception);
        }
    }

    private async Task Step4_GenerateIncrementalMigrationAndVerify(string projectPath, string connectionString, string dbName)
    {
        // Record initial migration as applied
        await MigrationApplier.EnsureMigrationsTableExists(connectionString, dbName);
        List<MigrationInfo> migrations = MigrationApplier.GetMigrationFiles(projectPath);
        MigrationInfo? initialMigration = migrations.FirstOrDefault(m => m.MigrationName.Contains("InitialSchema"));
        if (initialMigration != null)
        {
            await MigrationApplier.RecordMigrationApplied(connectionString, initialMigration.MigrationName, dbName);
        }
        
        ResultOrException<MigrationGenerationResult> result = await MigrationGenerator.GenerateIncrementalMigration(
            connectionString, dbName, projectPath, "AddNewColumnWithDefault");

        Assert.That(result.Exception, Is.Null, $"Step 4 failed: {result.Exception?.Message}");
        Assert.That(result.Result, Is.Not.Null, "Step 4: Migration generation returned null result");
        
        // Verify the up script includes the new column's default constraint
        string upScript = await File.ReadAllTextAsync(result.Result!.UpScriptPath);
        
        // SQL Server stores default values with parentheses, so check for the normalized value
        Assert.That(upScript.Contains("DEFAULT 42"), Is.True, "Step 4: Up script should include DEFAULT 42 for newColumn");
        Assert.That(upScript.Contains("NOT NULL"), Is.True, "Step 4: Up script should include NOT NULL for newColumn");
        
        // Verify it's an ALTER TABLE ADD statement (not CREATE TABLE)
        Assert.That(upScript.Contains("ALTER TABLE"), Is.True, "Step 4: Should use ALTER TABLE ADD for incremental migration");
    }

    private async Task Step5_ApplyToFreshDatabaseAndVerify(string projectPath, string connectionString, string dbName)
    {
        // Create a fresh database
        string freshDbName = $"MinfoldTest_DefaultConstraints_Fresh_{Guid.NewGuid():N}";
        string freshDbConnectionString = await CreateTempDatabase(connSettings.Connection, freshDbName);
        
        try
        {
            // Check what migrations exist before applying
            List<MigrationInfo> allMigrations = MigrationApplier.GetMigrationFiles(projectPath);
            Console.WriteLine($"\n=== Migrations found (before applying) ===");
            foreach (var migration in allMigrations)
            {
                Console.WriteLine($"  {migration.MigrationName} (Timestamp: {migration.Timestamp})");
            }
            
            // Check applied migrations (should be empty for fresh database)
            ResultOrException<List<string>> appliedBefore = await MigrationApplier.GetAppliedMigrations(freshDbConnectionString, freshDbName);
            Console.WriteLine($"\n=== Applied migrations (before): {string.Join(", ", appliedBefore.Result ?? new List<string>())} ===");
            
            // Apply all migrations
            ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(
                freshDbConnectionString, freshDbName, projectPath, false);

            Assert.That(applyResult.Exception, Is.Null, $"Step 5 failed: {applyResult.Exception?.Message}");
            Assert.That(applyResult.Result, Is.Not.Null, "Step 5: Apply migrations returned null result");
            Assert.That(applyResult.Result!.AppliedMigrations.Count, Is.GreaterThanOrEqualTo(2), 
                "Step 5: Expected at least 2 migrations to be applied");
            
            // Verify schema matches - get current schema from fresh database
            SqlService sqlService = new SqlService(freshDbConnectionString);
            ResultOrException<ConcurrentDictionary<string, SqlTable>> schemaResult = await sqlService.GetSchema(
                freshDbName, null, ["__MinfoldMigrations"]);
            
            Assert.That(schemaResult.Exception, Is.Null, "Step 5: Failed to get schema from fresh database");
            Assert.That(schemaResult.Result, Is.Not.Null, "Step 5: Schema result is null");
            
            // Verify the table exists and has all columns with default constraints
            Assert.That(schemaResult.Result!.ContainsKey("defaulttesttable"), Is.True, 
                "Step 5: Table should exist in fresh database");
            
            SqlTable table = schemaResult.Result["defaulttesttable"];
            
            // Verify all columns exist
            Assert.That(table.Columns.ContainsKey("intvalue"), Is.True, "Step 5: intValue column should exist");
            Assert.That(table.Columns.ContainsKey("stringvalue"), Is.True, "Step 5: stringValue column should exist");
            Assert.That(table.Columns.ContainsKey("datevalue"), Is.True, "Step 5: dateValue column should exist");
            Assert.That(table.Columns.ContainsKey("boolvalue"), Is.True, "Step 5: boolValue column should exist");
            Assert.That(table.Columns.ContainsKey("newcolumn"), Is.True, "Step 5: newColumn should exist");
            
            // Verify default constraints are preserved
            SqlTableColumn intValueCol = table.Columns["intvalue"];
            Assert.That(intValueCol.DefaultConstraintValue, Is.Not.Null.And.Not.Empty, 
                "Step 5: intValue should have a default constraint value");
            Assert.That(intValueCol.DefaultConstraintValue!.Contains("0"), Is.True, 
                "Step 5: intValue default constraint should contain 0");
            
            SqlTableColumn stringValueCol = table.Columns["stringvalue"];
            Assert.That(stringValueCol.DefaultConstraintValue, Is.Not.Null.And.Not.Empty, 
                "Step 5: stringValue should have a default constraint value");
            Assert.That(stringValueCol.DefaultConstraintValue!.Contains("test"), Is.True, 
                "Step 5: stringValue default constraint should contain 'test'");
            
            SqlTableColumn newColumnCol = table.Columns["newcolumn"];
            Assert.That(newColumnCol.DefaultConstraintValue, Is.Not.Null.And.Not.Empty, 
                "Step 5: newColumn should have a default constraint value");
            Assert.That(newColumnCol.DefaultConstraintValue!.Contains("42"), Is.True, 
                "Step 5: newColumn default constraint should contain 42");
            
            // Test that default constraints actually work by inserting a row without specifying values
            // Note: id is IDENTITY, so we don't specify it - SQL Server will auto-generate it
            ResultOrException<int> insertResult = await sqlService.Execute("""
                INSERT INTO [dbo].[DefaultTestTable] DEFAULT VALUES
                """);
            
            Assert.That(insertResult.Exception, Is.Null, 
                "Step 5: Should be able to insert row using default values");
            
            // Verify the defaults were applied
            await using SqlConnection conn = new SqlConnection(freshDbConnectionString);
            await conn.OpenAsync();
            
            // Get the inserted row (id will be auto-generated, so get the most recent one)
            SqlCommand selectCmd = new SqlCommand(
                "SELECT TOP 1 [intValue], [stringValue], [boolValue], [newColumn] FROM [dbo].[DefaultTestTable] ORDER BY [id] DESC", 
                conn);
            
            await using SqlDataReader reader = await selectCmd.ExecuteReaderAsync();
            Assert.That(await reader.ReadAsync(), Is.True, "Step 5: Should be able to read inserted row");
            
            Assert.That(reader.GetInt32(0), Is.EqualTo(0), "Step 5: intValue should be 0 (default)");
            Assert.That(reader.GetString(1), Is.EqualTo("test"), "Step 5: stringValue should be 'test' (default)");
            Assert.That(reader.GetBoolean(2), Is.EqualTo(true), "Step 5: boolValue should be 1 (default)");
            Assert.That(reader.GetInt32(3), Is.EqualTo(42), "Step 5: newColumn should be 42 (default)");
        }
        finally
        {
            await DropTempDatabase(freshDbConnectionString, freshDbName);
        }
    }
}

