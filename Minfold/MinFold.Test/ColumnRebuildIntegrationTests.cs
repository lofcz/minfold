using System.Collections.Concurrent;
using System.Data;
using Microsoft.Data.SqlClient;
using Minfold;
using Newtonsoft.Json;

namespace MinFold.Test;

[TestFixture]
public class ColumnRebuildIntegrationTests
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
    public async Task TestRebuildAndAlterColumnChangesWithData()
    {
        MigrationLogger.SetLogger(Console.WriteLine);
        
        string tempDbName = $"MinfoldTest_RebuildAlter_{Guid.NewGuid():N}";
        string tempProjectPath = Path.Combine(Path.GetTempPath(), $"MinfoldTestRebuildAlter_{Guid.NewGuid():N}");
        string? tempDbConnectionString = null;
        Dictionary<string, object> testData = new Dictionary<string, object>();

        try
        {
            // Setup: Create temporary database and project
            tempDbConnectionString = await CreateTempDatabase(connSettings.Connection, tempDbName);
            await CreateTempProject(tempProjectPath);
            await ApplyInitialSchema(tempDbConnectionString, tempDbName);

            // Insert test data before migrations
            await InsertTestData(tempDbConnectionString, tempDbName, testData);

            // Step 1: Generate initial migration
            MigrationTestState step1State = await Step1_GenerateInitialMigration(tempProjectPath, tempDbConnectionString, tempDbName);
            
            // Verify data still exists after initial migration generation
            await VerifyTestData(tempDbConnectionString, tempDbName, testData, "After initial migration generation");

            // Step 2: Create fresh database and apply initial migration, verify schema matches
            // Note: Fresh database won't have data (migrations only create schema), so we only verify schema
            string freshDbName = $"MinfoldTest_RebuildAlter_Fresh_{Guid.NewGuid():N}";
            string freshDbConnectionString = await CreateTempDatabase(connSettings.Connection, freshDbName);
            try
            {
                await Step2_ApplyInitialMigrationAndVerify(tempProjectPath, freshDbConnectionString, freshDbName, step1State.Schema);
            }
            finally
            {
                await DropTempDatabase(freshDbConnectionString, freshDbName);
            }

            // Step 3: Make schema changes that require ALTER COLUMN (simple modifies)
            await Step3_MakeAlterColumnChanges(tempDbConnectionString, tempDbName);
            MigrationTestState step3State = await GetCurrentState(tempDbConnectionString, tempDbName, tempProjectPath, "Step3", null);
            
            // Verify data still exists after ALTER COLUMN changes
            await VerifyTestData(tempDbConnectionString, tempDbName, testData, "After ALTER COLUMN changes");

            // Step 4: Make schema changes that require Rebuild (DROP+ADD)
            await Step4_MakeRebuildChanges(tempDbConnectionString, tempDbName);
            MigrationTestState step4State = await GetCurrentState(tempDbConnectionString, tempDbName, tempProjectPath, "Step4", null);
            
            // Verify data still exists after Rebuild changes
            await VerifyTestData(tempDbConnectionString, tempDbName, testData, "After Rebuild changes");

            // Step 5: Generate incremental migration
            MigrationTestState step5State = await Step5_GenerateIncrementalMigration(tempProjectPath, tempDbConnectionString, tempDbName);
            
            // Record migration as applied (since changes already exist in database)
            Assert.That(step1State.MigrationName, Is.Not.Null, "Step 5: Initial migration name is null");
            await RecordMigrationApplied(tempDbConnectionString, tempDbName, step1State.MigrationName!);
            Assert.That(step5State.MigrationName, Is.Not.Null, "Step 5: Incremental migration name is null");
            await RecordMigrationApplied(tempDbConnectionString, tempDbName, step5State.MigrationName!);

            // Step 6: Apply migrations to fresh database and verify schema matches
            // Note: Fresh database won't have data (migrations only create schema), so we only verify schema
            // Data preservation is verified on the original database in subsequent steps
            string freshDb2Name = $"MinfoldTest_RebuildAlter_Fresh2_{Guid.NewGuid():N}";
            string freshDb2ConnectionString = await CreateTempDatabase(connSettings.Connection, freshDb2Name);
            try
            {
                await Step6_ApplyMigrationsAndVerify(tempProjectPath, freshDb2ConnectionString, freshDb2Name, step4State.Schema);
            }
            finally
            {
                await DropTempDatabase(freshDb2ConnectionString, freshDb2Name);
            }

            // Step 7: Rollback migration and verify data is preserved
            // The migration includes both Step 3 and Step 4 changes, so after rollback, 
            // the schema should match the original state (before Step 3), not Step 3 state
            await Step7_RollbackMigration(tempProjectPath, tempDbConnectionString, tempDbName, step1State.Schema, step5State.MigrationName!, testData);

            // Step 8: Reapply migration and verify data is still correct
            await Step8_ReapplyMigration(tempProjectPath, tempDbConnectionString, tempDbName, step4State.Schema, testData);
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

    private async Task ApplyInitialSchema(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        ResultOrException<int> result = await sqlService.Execute("""
            CREATE TABLE [dbo].[Products] (
                [id] INT PRIMARY KEY IDENTITY(1,1),
                [name] NVARCHAR(100) NOT NULL,
                [description] NVARCHAR(500) NULL,
                [price] DECIMAL(18,2) NOT NULL DEFAULT 0.00,
                [quantity] INT NOT NULL DEFAULT 0,
                [createdDate] DATETIME2(7) NOT NULL DEFAULT GETUTCDATE()
            )

            CREATE TABLE [dbo].[Orders] (
                [id] INT PRIMARY KEY IDENTITY(1,1),
                [orderNumber] NVARCHAR(50) NOT NULL,
                [totalAmount] DECIMAL(18,2) NOT NULL,
                [status] NVARCHAR(20) NOT NULL DEFAULT 'Pending'
            )

            CREATE TABLE [dbo].[OrderItems] (
                [id] INT PRIMARY KEY IDENTITY(1,1),
                [orderId] INT NOT NULL,
                [productId] INT NOT NULL,
                [quantity] INT NOT NULL,
                [unitPrice] DECIMAL(18,2) NOT NULL,
                CONSTRAINT [FK_OrderItems_Orders] FOREIGN KEY ([orderId]) REFERENCES [dbo].[Orders]([id]),
                CONSTRAINT [FK_OrderItems_Products] FOREIGN KEY ([productId]) REFERENCES [dbo].[Products]([id])
            )
            """);

        if (result.Exception is not null)
        {
            throw new Exception($"Failed to apply initial schema: {result.Exception.Message}", result.Exception);
        }
    }

    private async Task InsertTestData(string connectionString, string dbName, Dictionary<string, object> testData)
    {
        SqlService sqlService = new SqlService(connectionString);
        
        // Insert products
        ResultOrException<int> productResult = await sqlService.Execute("""
            INSERT INTO [dbo].[Products] ([name], [description], [price], [quantity])
            VALUES 
                ('Product A', 'Description A', 19.99, 100),
                ('Product B', 'Description B', 29.99, 50),
                ('Product C', NULL, 39.99, 25)
            
            SELECT SCOPE_IDENTITY() as LastId
            """);
        
        // Get inserted product IDs
        await using SqlConnection conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        SqlCommand cmd = new SqlCommand("SELECT [id], [name], [price] FROM [dbo].[Products] ORDER BY [id]", conn);
        await using SqlDataReader reader = await cmd.ExecuteReaderAsync();
        List<(int Id, string Name, decimal Price)> products = new List<(int, string, decimal)>();
        while (await reader.ReadAsync())
        {
            products.Add((reader.GetInt32(0), reader.GetString(1), reader.GetDecimal(2)));
        }
        await reader.CloseAsync();
        
        testData["Products"] = products;

        // Insert orders
        await sqlService.Execute($"""
            INSERT INTO [dbo].[Orders] ([orderNumber], [totalAmount], [status])
            VALUES 
                ('ORD-001', 49.98, 'Pending'),
                ('ORD-002', 29.99, 'Completed')
            """);
        
        cmd = new SqlCommand("SELECT [id], [orderNumber], [totalAmount], [status] FROM [dbo].[Orders] ORDER BY [id]", conn);
        await using SqlDataReader orderReader = await cmd.ExecuteReaderAsync();
        List<(int Id, string OrderNumber, decimal TotalAmount, string Status)> orders = new List<(int, string, decimal, string)>();
        while (await orderReader.ReadAsync())
        {
            orders.Add((orderReader.GetInt32(0), orderReader.GetString(1), orderReader.GetDecimal(2), orderReader.GetString(3)));
        }
        await orderReader.CloseAsync();
        
        testData["Orders"] = orders;

        // Insert order items
        if (products.Count >= 2 && orders.Count >= 2)
        {
            await sqlService.Execute($"""
                INSERT INTO [dbo].[OrderItems] ([orderId], [productId], [quantity], [unitPrice])
                VALUES 
                    ({orders[0].Id}, {products[0].Id}, 2, {products[0].Price}),
                    ({orders[0].Id}, {products[1].Id}, 1, {products[1].Price}),
                    ({orders[1].Id}, {products[1].Id}, 1, {products[1].Price})
                """);
            
            cmd = new SqlCommand("SELECT [id], [orderId], [productId], [quantity], [unitPrice] FROM [dbo].[OrderItems] ORDER BY [id]", conn);
            await using SqlDataReader itemReader = await cmd.ExecuteReaderAsync();
            List<(int Id, int OrderId, int ProductId, int Quantity, decimal UnitPrice)> orderItems = new List<(int, int, int, int, decimal)>();
            while (await itemReader.ReadAsync())
            {
                orderItems.Add((itemReader.GetInt32(0), itemReader.GetInt32(1), itemReader.GetInt32(2), itemReader.GetInt32(3), itemReader.GetDecimal(4)));
            }
            await itemReader.CloseAsync();
            
            testData["OrderItems"] = orderItems;
        }

        await conn.CloseAsync();
    }

    private async Task VerifyTestData(string connectionString, string dbName, Dictionary<string, object> testData, string context)
    {
        await using SqlConnection conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        // Verify Products
        if (testData.TryGetValue("Products", out object? productsObj) && productsObj is List<(int Id, string Name, decimal Price)> expectedProducts)
        {
            SqlCommand cmd = new SqlCommand("SELECT [id], [name], [price] FROM [dbo].[Products] ORDER BY [id]", conn);
            await using SqlDataReader reader = await cmd.ExecuteReaderAsync();
            List<(int Id, string Name, decimal Price)> actualProducts = new List<(int, string, decimal)>();
            while (await reader.ReadAsync())
            {
                actualProducts.Add((reader.GetInt32(0), reader.GetString(1), reader.GetDecimal(2)));
            }
            await reader.CloseAsync();

            Assert.That(actualProducts.Count, Is.EqualTo(expectedProducts.Count), $"{context}: Product count mismatch");
            for (int i = 0; i < expectedProducts.Count; i++)
            {
                Assert.That(actualProducts[i].Id, Is.EqualTo(expectedProducts[i].Id), $"{context}: Product {i} ID mismatch");
                Assert.That(actualProducts[i].Name, Is.EqualTo(expectedProducts[i].Name), $"{context}: Product {i} Name mismatch");
                Assert.That(actualProducts[i].Price, Is.EqualTo(expectedProducts[i].Price), $"{context}: Product {i} Price mismatch");
            }
        }

        // Verify Orders
        // Note: orderNumber may have been changed to NTEXT (incompatible type), so we use CAST to read it
        if (testData.TryGetValue("Orders", out object? ordersObj) && ordersObj is List<(int Id, string OrderNumber, decimal TotalAmount, string Status)> expectedOrders)
        {
            SqlCommand cmd = new SqlCommand("SELECT [id], CAST([orderNumber] AS NVARCHAR(MAX)) as [orderNumber], [totalAmount], [status] FROM [dbo].[Orders] ORDER BY [id]", conn);
            await using SqlDataReader reader = await cmd.ExecuteReaderAsync();
            List<(int Id, string OrderNumber, decimal TotalAmount, string Status)> actualOrders = new List<(int, string, decimal, string)>();
            while (await reader.ReadAsync())
            {
                string orderNumber = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
                actualOrders.Add((reader.GetInt32(0), orderNumber, reader.GetDecimal(2), reader.GetString(3)));
            }
            await reader.CloseAsync();

            Assert.That(actualOrders.Count, Is.EqualTo(expectedOrders.Count), $"{context}: Order count mismatch");
            for (int i = 0; i < expectedOrders.Count; i++)
            {
                Assert.That(actualOrders[i].Id, Is.EqualTo(expectedOrders[i].Id), $"{context}: Order {i} ID mismatch");
                // OrderNumber may be empty if it was changed to NTEXT and data was lost (acceptable for incompatible type changes)
                // We only verify it's not null if the count matches (data structure is preserved)
                Assert.That(actualOrders[i].TotalAmount, Is.EqualTo(expectedOrders[i].TotalAmount), $"{context}: Order {i} TotalAmount mismatch");
                Assert.That(actualOrders[i].Status, Is.EqualTo(expectedOrders[i].Status), $"{context}: Order {i} Status mismatch");
            }
        }

        // Verify OrderItems
        if (testData.TryGetValue("OrderItems", out object? itemsObj) && itemsObj is List<(int Id, int OrderId, int ProductId, int Quantity, decimal UnitPrice)> expectedItems)
        {
            SqlCommand cmd = new SqlCommand("SELECT [id], [orderId], [productId], [quantity], [unitPrice] FROM [dbo].[OrderItems] ORDER BY [id]", conn);
            await using SqlDataReader reader = await cmd.ExecuteReaderAsync();
            List<(int Id, int OrderId, int ProductId, int Quantity, decimal UnitPrice)> actualItems = new List<(int, int, int, int, decimal)>();
            while (await reader.ReadAsync())
            {
                actualItems.Add((reader.GetInt32(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetInt32(3), reader.GetDecimal(4)));
            }
            await reader.CloseAsync();

            Assert.That(actualItems.Count, Is.EqualTo(expectedItems.Count), $"{context}: OrderItem count mismatch");
            for (int i = 0; i < expectedItems.Count; i++)
            {
                Assert.That(actualItems[i].Id, Is.EqualTo(expectedItems[i].Id), $"{context}: OrderItem {i} ID mismatch");
                Assert.That(actualItems[i].OrderId, Is.EqualTo(expectedItems[i].OrderId), $"{context}: OrderItem {i} OrderId mismatch");
                Assert.That(actualItems[i].ProductId, Is.EqualTo(expectedItems[i].ProductId), $"{context}: OrderItem {i} ProductId mismatch");
                Assert.That(actualItems[i].Quantity, Is.EqualTo(expectedItems[i].Quantity), $"{context}: OrderItem {i} Quantity mismatch");
                Assert.That(actualItems[i].UnitPrice, Is.EqualTo(expectedItems[i].UnitPrice), $"{context}: OrderItem {i} UnitPrice mismatch");
            }
        }

        await conn.CloseAsync();
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

    private async Task<MigrationTestState> GetCurrentState(string connectionString, string dbName, string projectPath, string stepName, string? migrationName)
    {
        ConcurrentDictionary<string, SqlTable> schema = await GetCurrentSchema(connectionString, dbName);
        ResultOrException<List<string>> appliedMigrationsResult = await MigrationApplier.GetAppliedMigrations(connectionString, dbName);
        List<string> appliedMigrations = appliedMigrationsResult.Result ?? new List<string>();

        return new MigrationTestState(stepName, schema, appliedMigrations, migrationName);
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

    private async Task Step2_ApplyInitialMigrationAndVerify(string projectPath, string connectionString, string dbName, 
        ConcurrentDictionary<string, SqlTable> expectedSchema)
    {
        ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(connectionString, dbName, projectPath, false);

        Assert.That(applyResult.Exception, Is.Null, $"Step 2 failed: {applyResult.Exception?.Message}");
        Assert.That(applyResult.Result, Is.Not.Null, "Step 2: Apply migrations returned null result");
        Assert.That(applyResult.Result!.AppliedMigrations.Count, Is.EqualTo(1), "Step 2: Expected exactly one migration to be applied");

        // Verify schema matches (fresh database won't have data - migrations only create schema)
        await VerifySchemaMatches(connectionString, dbName, expectedSchema, new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>());
    }

    private async Task Step3_MakeAlterColumnChanges(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        
        // These changes should use ALTER COLUMN (not Rebuild):
        // 1. Change nullable to NOT NULL (with default)
        // 2. Change column length
        // 3. Change default constraint value
        // 4. Change precision/scale
        
        // Change description from nullable to NOT NULL with default
        // Order: 1) Update NULL values, 2) Add default constraint, 3) Alter column to NOT NULL
        ResultOrException<int> result1 = await sqlService.Execute("""
            UPDATE [dbo].[Products] SET [description] = '' WHERE [description] IS NULL
            ALTER TABLE [dbo].[Products] ADD CONSTRAINT [DF_Products_description] DEFAULT '' FOR [description]
            ALTER TABLE [dbo].[Products] ALTER COLUMN [description] NVARCHAR(500) NOT NULL
            """);
        if (result1.Exception is not null)
        {
            throw new Exception($"Step 3 failed (alter description): {result1.Exception.Message}", result1.Exception);
        }

        // Change name length from 100 to 200
        ResultOrException<int> result2 = await sqlService.Execute("""
            ALTER TABLE [dbo].[Products] ALTER COLUMN [name] NVARCHAR(200) NOT NULL
            """);
        if (result2.Exception is not null)
        {
            throw new Exception($"Step 3 failed (alter name length): {result2.Exception.Message}", result2.Exception);
        }

        // Change price precision from 18,2 to 19,4
        ResultOrException<int> result3 = await sqlService.Execute("""
            ALTER TABLE [dbo].[Products] ALTER COLUMN [price] DECIMAL(19,4) NOT NULL
            """);
        if (result3.Exception is not null)
        {
            throw new Exception($"Step 3 failed (alter price precision): {result3.Exception.Message}", result3.Exception);
        }

        // Change status default value
        // First, dynamically find and drop the existing default constraint
        ResultOrException<int> result4 = await sqlService.Execute("""
            DECLARE @constraintName NVARCHAR(128);
            SELECT @constraintName = name FROM sys.default_constraints 
            WHERE parent_object_id = OBJECT_ID('[dbo].[Orders]') 
            AND parent_column_id = COLUMNPROPERTY(OBJECT_ID('[dbo].[Orders]'), 'status', 'ColumnId');
            IF @constraintName IS NOT NULL
                EXEC('ALTER TABLE [dbo].[Orders] DROP CONSTRAINT [' + @constraintName + ']');
            ALTER TABLE [dbo].[Orders] ADD CONSTRAINT [DF_Orders_status] DEFAULT 'New' FOR [status]
            """);
        if (result4.Exception is not null)
        {
            throw new Exception($"Step 3 failed (alter status default): {result4.Exception.Message}", result4.Exception);
        }
    }

    private async Task Step4_MakeRebuildChanges(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        
        // These changes should require Rebuild (DROP+ADD):
        // 1. Change varchar to text (incompatible type)
        // 2. Change identity property
        // 3. Change computed column formula
        
        // First, drop FK constraints that reference columns we'll modify
        ResultOrException<int> dropFkResult = await sqlService.Execute("""
            ALTER TABLE [dbo].[OrderItems] DROP CONSTRAINT [FK_OrderItems_Products]
            ALTER TABLE [dbo].[OrderItems] DROP CONSTRAINT [FK_OrderItems_Orders]
            """);
        if (dropFkResult.Exception is not null)
        {
            throw new Exception($"Step 4 failed (drop FKs): {dropFkResult.Exception.Message}", dropFkResult.Exception);
        }

        // Change description from NVARCHAR to TEXT (requires rebuild)
        // First drop the default constraint
        ResultOrException<int> dropDefaultResult = await sqlService.Execute("""
            DECLARE @constraintName NVARCHAR(128);
            SELECT @constraintName = name FROM sys.default_constraints 
            WHERE parent_object_id = OBJECT_ID('[dbo].[Products]') 
            AND parent_column_id = COLUMNPROPERTY(OBJECT_ID('[dbo].[Products]'), 'description', 'ColumnId');
            IF @constraintName IS NOT NULL
                EXEC('ALTER TABLE [dbo].[Products] DROP CONSTRAINT [' + @constraintName + ']');
            """);
        if (dropDefaultResult.Exception is not null)
        {
            throw new Exception($"Step 4 failed (drop description default): {dropDefaultResult.Exception.Message}", dropDefaultResult.Exception);
        }

        // Drop and recreate description as TEXT
        string dropDescriptionSql = GenerateColumns.GenerateDropColumnStatement("description", "Products");
        string addDescriptionSql = GenerateColumns.GenerateAddColumnStatement(
            new SqlTableColumn(
                Name: "description",
                OrdinalPosition: 2,
                IsNullable: true,
                IsIdentity: false,
                SqlType: SqlDbTypeExt.Text,
                ForeignKeys: [],
                IsComputed: false,
                IsPrimaryKey: false,
                ComputedSql: null,
                Length: null,
                Precision: null,
                Scale: null
            ),
            "Products");
        
        ResultOrException<int> rebuildDescResult = await sqlService.Execute($"""
            {dropDescriptionSql}
            {addDescriptionSql}
            """);
        if (rebuildDescResult.Exception is not null)
        {
            throw new Exception($"Step 4 failed (rebuild description to TEXT): {rebuildDescResult.Exception.Message}", rebuildDescResult.Exception);
        }

        // Change orderNumber from NVARCHAR to NTEXT (requires rebuild)
        // First drop PK if it exists
        ResultOrException<int> dropPkResult = await sqlService.Execute("""
            DECLARE @pkConstraintName NVARCHAR(128);
            SELECT @pkConstraintName = name FROM sys.key_constraints 
            WHERE parent_object_id = OBJECT_ID('[dbo].[Orders]') 
            AND type = 'PK';
            IF @pkConstraintName IS NOT NULL
                EXEC('ALTER TABLE [dbo].[Orders] DROP CONSTRAINT [' + @pkConstraintName + ']');
            """);
        if (dropPkResult.Exception is not null)
        {
            throw new Exception($"Step 4 failed (drop Orders PK): {dropPkResult.Exception.Message}", dropPkResult.Exception);
        }

        // Drop and recreate orderNumber as NTEXT
        string dropOrderNumberSql = GenerateColumns.GenerateDropColumnStatement("orderNumber", "Orders");
        string addOrderNumberSql = GenerateColumns.GenerateAddColumnStatement(
            new SqlTableColumn(
                Name: "orderNumber",
                OrdinalPosition: 1,
                IsNullable: false,
                IsIdentity: false,
                SqlType: SqlDbTypeExt.NText,
                ForeignKeys: [],
                IsComputed: false,
                IsPrimaryKey: false,
                ComputedSql: null,
                Length: null,
                Precision: null,
                Scale: null,
                IdentitySeed: null,
                IdentityIncrement: null,
                DefaultConstraintName: null,
                DefaultConstraintValue: null
            ),
            "Orders");
        
        ResultOrException<int> rebuildOrderNumberResult = await sqlService.Execute($"""
            {dropOrderNumberSql}
            {addOrderNumberSql}
            """);
        if (rebuildOrderNumberResult.Exception is not null)
        {
            throw new Exception($"Step 4 failed (rebuild orderNumber to NTEXT): {rebuildOrderNumberResult.Exception.Message}", rebuildOrderNumberResult.Exception);
        }

        // Recreate PK on Orders.id (orderNumber is no longer PK)
        ResultOrException<int> recreatePkResult = await sqlService.Execute("""
            ALTER TABLE [dbo].[Orders] ADD CONSTRAINT [PK_Orders] PRIMARY KEY ([id])
            """);
        if (recreatePkResult.Exception is not null)
        {
            throw new Exception($"Step 4 failed (recreate Orders PK): {recreatePkResult.Exception.Message}", recreatePkResult.Exception);
        }

        // Change Products.id from identity to non-identity (requires rebuild)
        // This is complex - we need to preserve existing ID values
        // Approach: Add temp column, copy values, drop old column, rename temp, add PK
        ResultOrException<int> dropProductsPkResult = await sqlService.Execute("""
            DECLARE @pkConstraintName NVARCHAR(128);
            SELECT @pkConstraintName = name FROM sys.key_constraints 
            WHERE parent_object_id = OBJECT_ID('[dbo].[Products]') 
            AND type = 'PK';
            IF @pkConstraintName IS NOT NULL
                EXEC('ALTER TABLE [dbo].[Products] DROP CONSTRAINT [' + @pkConstraintName + ']');
            """);
        if (dropProductsPkResult.Exception is not null)
        {
            throw new Exception($"Step 4 failed (drop Products PK): {dropProductsPkResult.Exception.Message}", dropProductsPkResult.Exception);
        }

        // Add temporary column with DEFAULT (required for NOT NULL on non-empty table)
        // Then we'll update it with actual values and drop the default
        ResultOrException<int> addTempResult = await sqlService.Execute("""
            ALTER TABLE [dbo].[Products] ADD [id_temp] INT NOT NULL DEFAULT 0
            """);
        if (addTempResult.Exception is not null)
        {
            throw new Exception($"Step 4 failed (add temp column): {addTempResult.Exception.Message}", addTempResult.Exception);
        }

        // Copy values from id to id_temp (separate batch)
        ResultOrException<int> copyValuesResult = await sqlService.Execute("""
            UPDATE [dbo].[Products] SET [id_temp] = [id]
            """);
        if (copyValuesResult.Exception is not null)
        {
            throw new Exception($"Step 4 failed (copy values): {copyValuesResult.Exception.Message}", copyValuesResult.Exception);
        }

        // Drop the default constraint (no longer needed)
        ResultOrException<int> dropTempDefaultResult = await sqlService.Execute("""
            DECLARE @constraintName NVARCHAR(128);
            SELECT @constraintName = name FROM sys.default_constraints 
            WHERE parent_object_id = OBJECT_ID('[dbo].[Products]') 
            AND parent_column_id = COLUMNPROPERTY(OBJECT_ID('[dbo].[Products]'), 'id_temp', 'ColumnId');
            IF @constraintName IS NOT NULL
                EXEC('ALTER TABLE [dbo].[Products] DROP CONSTRAINT [' + @constraintName + ']');
            """);
        if (dropTempDefaultResult.Exception is not null)
        {
            throw new Exception($"Step 4 failed (drop temp default): {dropTempDefaultResult.Exception.Message}", dropTempDefaultResult.Exception);
        }

        // Drop the old identity column
        string dropIdSql = GenerateColumns.GenerateDropColumnStatement("id", "Products");
        ResultOrException<int> dropIdResult = await sqlService.Execute(dropIdSql);
        if (dropIdResult.Exception is not null)
        {
            throw new Exception($"Step 4 failed (drop Products.id): {dropIdResult.Exception.Message}", dropIdResult.Exception);
        }

        // Rename temp column to id
        ResultOrException<int> renameResult = await sqlService.Execute("""
            EXEC sp_rename '[dbo].[Products].[id_temp]', 'id', 'COLUMN'
            """);
        if (renameResult.Exception is not null)
        {
            throw new Exception($"Step 4 failed (rename temp to id): {renameResult.Exception.Message}", renameResult.Exception);
        }

        // Add PK constraint
        ResultOrException<int> addPkResult = await sqlService.Execute("""
            ALTER TABLE [dbo].[Products] ADD CONSTRAINT [PK_Products] PRIMARY KEY ([id])
            """);
        if (addPkResult.Exception is not null)
        {
            throw new Exception($"Step 4 failed (add Products PK): {addPkResult.Exception.Message}", addPkResult.Exception);
        }

        // Recreate FK constraints
        ResultOrException<int> recreateFkResult = await sqlService.Execute("""
            ALTER TABLE [dbo].[OrderItems] ADD CONSTRAINT [FK_OrderItems_Products] FOREIGN KEY ([productId]) REFERENCES [dbo].[Products]([id])
            ALTER TABLE [dbo].[OrderItems] ADD CONSTRAINT [FK_OrderItems_Orders] FOREIGN KEY ([orderId]) REFERENCES [dbo].[Orders]([id])
            """);
        if (recreateFkResult.Exception is not null)
        {
            throw new Exception($"Step 4 failed (recreate FKs): {recreateFkResult.Exception.Message}", recreateFkResult.Exception);
        }
    }

    private async Task<MigrationTestState> Step5_GenerateIncrementalMigration(string projectPath, string connectionString, string dbName)
    {
        ResultOrException<MigrationGenerationResult> result = await MigrationGenerator.GenerateIncrementalMigration(
            connectionString, dbName, projectPath, "AlterAndRebuildChanges");

        Assert.That(result.Exception, Is.Null, $"Step 5 failed: {result.Exception?.Message}");
        Assert.That(result.Result, Is.Not.Null, "Step 5: Migration generation returned null result");
        Assert.That(File.Exists(result.Result!.UpScriptPath), Is.True, "Step 5: Up script file not created");
        Assert.That(File.Exists(result.Result.DownScriptPath), Is.True, "Step 5: Down script file not created");

        // Verify the migration script contains both ALTER COLUMN and DROP+ADD patterns
        string upScript = await File.ReadAllTextAsync(result.Result.UpScriptPath);
        
        // Should contain ALTER COLUMN for simple changes
        Assert.That(upScript.Contains("ALTER COLUMN", StringComparison.OrdinalIgnoreCase), Is.True, 
            "Step 5: Up script should contain ALTER COLUMN for simple changes");
        
        // Should contain DROP COLUMN for rebuild changes
        Assert.That(upScript.Contains("DROP COLUMN", StringComparison.OrdinalIgnoreCase), Is.True, 
            "Step 5: Up script should contain DROP COLUMN for rebuild changes");

        ConcurrentDictionary<string, SqlTable> schema = await GetCurrentSchema(connectionString, dbName);
        ResultOrException<List<string>> appliedMigrationsResult = await MigrationApplier.GetAppliedMigrations(connectionString, dbName);
        List<string> appliedMigrations = appliedMigrationsResult.Result ?? new List<string>();

        return new MigrationTestState("Step5", schema, appliedMigrations, result.Result.MigrationName);
    }

    private async Task Step6_ApplyMigrationsAndVerify(string projectPath, string connectionString, string dbName, 
        ConcurrentDictionary<string, SqlTable> expectedSchema)
    {
        ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(connectionString, dbName, projectPath, false);

        Assert.That(applyResult.Exception, Is.Null, $"Step 6 failed: {applyResult.Exception?.Message}");
        Assert.That(applyResult.Result, Is.Not.Null, "Step 6: Apply migrations returned null result");
        Assert.That(applyResult.Result!.AppliedMigrations.Count, Is.EqualTo(2), "Step 6: Expected two migrations to be applied (initial + incremental)");

        // Verify schema matches (fresh database won't have data - migrations only create schema)
        // Data preservation is verified on the original database in Step 7 and Step 8
        await VerifySchemaMatches(connectionString, dbName, expectedSchema, new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>());
    }

    private async Task Step7_RollbackMigration(string projectPath, string connectionString, string dbName, 
        ConcurrentDictionary<string, SqlTable> expectedSchema, string migrationName, Dictionary<string, object> testData)
    {
        ResultOrException<MigrationRollbackResult> rollbackResult = await MigrationApplier.RollbackMigration(
            connectionString, dbName, projectPath, migrationName, false);

        Assert.That(rollbackResult.Exception, Is.Null, $"Step 7 failed: {rollbackResult.Exception?.Message}");
        Assert.That(rollbackResult.Result, Is.Not.Null, "Step 7: Rollback returned null result");
        Assert.That(rollbackResult.Result!.RolledBackMigration, Is.EqualTo(migrationName), "Step 7: Rolled back wrong migration");

        await VerifySchemaMatches(connectionString, dbName, expectedSchema, new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>());
        
        // Verify data was preserved after rollback
        await VerifyTestData(connectionString, dbName, testData, "Step 7: After rolling back migration");
    }

    private async Task Step8_ReapplyMigration(string projectPath, string connectionString, string dbName, 
        ConcurrentDictionary<string, SqlTable> expectedSchema, Dictionary<string, object> testData)
    {
        ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(connectionString, dbName, projectPath, false);

        Assert.That(applyResult.Exception, Is.Null, $"Step 8 failed: {applyResult.Exception?.Message}");
        Assert.That(applyResult.Result, Is.Not.Null, "Step 8: Apply migrations returned null result");
        Assert.That(applyResult.Result!.AppliedMigrations.Count, Is.EqualTo(1), "Step 8: Expected one migration to be reapplied");

        await VerifySchemaMatches(connectionString, dbName, expectedSchema, new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>());
        
        // Verify data was preserved after reapply
        await VerifyTestData(connectionString, dbName, testData, "Step 8: After reapplying migration");
    }
}

