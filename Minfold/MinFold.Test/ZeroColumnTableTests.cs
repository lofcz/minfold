using System.Collections.Concurrent;
using System.Text;
using Microsoft.Data.SqlClient;
using Minfold;
using Newtonsoft.Json;

namespace MinFold.Test;

public class ZeroColumnTableTests
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
    public async Task TestZeroColumnScenarios()
    {
        MigrationLogger.SetLogger(Console.WriteLine);
        
        string tempDbName = $"MinfoldTest_ZeroColumn_{Guid.NewGuid():N}";
        string tempProjectPath = Path.Combine(Path.GetTempPath(), $"MinfoldTestZeroColumnProject_{Guid.NewGuid():N}");
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
            
            // Verify column order after initial migration
            if (step1State.Schema.TryGetValue("testtable", out SqlTable? step1Table))
            {
                List<string> step1ColumnOrder = GetColumnOrder(step1Table);
                Assert.That(step1ColumnOrder, Is.EqualTo(new List<string> { "id", "col1", "col2", "col3" }), 
                    "Step 1: Initial migration column order should be id, col1, col2, col3");
            }

            // Step 2: Scenario 1 - Remove multiple columns in succession to reduce table to 0 columns
            await Step2_RemoveMultipleColumnsInSuccession(tempDbConnectionString, tempDbName);
            MigrationTestState step2State = await GetCurrentState(tempDbConnectionString, tempDbName, tempProjectPath, "Step2", null);
            stateHistory["Step2"] = step2State;

            // Step 3: Generate migration for scenario 1
            MigrationTestState step3State = await Step3_GenerateMigrationForScenario1(tempProjectPath, tempDbConnectionString, tempDbName);
            stateHistory["Step3"] = step3State;

            // Record migration as applied
            Assert.That(step3State.MigrationName, Is.Not.Null, "Step 3: Migration name is null");
            await RecordMigrationApplied(tempDbConnectionString, tempDbName, step3State.MigrationName!);

            // Step 4: Apply migration to fresh database and verify
            string freshDb1Name = $"MinfoldTest_ZeroColumn_Fresh1_{Guid.NewGuid():N}";
            string freshDb1ConnectionString = await CreateTempDatabase(connSettings.Connection, freshDb1Name);
            try
            {
                await Step4_ApplyScenario1MigrationAndVerify(tempProjectPath, freshDb1ConnectionString, freshDb1Name, step2State.Schema);
                
                // Verify column order is preserved after applying migration to fresh database
                ConcurrentDictionary<string, SqlTable> freshDb1Schema = await GetCurrentSchema(freshDb1ConnectionString, freshDb1Name);
                if (freshDb1Schema.TryGetValue("testtable", out SqlTable? freshDb1Table) && 
                    step2State.Schema.TryGetValue("testtable", out SqlTable? step2Table))
                {
                    VerifyColumnOrder(freshDb1Table, step2Table, "Step 4: Fresh database after applying migration");
                }
            }
            finally
            {
                await DropTempDatabase(freshDb1ConnectionString, freshDb1Name);
            }

            // Step 5: Rollback scenario 1 migration and verify schema restored
            await Step5_RollbackScenario1Migration(tempProjectPath, tempDbConnectionString, tempDbName, step1State.Schema, step3State.MigrationName!);
            
            // Verify column order is preserved after rollback
            ConcurrentDictionary<string, SqlTable> afterRollbackSchema = await GetCurrentSchema(tempDbConnectionString, tempDbName);
            if (afterRollbackSchema.TryGetValue("testtable", out SqlTable? afterRollbackTable) && 
                step1State.Schema.TryGetValue("testtable", out SqlTable? step1TableAfterRollback))
            {
                VerifyColumnOrder(afterRollbackTable, step1TableAfterRollback, "Step 5: After rolling back scenario 1 migration");
            }

            // Step 6: Scenario 2 - Modify the only column while also dropping other columns
            // This tests the case where we modify a column that would become the only column,
            // but we also drop other columns, reducing the table to 0 columns
            await Step6_ModifyOnlyColumnWhileDroppingOthers(tempDbConnectionString, tempDbName);
            MigrationTestState step6State = await GetCurrentState(tempDbConnectionString, tempDbName, tempProjectPath, "Step6", null);
            stateHistory["Step6"] = step6State;

            // Step 7: Generate migration for scenario 2
            MigrationTestState step7State = await Step7_GenerateMigrationForScenario2(tempProjectPath, tempDbConnectionString, tempDbName);
            stateHistory["Step7"] = step7State;

            // Record migration as applied
            Assert.That(step7State.MigrationName, Is.Not.Null, "Step 7: Migration name is null");
            await RecordMigrationApplied(tempDbConnectionString, tempDbName, step7State.MigrationName!);

            // Step 8: Apply migration to fresh database and verify
            string freshDb2Name = $"MinfoldTest_ZeroColumn_Fresh2_{Guid.NewGuid():N}";
            string freshDb2ConnectionString = await CreateTempDatabase(connSettings.Connection, freshDb2Name);
            try
            {
                await Step8_ApplyScenario2MigrationAndVerify(tempProjectPath, freshDb2ConnectionString, freshDb2Name, step6State.Schema);
                
                // Verify column order is preserved after applying migration to fresh database
                ConcurrentDictionary<string, SqlTable> freshDb2Schema = await GetCurrentSchema(freshDb2ConnectionString, freshDb2Name);
                if (freshDb2Schema.TryGetValue("testtable", out SqlTable? freshDb2Table) && 
                    step6State.Schema.TryGetValue("testtable", out SqlTable? step6Table))
                {
                    VerifyColumnOrder(freshDb2Table, step6Table, "Step 8: Fresh database after applying scenario 2 migration");
                }
            }
            finally
            {
                await DropTempDatabase(freshDb2ConnectionString, freshDb2Name);
            }

            // Step 9: Rollback scenario 2 migration and verify schema restored
            await Step9_RollbackScenario2Migration(tempProjectPath, tempDbConnectionString, tempDbName, step1State.Schema, step7State.MigrationName!);
            
            // Verify column order is preserved after final rollback
            ConcurrentDictionary<string, SqlTable> finalSchema = await GetCurrentSchema(tempDbConnectionString, tempDbName);
            if (finalSchema.TryGetValue("testtable", out SqlTable? finalTable) && 
                step1State.Schema.TryGetValue("testtable", out SqlTable? step1FinalTable))
            {
                VerifyColumnOrder(finalTable, step1FinalTable, "Step 9: After rolling back scenario 2 migration (final state)");
            }
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
            CREATE TABLE [dbo].[TestTable] (
                [id] INT PRIMARY KEY IDENTITY(1,1),
                [col1] NVARCHAR(100) NOT NULL,
                [col2] INT NOT NULL,
                [col3] DATETIME2(7) NULL
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

    /// <summary>
    /// Gets the column order (by name) from a table schema, ordered by OrdinalPosition.
    /// </summary>
    private List<string> GetColumnOrder(SqlTable table)
    {
        return table.Columns.Values
            .OrderBy(c => c.OrdinalPosition)
            .Select(c => c.Name)
            .ToList();
    }

    /// <summary>
    /// Verifies that column order matches between two tables.
    /// </summary>
    private void VerifyColumnOrder(SqlTable actualTable, SqlTable expectedTable, string context)
    {
        List<string> actualOrder = GetColumnOrder(actualTable);
        List<string> expectedOrder = GetColumnOrder(expectedTable);
        
        Assert.That(actualOrder, Is.EqualTo(expectedOrder), 
            $"{context}: Column order mismatch. Expected: [{string.Join(", ", expectedOrder)}], Actual: [{string.Join(", ", actualOrder)}]");
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

    /// <summary>
    /// Scenario 1: Remove multiple columns in succession, reducing table to a single column.
    /// Then modify that single column (requiring DROP+ADD), which should trigger the safe wrapper logic.
    /// This tests the case where we drop multiple columns, leaving only one, and then modify that one column.
    /// </summary>
    private async Task Step2_RemoveMultipleColumnsInSuccession(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        
        // Drop PK constraint first
        ResultOrException<int> result1 = await sqlService.Execute("""
            ALTER TABLE [dbo].[TestTable] DROP CONSTRAINT [PK__TestTable__3213E83F]
            """);
        if (result1.Exception is not null)
        {
            // Try alternative PK name format
            await DropPrimaryKeyConstraint(sqlService, "TestTable");
        }

        // Drop columns one by one until only id remains
        // First drop col3
        ResultOrException<int> result2 = await sqlService.Execute("""
            ALTER TABLE [dbo].[TestTable] DROP COLUMN [col3]
            """);
        if (result2.Exception is not null)
        {
            throw new Exception($"Step 2 failed (drop col3): {result2.Exception.Message}", result2.Exception);
        }

        // Then drop col2
        ResultOrException<int> result3 = await sqlService.Execute("""
            ALTER TABLE [dbo].[TestTable] DROP COLUMN [col2]
            """);
        if (result3.Exception is not null)
        {
            throw new Exception($"Step 2 failed (drop col2): {result3.Exception.Message}", result3.Exception);
        }

        // Then drop col1 (leaving only id)
        ResultOrException<int> result4 = await sqlService.Execute("""
            ALTER TABLE [dbo].[TestTable] DROP COLUMN [col1]
            """);
        if (result4.Exception is not null)
        {
            throw new Exception($"Step 2 failed (drop col1): {result4.Exception.Message}", result4.Exception);
        }

        // Now modify id column (remove identity) - this requires DROP+ADD
        // Since id is now the only column, we need to use the safe wrapper approach:
        // Add new column with temporary name, drop old column, rename new column
        string tempColumnName = $"id_tmp_{Guid.NewGuid():N}";
        string addTempColumnSql = GenerateColumns.GenerateAddColumnStatement(
            new SqlTableColumn(
                Name: tempColumnName,
                OrdinalPosition: 0,
                IsNullable: false,
                IsIdentity: false,  // Remove identity
                SqlType: SqlDbTypeExt.Int,
                ForeignKeys: [],
                IsComputed: false,
                IsPrimaryKey: false,
                ComputedSql: null,
                LengthOrPrecision: null
            ), 
            "TestTable");
        string dropOldColumnSql = GenerateColumns.GenerateDropColumnStatement("id", "TestTable");
        string renameColumnSql = GenerateColumns.GenerateRenameColumnStatement("TestTable", tempColumnName, "id");
        
        ResultOrException<int> result5 = await sqlService.Execute($"""
            {addTempColumnSql}
            {dropOldColumnSql}
            {renameColumnSql}
            """);
        if (result5.Exception is not null)
        {
            throw new Exception($"Step 2 failed (modify id): {result5.Exception.Message}", result5.Exception);
        }
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

    private async Task<MigrationTestState> Step3_GenerateMigrationForScenario1(string projectPath, string connectionString, string dbName)
    {
        ResultOrException<MigrationGenerationResult> result = await MigrationGenerator.GenerateIncrementalMigration(
            connectionString, dbName, projectPath, "RemoveAllColumns");

        Assert.That(result.Exception, Is.Null, $"Step 3 failed: {result.Exception?.Message}");
        Assert.That(result.Result, Is.Not.Null, "Step 3: Migration generation returned null result");
        Assert.That(File.Exists(result.Result!.UpScriptPath), Is.True, "Step 3: Up script file not created");
        Assert.That(File.Exists(result.Result.DownScriptPath), Is.True, "Step 3: Down script file not created");

        ConcurrentDictionary<string, SqlTable> schema = await GetCurrentSchema(connectionString, dbName);
        ResultOrException<List<string>> appliedMigrationsResult = await MigrationApplier.GetAppliedMigrations(connectionString, dbName);
        List<string> appliedMigrations = appliedMigrationsResult.Result ?? new List<string>();

        return new MigrationTestState("Step3", schema, appliedMigrations, result.Result.MigrationName);
    }

    private async Task Step4_ApplyScenario1MigrationAndVerify(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema)
    {
        ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(connectionString, dbName, projectPath, false);

        Assert.That(applyResult.Exception, Is.Null, $"Step 4 failed: {applyResult.Exception?.Message}");
        Assert.That(applyResult.Result, Is.Not.Null, "Step 4: Apply migrations returned null result");
        Assert.That(applyResult.Result!.AppliedMigrations.Count, Is.GreaterThanOrEqualTo(1), "Step 4: Expected at least one migration to be applied");

        await VerifySchemaMatches(connectionString, dbName, expectedSchema, new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>());
    }

    private async Task Step5_RollbackScenario1Migration(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema, string migrationName)
    {
        ResultOrException<MigrationRollbackResult> rollbackResult = await MigrationApplier.RollbackMigration(connectionString, dbName, projectPath, migrationName, false);

        Assert.That(rollbackResult.Exception, Is.Null, $"Step 5 failed: {rollbackResult.Exception?.Message}");
        Assert.That(rollbackResult.Result, Is.Not.Null, "Step 5: Rollback migration returned null result");

        // Delete the rolled-back migration folder
        string migrationsPath = MigrationUtilities.GetMigrationsPath(projectPath);
        string migrationFolder = Path.Combine(migrationsPath, migrationName);
        
        if (Directory.Exists(migrationFolder))
        {
            Directory.Delete(migrationFolder, true);
        }

        await VerifySchemaMatches(connectionString, dbName, expectedSchema, new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>());
    }

    /// <summary>
    /// Scenario 2: Drop multiple columns in a single migration that would reduce table to a single column,
    /// then modify that single column in the same migration.
    /// This tests the case where the migration generator needs to handle multiple column drops
    /// that would leave only one column, and then modify that column (requiring safe wrapper).
    /// </summary>
    private async Task Step6_ModifyOnlyColumnWhileDroppingOthers(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        
        // First, restore the table to have multiple columns again
        // (in case it was modified in scenario 1)
        ResultOrException<int> checkResult = await sqlService.Execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TestTable')
            BEGIN
                CREATE TABLE [dbo].[TestTable] (
                    [id] INT PRIMARY KEY IDENTITY(1,1),
                    [col1] NVARCHAR(100) NOT NULL,
                    [col2] INT NOT NULL,
                    [col3] DATETIME2(7) NULL
                )
            END
            ELSE
            BEGIN
                -- Table exists, ensure it has the columns we need
                IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('[dbo].[TestTable]') AND name = 'col1')
                BEGIN
                    ALTER TABLE [dbo].[TestTable] ADD [col1] NVARCHAR(100) NOT NULL DEFAULT ''
                END
                IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('[dbo].[TestTable]') AND name = 'col2')
                BEGIN
                    ALTER TABLE [dbo].[TestTable] ADD [col2] INT NOT NULL DEFAULT 0
                END
                IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('[dbo].[TestTable]') AND name = 'col3')
                BEGIN
                    ALTER TABLE [dbo].[TestTable] ADD [col3] DATETIME2(7) NULL
                END
                -- Ensure id has identity
                IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('[dbo].[TestTable]') AND name = 'id' AND is_identity = 0)
                BEGIN
                    -- Drop and recreate with identity
                    ALTER TABLE [dbo].[TestTable] DROP COLUMN [id]
                    ALTER TABLE [dbo].[TestTable] ADD [id] INT NOT NULL IDENTITY(1,1)
                END
            END
            """);
        if (checkResult.Exception is not null)
        {
            throw new Exception($"Step 6 failed (restore table): {checkResult.Exception.Message}", checkResult.Exception);
        }

        // Drop PK constraint
        await DropPrimaryKeyConstraint(sqlService, "TestTable");

        // Drop col3 first
        ResultOrException<int> result1 = await sqlService.Execute("""
            ALTER TABLE [dbo].[TestTable] DROP COLUMN [col3]
            """);
        if (result1.Exception is not null)
        {
            throw new Exception($"Step 6 failed (drop col3): {result1.Exception.Message}", result1.Exception);
        }

        // Drop col2 (leaving id and col1)
        ResultOrException<int> result2 = await sqlService.Execute("""
            ALTER TABLE [dbo].[TestTable] DROP COLUMN [col2]
            """);
        if (result2.Exception is not null)
        {
            throw new Exception($"Step 6 failed (drop col2): {result2.Exception.Message}", result2.Exception);
        }

        // Drop col1 (leaving only id)
        ResultOrException<int> result3 = await sqlService.Execute("""
            ALTER TABLE [dbo].[TestTable] DROP COLUMN [col1]
            """);
        if (result3.Exception is not null)
        {
            throw new Exception($"Step 6 failed (drop col1): {result3.Exception.Message}", result3.Exception);
        }

        // Now modify id column (remove identity) - this requires DROP+ADD
        // Since id is now the only column, we need to use the safe wrapper approach:
        // Add new column with temporary name, drop old column, rename new column
        string tempColumnName = $"id_tmp_{Guid.NewGuid():N}";
        string addTempColumnSql = GenerateColumns.GenerateAddColumnStatement(
            new SqlTableColumn(
                Name: tempColumnName,
                OrdinalPosition: 0,
                IsNullable: false,
                IsIdentity: false,  // Remove identity
                SqlType: SqlDbTypeExt.Int,
                ForeignKeys: [],
                IsComputed: false,
                IsPrimaryKey: false,
                ComputedSql: null,
                LengthOrPrecision: null
            ), 
            "TestTable");
        string dropOldColumnSql = GenerateColumns.GenerateDropColumnStatement("id", "TestTable");
        string renameColumnSql = GenerateColumns.GenerateRenameColumnStatement("TestTable", tempColumnName, "id");
        
        ResultOrException<int> result4 = await sqlService.Execute($"""
            {addTempColumnSql}
            {dropOldColumnSql}
            {renameColumnSql}
            """);
        if (result4.Exception is not null)
        {
            throw new Exception($"Step 6 failed (modify id): {result4.Exception.Message}", result4.Exception);
        }
    }

    private async Task<MigrationTestState> Step7_GenerateMigrationForScenario2(string projectPath, string connectionString, string dbName)
    {
        ResultOrException<MigrationGenerationResult> result = await MigrationGenerator.GenerateIncrementalMigration(
            connectionString, dbName, projectPath, "ModifyOnlyColumnAndDropAll");

        Assert.That(result.Exception, Is.Null, $"Step 7 failed: {result.Exception?.Message}");
        Assert.That(result.Result, Is.Not.Null, "Step 7: Migration generation returned null result");
        Assert.That(File.Exists(result.Result!.UpScriptPath), Is.True, "Step 7: Up script file not created");
        Assert.That(File.Exists(result.Result.DownScriptPath), Is.True, "Step 7: Down script file not created");

        ConcurrentDictionary<string, SqlTable> schema = await GetCurrentSchema(connectionString, dbName);
        ResultOrException<List<string>> appliedMigrationsResult = await MigrationApplier.GetAppliedMigrations(connectionString, dbName);
        List<string> appliedMigrations = appliedMigrationsResult.Result ?? new List<string>();

        return new MigrationTestState("Step7", schema, appliedMigrations, result.Result.MigrationName);
    }

    private async Task Step8_ApplyScenario2MigrationAndVerify(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema)
    {
        ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(connectionString, dbName, projectPath, false);

        Assert.That(applyResult.Exception, Is.Null, $"Step 8 failed: {applyResult.Exception?.Message}");
        Assert.That(applyResult.Result, Is.Not.Null, "Step 8: Apply migrations returned null result");
        Assert.That(applyResult.Result!.AppliedMigrations.Count, Is.GreaterThanOrEqualTo(1), "Step 8: Expected at least one migration to be applied");

        await VerifySchemaMatches(connectionString, dbName, expectedSchema, new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>());
    }

    private async Task Step9_RollbackScenario2Migration(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema, string migrationName)
    {
        ResultOrException<MigrationRollbackResult> rollbackResult = await MigrationApplier.RollbackMigration(connectionString, dbName, projectPath, migrationName, false);

        Assert.That(rollbackResult.Exception, Is.Null, $"Step 9 failed: {rollbackResult.Exception?.Message}");
        Assert.That(rollbackResult.Result, Is.Not.Null, "Step 9: Rollback migration returned null result");

        // Delete the rolled-back migration folder
        string migrationsPath = MigrationUtilities.GetMigrationsPath(projectPath);
        string migrationFolder = Path.Combine(migrationsPath, migrationName);
        
        if (Directory.Exists(migrationFolder))
        {
            Directory.Delete(migrationFolder, true);
        }

        await VerifySchemaMatches(connectionString, dbName, expectedSchema, new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>());
    }

    [Test]
    public async Task TestSingleColumnModifyWithAddColumn()
    {
        MigrationLogger.SetLogger(Console.WriteLine);
        
        string tempDbName = $"MinfoldTest_SingleColumnModify_{Guid.NewGuid():N}";
        string tempProjectPath = Path.Combine(Path.GetTempPath(), $"MinfoldTestSingleColumnModifyProject_{Guid.NewGuid():N}");
        string? tempDbConnectionString = null;
        Dictionary<string, MigrationTestState> stateHistory = new Dictionary<string, MigrationTestState>();

        try
        {
            // Setup: Create temporary database and project
            tempDbConnectionString = await CreateTempDatabase(connSettings.Connection, tempDbName);
            await CreateTempProject(tempProjectPath);
            
            // Step 1: Create table with id INT NOT NULL (only column)
            await Step1_CreateSingleColumnTable(tempDbConnectionString, tempDbName);
            
            // Step 2: Generate initial migration
            MigrationTestState step2State = await Step2_GenerateInitialMigration(tempProjectPath, tempDbConnectionString, tempDbName);
            stateHistory["Step2"] = step2State;
            
            // Verify column order after initial migration (should be [id])
            if (step2State.Schema.TryGetValue("sampletable", out SqlTable? step2Table))
            {
                List<string> step2ColumnOrder = GetColumnOrder(step2Table);
                Assert.That(step2ColumnOrder, Is.EqualTo(new List<string> { "id" }), 
                    "Step 2: Initial migration column order should be [id]");
            }
            
            // Record initial migration as applied (database-first approach)
            Assert.That(step2State.MigrationName, Is.Not.Null, "Step 2: Initial migration name is null");
            await RecordMigrationApplied(tempDbConnectionString, tempDbName, step2State.MigrationName!);
            
            // Step 3: Modify id to add PK and IDENTITY(1,1), and add text NVARCHAR(MAX) NULL
            await Step3_ModifyIdAndAddTextColumn(tempDbConnectionString, tempDbName);
            MigrationTestState step3State = await GetCurrentState(tempDbConnectionString, tempDbName, tempProjectPath, "Step3", null);
            stateHistory["Step3"] = step3State;
            
            // Verify column order after manual modification and reordering
            // After adding text and modifying id, we manually reordered to [id, text]
            // The migration generator should preserve this correct order
            if (step3State.Schema.TryGetValue("sampletable", out SqlTable? step3Table))
            {
                List<string> step3ColumnOrder = GetColumnOrder(step3Table);
                Assert.That(step3ColumnOrder, Is.EqualTo(new List<string> { "id", "text" }), 
                    "Step 3: After manual modification and reordering, column order should be [id, text]");
            }
            
            // Step 4: Generate incremental migration
            MigrationTestState step4State = await Step4_GenerateIncrementalMigration(tempProjectPath, tempDbConnectionString, tempDbName);
            stateHistory["Step4"] = step4State;
            
            // Record migration as applied
            Assert.That(step4State.MigrationName, Is.Not.Null, "Step 4: Migration name is null");
            await RecordMigrationApplied(tempDbConnectionString, tempDbName, step4State.MigrationName!);
            
            // Step 5: Apply migration to fresh database and verify
            string freshDbName = $"MinfoldTest_SingleColumnModify_Fresh_{Guid.NewGuid():N}";
            string freshDbConnectionString = await CreateTempDatabase(connSettings.Connection, freshDbName);
            try
            {
                await Step5_ApplyMigrationAndVerify(tempProjectPath, freshDbConnectionString, freshDbName, step3State.Schema);
                
                // Verify column order is preserved after applying migration to fresh database
                ConcurrentDictionary<string, SqlTable> freshDbSchema = await GetCurrentSchema(freshDbConnectionString, freshDbName);
                if (freshDbSchema.TryGetValue("sampletable", out SqlTable? freshDbTable) && 
                    step3State.Schema.TryGetValue("sampletable", out SqlTable? step3TableForFresh))
                {
                    VerifyColumnOrder(freshDbTable, step3TableForFresh, "Step 5: Fresh database after applying migrations");
                }
            }
            finally
            {
                await DropTempDatabase(freshDbConnectionString, freshDbName);
            }
            
            // Step 6: Rollback to initial migration and verify schema
            // We need to rollback the incremental migration (not the initial one) to get back to initial state
            Assert.That(step2State.MigrationName, Is.Not.Null, "Step 6: Initial migration name is null");
            Assert.That(step4State.MigrationName, Is.Not.Null, "Step 6: Incremental migration name is null");
            await Step6_RollbackToInitial(tempProjectPath, tempDbConnectionString, tempDbName, step2State.Schema, step4State.MigrationName!);
            
            // Verify column order is preserved after rollback (should be [id])
            ConcurrentDictionary<string, SqlTable> afterRollbackSchema = await GetCurrentSchema(tempDbConnectionString, tempDbName);
            if (afterRollbackSchema.TryGetValue("sampletable", out SqlTable? afterRollbackTable) && 
                step2State.Schema.TryGetValue("sampletable", out SqlTable? step2TableAfterRollback))
            {
                VerifyColumnOrder(afterRollbackTable, step2TableAfterRollback, "Step 6: After rolling back incremental migration");
            }
            
            // Step 7: Reapply migration and verify final schema
            await Step7_ReapplyAndVerify(tempProjectPath, tempDbConnectionString, tempDbName, step3State.Schema);
            
            // Verify column order is preserved after reapply (should be [id, text])
            ConcurrentDictionary<string, SqlTable> finalSchema = await GetCurrentSchema(tempDbConnectionString, tempDbName);
            if (finalSchema.TryGetValue("sampletable", out SqlTable? finalTable) && 
                step3State.Schema.TryGetValue("sampletable", out SqlTable? step3FinalTable))
            {
                VerifyColumnOrder(finalTable, step3FinalTable, "Step 7: After reapplying incremental migration (final state)");
            }
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

    private async Task Step1_CreateSingleColumnTable(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        ResultOrException<int> result = await sqlService.Execute("""
            CREATE TABLE [dbo].[SampleTable] (
                [id] INT NOT NULL
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
            connectionString, dbName, projectPath, "InitialSchema");

        Assert.That(result.Exception, Is.Null, $"Step 2 failed: {result.Exception?.Message}");
        Assert.That(result.Result, Is.Not.Null, "Step 2: Migration generation returned null result");
        Assert.That(File.Exists(result.Result!.UpScriptPath), Is.True, "Step 2: Up script file not created");
        Assert.That(File.Exists(result.Result.DownScriptPath), Is.True, "Step 2: Down script file not created");

        ConcurrentDictionary<string, SqlTable> schema = await GetCurrentSchema(connectionString, dbName);
        return new MigrationTestState("Step2", schema, new List<string>(), result.Result.MigrationName);
    }

    private async Task Step3_ModifyIdAndAddTextColumn(string connectionString, string dbName)
    {
        SqlService sqlService = new SqlService(connectionString);
        
        // This step simulates the scenario where we want to:
        // 1. Add text column (SQL Server adds it at the end: [id, text])
        // 2. Modify id to add PK and IDENTITY(1,1) (drop/add puts it at the end: [text, id])
        // 3. Manually reorder columns to correct order: [id, text]
        // The migration generator should preserve this correct order
        
        // Add text column first (manually, to simulate the target state)
        // SQL Server adds new columns at the end, so order becomes [id, text]
        ResultOrException<int> result1 = await sqlService.Execute("""
            ALTER TABLE [dbo].[SampleTable] ADD [text] NVARCHAR(MAX) NULL
            """);
        if (result1.Exception is not null)
        {
            throw new Exception($"Step 3 failed (add text column): {result1.Exception.Message}", result1.Exception);
        }
        
        // Now modify id column to add PK and IDENTITY(1,1)
        // Use safe wrapper approach since we now have 2 columns
        // This will drop and add id, which puts it at the end: [text, id]
        string tempColumnName = $"id_tmp_{Guid.NewGuid():N}";
        string addTempColumnSql = GenerateColumns.GenerateAddColumnStatement(
            new SqlTableColumn(
                Name: tempColumnName,
                OrdinalPosition: 0,
                IsNullable: false,
                IsIdentity: true,
                SqlType: SqlDbTypeExt.Int,
                ForeignKeys: [],
                IsComputed: false,
                IsPrimaryKey: true,
                ComputedSql: null,
                LengthOrPrecision: null,
                IdentitySeed: 1,
                IdentityIncrement: 1
            ), 
            "SampleTable");
        string dropOldColumnSql = GenerateColumns.GenerateDropColumnStatement("id", "SampleTable");
        string renameColumnSql = GenerateColumns.GenerateRenameColumnStatement("SampleTable", tempColumnName, "id");
        
        ResultOrException<int> result2 = await sqlService.Execute($"""
            {addTempColumnSql}
            {dropOldColumnSql}
            {renameColumnSql}
            """);
        if (result2.Exception is not null)
        {
            throw new Exception($"Step 3 failed (modify id): {result2.Exception.Message}", result2.Exception);
        }
        
        // Add PK constraint
        ResultOrException<int> result3 = await sqlService.Execute("""
            ALTER TABLE [dbo].[SampleTable] ADD CONSTRAINT [PK_SampleTable] PRIMARY KEY ([id])
            """);
        if (result3.Exception is not null)
        {
            throw new Exception($"Step 3 failed (add PK): {result3.Exception.Message}", result3.Exception);
        }
        
        // Now manually reorder columns to correct order: [id, text]
        // Get current schema (has [text, id] order)
        ConcurrentDictionary<string, SqlTable> currentSchema = await GetCurrentSchema(connectionString, dbName);
        if (!currentSchema.TryGetValue("sampletable", out SqlTable? actualTable))
        {
            throw new Exception("Step 3 failed: Could not find SampleTable in current schema");
        }
        
        // Build desired table with correct column order: [id, text]
        Dictionary<string, SqlTableColumn> desiredColumns = new Dictionary<string, SqlTableColumn>(StringComparer.OrdinalIgnoreCase);
        
        // Get id column (should be first)
        if (actualTable.Columns.TryGetValue("id", out SqlTableColumn? idCol))
        {
            desiredColumns["id"] = idCol with { OrdinalPosition = 1 };
        }
        
        // Get text column (should be second)
        if (actualTable.Columns.TryGetValue("text", out SqlTableColumn? textCol))
        {
            desiredColumns["text"] = textCol with { OrdinalPosition = 2 };
        }
        
        SqlTable desiredTable = new SqlTable(actualTable.Name, desiredColumns, actualTable.Indexes, actualTable.Schema);
        
        // Generate reorder SQL
        (string reorderSql, List<string> constraintSql) = GenerateTables.GenerateColumnReorderStatement(
            actualTable,
            desiredTable,
            new ConcurrentDictionary<string, SqlTable>(currentSchema, StringComparer.OrdinalIgnoreCase));
        
        if (!string.IsNullOrEmpty(reorderSql))
        {
            // Execute reorder SQL
            StringBuilder reorderScript = new StringBuilder();
            reorderScript.AppendLine(reorderSql);
            
            // Add constraint recreation SQL
            foreach (string constraint in constraintSql)
            {
                reorderScript.AppendLine(constraint);
            }
            
            ResultOrException<int> result4 = await sqlService.Execute(reorderScript.ToString());
            if (result4.Exception is not null)
            {
                throw new Exception($"Step 3 failed (reorder columns): {result4.Exception.Message}", result4.Exception);
            }
        }
    }

    private async Task<MigrationTestState> Step4_GenerateIncrementalMigration(string projectPath, string connectionString, string dbName)
    {
        ResultOrException<MigrationGenerationResult> result = await MigrationGenerator.GenerateIncrementalMigration(
            connectionString, dbName, projectPath, "AddIdentityAndTextColumn");

        Assert.That(result.Exception, Is.Null, $"Step 4 failed: {result.Exception?.Message}");
        Assert.That(result.Result, Is.Not.Null, "Step 4: Migration generation returned null result");
        Assert.That(File.Exists(result.Result!.UpScriptPath), Is.True, "Step 4: Up script file not created");
        Assert.That(File.Exists(result.Result.DownScriptPath), Is.True, "Step 4: Down script file not created");

        ConcurrentDictionary<string, SqlTable> schema = await GetCurrentSchema(connectionString, dbName);
        ResultOrException<List<string>> appliedMigrationsResult = await MigrationApplier.GetAppliedMigrations(connectionString, dbName);
        List<string> appliedMigrations = appliedMigrationsResult.Result ?? new List<string>();

        return new MigrationTestState("Step4", schema, appliedMigrations, result.Result.MigrationName);
    }

    private async Task Step5_ApplyMigrationAndVerify(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema)
    {
        // Verify that migration files exist before applying
        List<MigrationInfo> allMigrations = MigrationApplier.GetMigrationFiles(projectPath);
        Assert.That(allMigrations.Count, Is.GreaterThanOrEqualTo(2), $"Step 5: Expected at least 2 migrations (initial + incremental), but found {allMigrations.Count}");
        
        ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(connectionString, dbName, projectPath, false);

        Assert.That(applyResult.Exception, Is.Null, $"Step 5 failed: {applyResult.Exception?.Message}");
        Assert.That(applyResult.Result, Is.Not.Null, "Step 5: Apply migrations returned null result");
        Assert.That(applyResult.Result!.AppliedMigrations.Count, Is.GreaterThanOrEqualTo(2), $"Step 5: Expected at least 2 migrations to be applied (initial + incremental), but only {applyResult.Result.AppliedMigrations.Count} were applied: {string.Join(", ", applyResult.Result.AppliedMigrations)}");

        await VerifySchemaMatches(connectionString, dbName, expectedSchema, new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>());
    }

    private async Task Step6_RollbackToInitial(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema, string incrementalMigrationName)
    {
        // Rollback the incremental migration to restore the initial state
        ResultOrException<MigrationRollbackResult> rollbackResult = await MigrationApplier.RollbackMigration(connectionString, dbName, projectPath, incrementalMigrationName, false);

        Assert.That(rollbackResult.Exception, Is.Null, $"Step 6 failed: {rollbackResult.Exception?.Message}");
        Assert.That(rollbackResult.Result, Is.Not.Null, "Step 6: Rollback migration returned null result");

        await VerifySchemaMatches(connectionString, dbName, expectedSchema, new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>());
    }

    private async Task Step7_ReapplyAndVerify(string projectPath, string connectionString, string dbName, ConcurrentDictionary<string, SqlTable> expectedSchema)
    {
        ResultOrException<MigrationApplyResult> applyResult = await MigrationApplier.ApplyMigrations(connectionString, dbName, projectPath, false);

        Assert.That(applyResult.Exception, Is.Null, $"Step 7 failed: {applyResult.Exception?.Message}");
        Assert.That(applyResult.Result, Is.Not.Null, "Step 7: Apply migrations returned null result");

        await VerifySchemaMatches(connectionString, dbName, expectedSchema, new ConcurrentDictionary<string, SqlSequence>(), new ConcurrentDictionary<string, SqlStoredProcedure>());
    }
}

