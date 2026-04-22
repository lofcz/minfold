using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using Minfold;
using Newtonsoft.Json;

namespace MinFold.Test;

/// <summary>
/// Filtered indexes (e.g. UNIQUE WHERE col IS NOT NULL): schema load and generated CREATE INDEX must preserve the predicate.
/// </summary>
[TestFixture]
public class FilteredIndexTests
{
    private static SqlSettings connSettings = null!;

    public record SqlSettings(string Connection, string Database);

    [SetUp]
    public async Task Setup()
    {
        if (!File.Exists("conn.txt"))
        {
            throw new Exception("Please make a copy of conn_proto.txt and replace it's content with the connection string");
        }

        connSettings = JsonConvert.DeserializeObject<SqlSettings>(await File.ReadAllTextAsync("conn.txt"))
            ?? throw new Exception("Failed to deserialize content of conn.txt as valid JSON");
    }

    [Test]
    public async Task GetSchema_LoadsFilterPredicate_ForFilteredUniqueIndex()
    {
        string tempDbName = $"MinfoldTest_FilteredIdx_{Guid.NewGuid():N}";
        string? tempCs = null;
        try
        {
            tempCs = await CreateTempDatabase(connSettings.Connection, tempDbName);
            SqlService sql = new SqlService(tempCs);
            ResultOrException<int> ddl = await sql.Execute("""
                CREATE TABLE [dbo].[FilteredIndexTestTable] (
                    [id] INT NOT NULL CONSTRAINT [PK_FilteredIndexTestTable] PRIMARY KEY,
                    [code] NVARCHAR(50) NULL
                );
                CREATE UNIQUE NONCLUSTERED INDEX [IX_FilteredIndexTestTable_code]
                    ON [dbo].[FilteredIndexTestTable]([code])
                    WHERE ([code] IS NOT NULL);
                """);

            Assert.That(ddl.Exception, Is.Null, ddl.Exception?.Message);

            ResultOrException<ConcurrentDictionary<string, SqlTable>> schemaResult =
                await sql.GetSchema(tempDbName, null, ["__MinfoldMigrations"]);

            Assert.That(schemaResult.Exception, Is.Null, schemaResult.Exception?.Message);
            Assert.That(schemaResult.Result, Is.Not.Null);
            Assert.That(schemaResult.Result!.TryGetValue("filteredindextesttable", out SqlTable? table), Is.True);

            SqlIndex? ix = table!.Indexes.Find(i => i.Name.Equals("IX_FilteredIndexTestTable_code", StringComparison.OrdinalIgnoreCase));
            Assert.That(ix, Is.Not.Null, "Expected index IX_FilteredIndexTestTable_code in schema");
            Assert.That(ix!.IsUnique, Is.True);
            Assert.That(ix.FilterPredicate, Is.Not.Null.And.Not.Empty, "Filter predicate must be loaded for filtered index");
            Assert.That(ix.FilterPredicate, Does.Contain("IS NOT NULL").IgnoreCase);
        }
        finally
        {
            if (tempCs is not null)
            {
                await DropTempDatabase(tempCs, tempDbName);
            }
        }
    }

    [Test]
    public async Task GenerateCreateIndexStatement_IncludesWhereClause_WhenFilterPredicateSet()
    {
        string tempDbName = $"MinfoldTest_FilteredIdxGen_{Guid.NewGuid():N}";
        string? tempCs = null;
        try
        {
            tempCs = await CreateTempDatabase(connSettings.Connection, tempDbName);
            SqlService sql = new SqlService(tempCs);
            await sql.Execute("""
                CREATE TABLE [dbo].[FilteredIndexGenTable] (
                    [id] INT NOT NULL CONSTRAINT [PK_FilteredIndexGenTable] PRIMARY KEY,
                    [code] NVARCHAR(50) NULL
                );
                CREATE UNIQUE NONCLUSTERED INDEX [IX_FilteredIndexGenTable_code]
                    ON [dbo].[FilteredIndexGenTable]([code])
                    WHERE ([code] IS NOT NULL);
                """);

            ResultOrException<ConcurrentDictionary<string, SqlTable>> schemaResult =
                await sql.GetSchema(tempDbName, null, ["__MinfoldMigrations"]);

            Assert.That(schemaResult.Result, Is.Not.Null);
            SqlTable table = schemaResult.Result!["filteredindexgentable"];
            SqlIndex ix = table.Indexes.Single(i => i.Name.Equals("IX_FilteredIndexGenTable_code", StringComparison.OrdinalIgnoreCase));

            string stmt = GenerateIndexes.GenerateCreateIndexStatement(ix);
            Assert.That(stmt, Does.Contain("WHERE").IgnoreCase);
            Assert.That(ix.FilterPredicate, Is.Not.Null);
            Assert.That(stmt, Does.Contain(ix.FilterPredicate!));
        }
        finally
        {
            if (tempCs is not null)
            {
                await DropTempDatabase(tempCs, tempDbName);
            }
        }
    }

    private static async Task<string> CreateTempDatabase(string baseConnectionString, string dbName)
    {
        SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(baseConnectionString)
        {
            InitialCatalog = "master"
        };

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

    private static async Task DropTempDatabase(string connectionString, string dbName)
    {
        try
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString)
            {
                InitialCatalog = "master"
            };

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
}
