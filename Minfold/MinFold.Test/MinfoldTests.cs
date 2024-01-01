using Minfold;
using Minfold.SqlJson;
using Newtonsoft.Json;

namespace MinFold.Test;

public class MinfoldTests
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
        
        ResultOrException<int> schemaResult = await SqlService.Execute("""
           drop table if exists Foo
           drop table if exists Bar
           create table Foo (
               a integer not null,
               b integer,
               c nvarchar(max)
           )
           create table Bar (
               a integer,
               b float,
               c bit,
               d datetime2(7)
           )
        """);

        if (schemaResult.Exception is not null)
        {
            throw schemaResult.Exception;
        }
    }
    
    private static IEnumerable<string> GetTestCases()
    {
        string projectDirectory = Directory.GetParent(Environment.CurrentDirectory)?.Parent?.Parent?.FullName ?? string.Empty;
        
        return Directory.EnumerateFiles($"{projectDirectory}\\Sql", "*.in.txt", SearchOption.AllDirectories);
    }

    [Test, TestCaseSource(nameof(GetTestCases))]
    public async Task TestNestedFrom(string testSrc)
    {
        string outputPath = testSrc.Replace(".in.txt", ".out.txt");
        
        if (!File.Exists(outputPath))
        {
            Assert.Fail("Test case expected output not found");
        }

        string? Normalize(string? str)
        {
            return str?.Trim().Replace("\r\n", "\n");
        }
        
        string input = await File.ReadAllTextAsync(testSrc);
        string output = await File.ReadAllTextAsync(outputPath);
        string expected;
        
        MinfoldSqlResult result = await MinfoldSql.Map(connSettings.Connection, connSettings.Database, input);

        if (testSrc.Contains("ResultAmbiguity"))
        {
            Assert.That(result.ResultType, Is.EqualTo(MinfoldSqlResultTypes.MappingAmbiguities));

            string? errors = Normalize(result.MappingAmbiguities?.DumpConcat(ErrorDumpModes.Serializable));
            expected = Normalize(output) ?? string.Empty;
            
            if (!string.Equals(expected, errors))
            {
                string? readableErrors = result.MappingAmbiguities?.DumpConcat(ErrorDumpModes.HumanReadable);
            }
            
            Assert.That(expected, Is.EqualTo(errors));
            return;
        }

        string? genCode = Normalize(result.GeneratedCode);
        expected = Normalize(output) ?? string.Empty;
        
        if (!string.Equals(expected, genCode))
        {
            // breakpoint placeholder
        }
        
        Assert.That(expected, Is.EqualTo(genCode));
    }
}