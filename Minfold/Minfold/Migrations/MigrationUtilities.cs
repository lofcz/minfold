using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace Minfold;

public static class MigrationUtilities
{
    private static readonly Sql160ScriptGenerator ScriptGenerator = new Sql160ScriptGenerator(new SqlScriptGeneratorOptions
    {
        KeywordCasing = KeywordCasing.Uppercase,
        IncludeSemicolons = true,
        NewLineBeforeFromClause = true,
        NewLineBeforeOrderByClause = true,
        NewLineBeforeWhereClause = true,
        AlignClauseBodies = false
    });

    private static readonly TSql160Parser Parser = new TSql160Parser(true, SqlEngineType.All);

    public static string GetMigrationsPath(string codePath)
    {
        return Path.Combine(codePath, "Dao", "Migrations");
    }

    public static string GetNextMigrationTimestamp()
    {
        return DateTime.UtcNow.ToString("yyyyMMddHHmmss");
    }

    public static string FormatMigrationScript(string script)
    {
        if (string.IsNullOrWhiteSpace(script))
        {
            return script;
        }

        try
        {
            using StringReader rdr = new StringReader(script);
            TSqlFragment tree = Parser.Parse(rdr, out IList<ParseError>? errors);

            if (errors?.Count > 0)
            {
                // If parsing fails, return original script
                return script;
            }

            ScriptGenerator.GenerateScript(tree, out string formattedQuery);
            return formattedQuery;
        }
        catch
        {
            // If formatting fails, return original script
            return script;
        }
    }
}

