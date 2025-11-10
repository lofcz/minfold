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

    /// <summary>
    /// Gets the next migration timestamp, ensuring uniqueness by checking if a migration folder
    /// with that timestamp already exists. If it does, increments the timestamp until a free slot is found.
    /// </summary>
    public static string GetNextMigrationTimestamp(string codePath)
    {
        string migrationsPath = GetMigrationsPath(codePath);
        string baseTimestamp = DateTime.UtcNow.ToString("yyyyMMddHHmmss");
        string candidateTimestamp = baseTimestamp;
        int increment = 0;

        // Ensure the migrations directory exists
        Directory.CreateDirectory(migrationsPath);

        // Check if a migration folder with this timestamp already exists
        // If it does, increment the timestamp until we find a free slot
        while (true)
        {
            // Check if any migration folder starts with this timestamp
            string[] existingFolders = Directory.GetDirectories(migrationsPath);
            bool timestampExists = existingFolders.Any(folder =>
            {
                string folderName = Path.GetFileName(folder);
                // Check if folder name starts with the candidate timestamp
                return folderName.StartsWith(candidateTimestamp, StringComparison.Ordinal);
            });

            if (!timestampExists)
            {
                // Found a free slot
                return candidateTimestamp;
            }

            // Timestamp collision - increment by 1 second
            increment++;
            DateTime baseTime = DateTime.ParseExact(baseTimestamp, "yyyyMMddHHmmss", null, System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal);
            DateTime incrementedTime = baseTime.AddSeconds(increment);
            candidateTimestamp = incrementedTime.ToString("yyyyMMddHHmmss");

            // Safety check: if we've incremented too much (more than 60 seconds), something is wrong
            if (increment > 60)
            {
                throw new InvalidOperationException(
                    $"Unable to find a unique migration timestamp after {increment} attempts. " +
                    $"There may be too many migrations being created simultaneously.");
            }
        }
    }

    /// <summary>
    /// Gets the next migration timestamp without checking for uniqueness.
    /// Use this only when you don't have access to codePath or when uniqueness is guaranteed elsewhere.
    /// </summary>
    public static string GetNextMigrationTimestampUnsafe()
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

public static class MigrationLogger
{
    private static Action<string>? _logCallback;

    public static void SetLogger(Action<string>? callback)
    {
        _logCallback = callback;
    }

    public static void Log(string message)
    {
        _logCallback?.Invoke(message);
    }
}

