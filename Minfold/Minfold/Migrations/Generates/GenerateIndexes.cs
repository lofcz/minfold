namespace Minfold;

public static class GenerateIndexes
{
    public static string GenerateDropIndexStatement(SqlIndex index)
    {
        // Use dynamic SQL to check if index exists before dropping
        return $"""
            IF EXISTS (SELECT * FROM sys.indexes WHERE name = '{index.Name}' AND object_id = OBJECT_ID('[{index.Schema}].[{index.Table}]'))
            BEGIN
                DROP INDEX [{index.Name}] ON [{index.Schema}].[{index.Table}];
            END
            """;
    }

    public static string GenerateCreateIndexStatement(SqlIndex index)
    {
        System.Text.StringBuilder sb = new System.Text.StringBuilder();
        // Use dynamic SQL to check if index doesn't exist before creating
        string keyColumns = string.Join(", ", index.Columns.Select(c => $"[{c}]"));
        string whereClause = string.IsNullOrWhiteSpace(index.FilterPredicate)
            ? string.Empty
            : $" WHERE {index.FilterPredicate}";
        sb.AppendLine($"""
            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '{index.Name}' AND object_id = OBJECT_ID('[{index.Schema}].[{index.Table}]'))
            BEGIN
                CREATE {(index.IsUnique ? "UNIQUE " : "")}NONCLUSTERED INDEX [{index.Name}] ON [{index.Schema}].[{index.Table}] ({keyColumns}){whereClause};
            END
            """);
        return sb.ToString();
    }
}

