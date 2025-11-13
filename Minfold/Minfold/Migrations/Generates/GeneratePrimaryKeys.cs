namespace Minfold;

public static class GeneratePrimaryKeys
{
    public static string GenerateDropPrimaryKeyStatement(string tableName, string constraintName, string schema = "dbo", string? variableSuffix = null)
    {
        // Use provided suffix or generate a deterministic one to avoid conflicts
        string varSuffix = variableSuffix ?? MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(schema, tableName, constraintName, "droppk");
        
        // Use dynamic SQL to check if constraint exists before dropping
        return $"""
            DECLARE @pkConstraintName_{varSuffix} NVARCHAR(128);
            SELECT @pkConstraintName_{varSuffix} = name FROM sys.key_constraints 
            WHERE parent_object_id = OBJECT_ID('[{schema}].[{tableName}]') 
            AND type = 'PK'
            AND name = '{constraintName}';
            IF @pkConstraintName_{varSuffix} IS NOT NULL
                EXEC('ALTER TABLE [{schema}].[{tableName}] DROP CONSTRAINT [' + @pkConstraintName_{varSuffix} + ']');
            """;
    }

    public static string GenerateAddPrimaryKeyStatement(string tableName, List<string> columnNames, string constraintName, string schema = "dbo")
    {
        System.Text.StringBuilder sb = new System.Text.StringBuilder();
        sb.Append($"ALTER TABLE [{schema}].[{tableName}] ADD CONSTRAINT [{constraintName}] PRIMARY KEY (");
        sb.Append(string.Join(", ", columnNames.Select(c => $"[{c}]")));
        sb.AppendLine(");");
        return sb.ToString();
    }
}
