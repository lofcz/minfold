namespace Minfold;

public static class GenerateProcedures
{
    public static string GenerateCreateProcedureStatement(SqlStoredProcedure procedure)
    {
        // CREATE PROCEDURE must be the first statement in a batch, so we use DROP IF EXISTS + GO + CREATE pattern
        // This ensures idempotency and proper batching
        return $"""
            GO
            IF EXISTS (SELECT * FROM sys.procedures WHERE name = '{procedure.Name}' AND schema_id = SCHEMA_ID('{procedure.Schema}'))
                DROP PROCEDURE [{procedure.Schema}].[{procedure.Name}];
            GO
            {procedure.Definition}
            GO
            """;
    }

    public static string GenerateDropProcedureStatement(string procedureName, string schema = "dbo")
    {
        // DROP PROCEDURE can be in a batch, but we add GO for consistency and to ensure proper batching
        return $"""
            GO
            IF EXISTS (SELECT * FROM sys.procedures WHERE name = '{procedureName}' AND schema_id = SCHEMA_ID('{schema}'))
                DROP PROCEDURE [{schema}].[{procedureName}];
            GO
            """;
    }
}

