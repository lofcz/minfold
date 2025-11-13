namespace Minfold;

public static class GenerateSequences
{
    public static string GenerateCreateSequenceStatement(SqlSequence sequence)
    {
        System.Text.StringBuilder sb = new System.Text.StringBuilder();
        sb.Append($"CREATE SEQUENCE [{sequence.Schema}].[{sequence.Name}] AS {sequence.DataType}");
        
        if (sequence.StartValue.HasValue)
        {
            sb.Append($" START WITH {sequence.StartValue.Value}");
        }
        
        if (sequence.Increment.HasValue)
        {
            sb.Append($" INCREMENT BY {sequence.Increment.Value}");
        }
        
        if (sequence.MinValue.HasValue)
        {
            sb.Append($" MINVALUE {sequence.MinValue.Value}");
        }
        else
        {
            sb.Append(" NO MINVALUE");
        }
        
        if (sequence.MaxValue.HasValue)
        {
            sb.Append($" MAXVALUE {sequence.MaxValue.Value}");
        }
        else
        {
            sb.Append(" NO MAXVALUE");
        }
        
        if (sequence.Cycle)
        {
            sb.Append(" CYCLE");
        }
        else
        {
            sb.Append(" NO CYCLE");
        }
        
        if (sequence.CacheSize.HasValue)
        {
            sb.Append($" CACHE {sequence.CacheSize.Value}");
        }
        else
        {
            sb.Append(" NO CACHE");
        }
        
        sb.AppendLine(";");
        return sb.ToString();
    }

    public static string GenerateDropSequenceStatement(string sequenceName, string schema = "dbo")
    {
        // Use dynamic SQL to check if sequence exists before dropping
        return $"""
            IF EXISTS (SELECT * FROM sys.sequences WHERE name = '{sequenceName}' AND schema_id = SCHEMA_ID('{schema}'))
            BEGIN
                DROP SEQUENCE [{schema}].[{sequenceName}];
            END
            """;
    }

    public static string GenerateAlterSequenceStatement(SqlSequence oldSequence, SqlSequence newSequence)
    {
        // SQL Server doesn't support ALTER SEQUENCE for all properties, so we'll use DROP + CREATE
        // But for incremental changes, we can try ALTER for some properties
        // For simplicity and reliability, we'll use DROP + CREATE
        System.Text.StringBuilder sb = new System.Text.StringBuilder();
        sb.AppendLine(GenerateDropSequenceStatement(oldSequence.Name, oldSequence.Schema));
        sb.Append(GenerateCreateSequenceStatement(newSequence));
        return sb.ToString();
    }
}

