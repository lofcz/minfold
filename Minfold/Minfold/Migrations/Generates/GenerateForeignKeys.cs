using System.Text;

namespace Minfold;

public static class GenerateForeignKeys
{
    public static string GenerateForeignKeyStatement(
        List<SqlForeignKey> fkGroup, 
        Dictionary<string, SqlTable> allTables,
        bool forceNoCheck = false)
    {
        if (fkGroup.Count == 0)
        {
            return string.Empty;
        }

        SqlForeignKey firstFk = fkGroup[0];
        StringBuilder sb = new StringBuilder();

        sb.Append($"ALTER TABLE [{firstFk.Schema}].[{firstFk.Table}] WITH ");
        
        // Use forceNoCheck if specified, otherwise use original NotEnforced value
        bool useNoCheck = forceNoCheck || firstFk.NotEnforced;
        sb.Append(useNoCheck ? "NOCHECK" : "CHECK");
        sb.Append(" ADD CONSTRAINT [");
        sb.Append(firstFk.Name);
        sb.Append("] FOREIGN KEY(");

        // Multi-column FK support
        List<string> columns = fkGroup.OrderBy(f => f.Column).Select(f => $"[{f.Column}]").ToList();
        List<string> refColumns = fkGroup.OrderBy(f => f.Column).Select(f => $"[{f.RefColumn}]").ToList();

        sb.Append(string.Join(", ", columns));
        sb.Append($") REFERENCES [{firstFk.RefSchema}].[{firstFk.RefTable}](");
        sb.Append(string.Join(", ", refColumns));
        sb.Append(")");

        if (firstFk.NotForReplication)
        {
            sb.Append(" NOT FOR REPLICATION");
        }

        // Delete action
        switch (firstFk.DeleteAction)
        {
            case 1:
                sb.Append(" ON DELETE CASCADE");
                break;
            case 2:
                sb.Append(" ON DELETE SET NULL");
                break;
            case 3:
                sb.Append(" ON DELETE SET DEFAULT");
                break;
        }

        // Update action
        switch (firstFk.UpdateAction)
        {
            case 1:
                sb.Append(" ON UPDATE CASCADE");
                break;
            case 2:
                sb.Append(" ON UPDATE SET NULL");
                break;
            case 3:
                sb.Append(" ON UPDATE SET DEFAULT");
                break;
        }

        sb.AppendLine(";");

        // Only add CHECK CONSTRAINT statement if NOT forcing NOCHECK and NOT originally NOCHECK
        if (!forceNoCheck && !firstFk.NotEnforced)
        {
            sb.Append($"ALTER TABLE [{firstFk.Schema}].[{firstFk.Table}] CHECK CONSTRAINT [{firstFk.Name}];");
            sb.AppendLine();
        }

        return sb.ToString();
    }

    public static string GenerateDropForeignKeyStatement(SqlForeignKey fk, string? variableSuffix = null)
    {
        // Use provided suffix or generate a deterministic one to avoid conflicts
        string varSuffix = variableSuffix ?? MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(fk.Schema, fk.Table, fk.Name, "dropfk");
        
        // Use dynamic SQL to check if constraint exists before dropping
        return $"""
            DECLARE @fkConstraintName_{varSuffix} NVARCHAR(128);
            SELECT @fkConstraintName_{varSuffix} = name FROM sys.foreign_keys 
            WHERE parent_object_id = OBJECT_ID('[{fk.Schema}].[{fk.Table}]') 
            AND name = '{fk.Name}';
            IF @fkConstraintName_{varSuffix} IS NOT NULL
                EXEC('ALTER TABLE [{fk.Schema}].[{fk.Table}] DROP CONSTRAINT [' + @fkConstraintName_{varSuffix} + ']');
            """;
    }
}

