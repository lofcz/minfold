using System.Text;

namespace Minfold.Migrations.Phases.Up;

public static class GeneratePhase1Tables
{
    public static string Generate(SchemaDiff diff)
    {
        StringBuilder sb = new StringBuilder();
        
        // Create new tables
        foreach (SqlTable newTable in diff.NewTables.OrderBy(t => t.Name))
        {
            string createTableSql = GenerateTables.GenerateCreateTableStatement(newTable);
            sb.AppendLine(createTableSql);
            sb.AppendLine();
        }
        
        return sb.ToString().Trim();
    }
}

