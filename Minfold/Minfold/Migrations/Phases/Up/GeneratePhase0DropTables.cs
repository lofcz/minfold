using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Up;

public static class GeneratePhase0DropTables
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable> targetSchema)
    {
        StringBuilder sb = new StringBuilder();
        
        // Drop tables (drop FKs first, then tables)
        foreach (string droppedTableName in diff.DroppedTableNames.OrderByDescending(t => t))
        {
            // Get schema from target schema (before migration)
            string schema = "dbo";
            if (targetSchema.TryGetValue(droppedTableName.ToLowerInvariant(), out SqlTable? droppedTable))
            {
                schema = droppedTable.Schema;
            }
            sb.AppendLine($"DROP TABLE IF EXISTS [{schema}].[{droppedTableName}];");
        }
        
        return sb.ToString().Trim();
    }
}

