using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Down;

public static class GenerateDownPhase5DropTables
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable> currentSchema)
    {
        StringBuilder content = new StringBuilder();
        
        // Drop tables that were added by the migration (in reverse order)
        // In downDiff, DroppedTableNames = tables in currentSchema (after migration) but not in targetSchema (before migration)
        // These are tables that were ADDED by the migration, so we need to DROP them in the down script
        for (int i = diff.DroppedTableNames.Count - 1; i >= 0; i--)
        {
            string droppedTableName = diff.DroppedTableNames[i];
            // Get schema from current schema (after migration)
            string schema = "dbo";
            if (currentSchema != null && currentSchema.TryGetValue(droppedTableName.ToLowerInvariant(), out SqlTable? droppedTable))
            {
                schema = droppedTable.Schema;
            }
            content.AppendLine($"DROP TABLE IF EXISTS [{schema}].[{droppedTableName}];");
        }
        
        return content.ToString().Trim();
    }
}

