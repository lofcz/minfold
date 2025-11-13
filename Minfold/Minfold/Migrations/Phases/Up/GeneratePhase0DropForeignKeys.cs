using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Up;

public static class GeneratePhase0DropForeignKeys
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable> targetSchema)
    {
        StringBuilder sb = new StringBuilder();
        
        // Drop FKs from tables that will be dropped
        foreach (string droppedTableName in diff.DroppedTableNames)
        {
            // Find the table schema from target schema to get its FKs
            if (targetSchema.TryGetValue(droppedTableName.ToLowerInvariant(), out SqlTable? droppedTable))
            {
                // Drop FKs for this table
                HashSet<string> processedFksDrop = new HashSet<string>();
                foreach (SqlTableColumn column in droppedTable.Columns.Values)
                {
                    foreach (SqlForeignKey fk in column.ForeignKeys)
                    {
                        if (!processedFksDrop.Contains(fk.Name))
                        {
                            sb.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(fk));
                            processedFksDrop.Add(fk.Name);
                        }
                    }
                }
            }
        }
        
        return sb.ToString().Trim();
    }
}

