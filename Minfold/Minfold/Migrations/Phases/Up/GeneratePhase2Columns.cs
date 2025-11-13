using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Up;

public static class GeneratePhase2Columns
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable> currentSchema,
        ConcurrentDictionary<string, SqlTable> targetSchema)
    {
        StringBuilder sb = new StringBuilder();
        
        // Column modifications
        foreach (TableDiff tableDiff in diff.ModifiedTables.OrderBy(t => t.TableName))
        {
            // Pass current schema (for dropping indexes) and target schema (for single-column detection)
            string columnModifications = GenerateColumns.GenerateColumnModifications(
                tableDiff, 
                currentSchema,
                targetSchema);
            if (!string.IsNullOrWhiteSpace(columnModifications))
            {
                sb.Append(columnModifications);
                sb.AppendLine();
            }
        }
        
        return sb.ToString().Trim();
    }
}

