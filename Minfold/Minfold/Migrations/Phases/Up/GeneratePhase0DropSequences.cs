using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Up;

public static class GeneratePhase0DropSequences
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlSequence> targetSequences)
    {
        StringBuilder sb = new StringBuilder();
        
        // Drop sequences that are being dropped (before dropping tables)
        foreach (string droppedSequenceName in diff.DroppedSequenceNames.OrderByDescending(s => s))
        {
            // Get schema from target schema (before migration)
            string schema = "dbo";
            if (targetSequences.TryGetValue(droppedSequenceName.ToLowerInvariant(), out SqlSequence? droppedSequence))
            {
                schema = droppedSequence.Schema;
            }
            sb.Append(GenerateSequences.GenerateDropSequenceStatement(droppedSequenceName, schema));
            sb.AppendLine();
        }
        
        return sb.ToString().Trim();
    }
}

