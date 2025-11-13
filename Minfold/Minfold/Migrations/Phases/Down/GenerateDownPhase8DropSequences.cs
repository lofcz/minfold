using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Down;

public static class GenerateDownPhase8DropSequences
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlSequence> currentSequences)
    {
        StringBuilder content = new StringBuilder();
        
        // Drop sequences that were added or modified by the migration (reverse order)
        // In downDiff, DroppedSequenceNames = sequences in currentSchema (after migration) but not in targetSchema (before migration)
        // These are sequences that were ADDED by the migration, so we need to DROP them in the down script
        if (diff.DroppedSequenceNames.Count > 0)
        {
            MigrationLogger.Log($"\n=== Dropping sequences (were added by migration): {string.Join(", ", diff.DroppedSequenceNames)} ===");
        }
        foreach (string droppedSequenceName in diff.DroppedSequenceNames.OrderByDescending(s => s))
        {
            MigrationLogger.Log($"  [DROP SEQUENCE] {droppedSequenceName}");
            // Get schema from current schema (after migration)
            string schema = "dbo";
            if (currentSequences.TryGetValue(droppedSequenceName.ToLowerInvariant(), out SqlSequence? droppedSequence))
            {
                schema = droppedSequence.Schema;
            }
            content.AppendLine(GenerateSequences.GenerateDropSequenceStatement(droppedSequenceName, schema));
        }
        // For Modify: drop the current sequence (OldSequence - after modification) before recreating the old one
        foreach (SequenceChange sequenceChange in diff.ModifiedSequences)
        {
            if (sequenceChange.ChangeType == SequenceChangeType.Modify && sequenceChange.OldSequence != null)
            {
                content.AppendLine(GenerateSequences.GenerateDropSequenceStatement(sequenceChange.OldSequence.Name, sequenceChange.OldSequence.Schema));
            }
        }
        
        return content.ToString().Trim();
    }
}

