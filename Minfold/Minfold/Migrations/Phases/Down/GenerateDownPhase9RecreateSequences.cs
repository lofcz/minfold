using System.Text;

namespace Minfold.Migrations.Phases.Down;

public static class GenerateDownPhase9RecreateSequences
{
    public static string Generate(SchemaDiff diff)
    {
        StringBuilder content = new StringBuilder();
        
        // Recreate sequences that were dropped or modified by the migration
        // In downDiff, NewSequences = sequences in targetSchema (before migration) but not in currentSchema (after migration)
        // These are sequences that were DROPPED by the migration, so we need to RECREATE them in the down script
        if (diff.NewSequences.Count > 0)
        {
            MigrationLogger.Log($"\n=== Recreating sequences (were dropped by migration): {string.Join(", ", diff.NewSequences.Select(s => s.Name))} ===");
        }
        foreach (SqlSequence newSequence in diff.NewSequences.OrderBy(s => s.Name))
        {
            MigrationLogger.Log($"  [CREATE SEQUENCE] {newSequence.Name}");
            content.Append(GenerateSequences.GenerateCreateSequenceStatement(newSequence));
            content.AppendLine();
        }
        // For Modify: restore the old sequence (NewSequence - before modification)
        foreach (SequenceChange sequenceChange in diff.ModifiedSequences)
        {
            if (sequenceChange.ChangeType == SequenceChangeType.Modify && sequenceChange.NewSequence != null)
            {
                content.Append(GenerateSequences.GenerateCreateSequenceStatement(sequenceChange.NewSequence));
                content.AppendLine();
            }
        }
        
        return content.ToString().Trim();
    }
}

