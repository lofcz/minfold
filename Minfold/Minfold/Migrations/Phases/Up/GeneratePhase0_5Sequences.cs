using System.Text;

namespace Minfold.Migrations.Phases.Up;

public static class GeneratePhase0_5Sequences
{
    public static string Generate(SchemaDiff diff)
    {
        StringBuilder sb = new StringBuilder();
        
        // Create sequences (before tables, so they can be used in table defaults)
        foreach (SqlSequence newSequence in diff.NewSequences.OrderBy(s => s.Name))
        {
            sb.Append(GenerateSequences.GenerateCreateSequenceStatement(newSequence));
            sb.AppendLine();
        }
        
        // Note: Modified sequences are handled via GenerateAlterSequenceStatement which drops and recreates
        foreach (SequenceChange sequenceChange in diff.ModifiedSequences)
        {
            if (sequenceChange.ChangeType == SequenceChangeType.Modify && sequenceChange.NewSequence != null)
            {
                // For modifications, drop and recreate (handled by GenerateAlterSequenceStatement)
                sb.Append(GenerateSequences.GenerateAlterSequenceStatement(sequenceChange.OldSequence!, sequenceChange.NewSequence));
                sb.AppendLine();
            }
        }
        
        return sb.ToString().Trim();
    }
}

