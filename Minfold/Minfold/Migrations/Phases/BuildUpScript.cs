using System.Text;

namespace Minfold;

public static class BuildUpScript
{
    public static string Build(List<PhaseContent> phases)
    {
        StringBuilder upScript = new StringBuilder();
        upScript.AppendLine("-- Generated using Minfold, do not edit manually");
        upScript.AppendLine("SET XACT_ABORT ON;");
        upScript.AppendLine();
        
        foreach (PhaseContent phase in phases.OrderBy(p => p.PhaseNumber))
        {
            if (!string.IsNullOrWhiteSpace(phase.Content))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phase.PhaseNumber, phase.Description));
                upScript.AppendLine(phase.Content);
                upScript.AppendLine();
            }
        }
        
        return upScript.ToString().TrimEnd();
    }
}

