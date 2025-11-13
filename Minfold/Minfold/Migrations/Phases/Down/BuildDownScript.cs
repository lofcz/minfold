using System.Text;

namespace Minfold.Migrations.Phases.Down;

public static class BuildDownScript
{
    public static string Build(List<PhaseContent> phases)
    {
        StringBuilder downScript = new StringBuilder();
        downScript.AppendLine("-- Generated using Minfold, do not edit manually");
        downScript.AppendLine("SET XACT_ABORT ON;");
        downScript.AppendLine();
        
        foreach (PhaseContent phase in phases.OrderBy(p => p.PhaseNumber))
        {
            if (!string.IsNullOrWhiteSpace(phase.Content))
            {
                downScript.Append(MigrationGenerator.GenerateSectionHeader(phase.PhaseNumber, phase.Description));
                downScript.AppendLine(phase.Content);
                downScript.AppendLine();
            }
        }
        
        return downScript.ToString().TrimEnd();
    }
}

