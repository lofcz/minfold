using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Down;

public static class GenerateDownPhase10DropProcedures
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlStoredProcedure> currentProcedures)
    {
        StringBuilder content = new StringBuilder();
        
        // Drop procedures that were added or modified by the migration (reverse order)
        // In downDiff, DroppedProcedureNames = procedures in currentSchema (after migration) but not in targetSchema (before migration)
        // These are procedures that were ADDED by the migration, so we need to DROP them in the down script
        foreach (string droppedProcedureName in diff.DroppedProcedureNames.OrderByDescending(p => p))
        {
            // Get schema from current schema (after migration)
            string schema = "dbo";
            if (currentProcedures.TryGetValue(droppedProcedureName.ToLowerInvariant(), out SqlStoredProcedure? droppedProcedure))
            {
                schema = droppedProcedure.Schema;
            }
            content.Append(GenerateProcedures.GenerateDropProcedureStatement(droppedProcedureName, schema));
            content.AppendLine();
        }
        // For Modify: drop the current procedure (OldProcedure - after modification) before recreating the old one
        foreach (ProcedureChange procedureChange in diff.ModifiedProcedures)
        {
            if (procedureChange.ChangeType == ProcedureChangeType.Modify && procedureChange.OldProcedure != null)
            {
                content.Append(GenerateProcedures.GenerateDropProcedureStatement(procedureChange.OldProcedure.Name, procedureChange.OldProcedure.Schema));
                content.AppendLine();
            }
        }
        
        return content.ToString().Trim();
    }
}

