using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Up;

public static class GeneratePhase0DropProcedures
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlStoredProcedure> targetProcedures)
    {
        StringBuilder sb = new StringBuilder();
        
        // Drop procedures that are being dropped or modified (before dropping tables)
        foreach (string droppedProcedureName in diff.DroppedProcedureNames.OrderByDescending(p => p))
        {
            // Get schema from target schema (before migration)
            string schema = "dbo";
            if (targetProcedures.TryGetValue(droppedProcedureName.ToLowerInvariant(), out SqlStoredProcedure? droppedProcedure))
            {
                schema = droppedProcedure.Schema;
            }
            sb.Append(GenerateProcedures.GenerateDropProcedureStatement(droppedProcedureName, schema));
            sb.AppendLine();
        }
        
        // Note: Modified procedures are handled by dropping old and creating new
        foreach (ProcedureChange procedureChange in diff.ModifiedProcedures)
        {
            if (procedureChange.ChangeType == ProcedureChangeType.Modify && procedureChange.OldProcedure != null)
            {
                // For modifications, drop the old procedure before creating the new one
                sb.Append(GenerateProcedures.GenerateDropProcedureStatement(procedureChange.OldProcedure.Name, procedureChange.OldProcedure.Schema));
                sb.AppendLine();
            }
        }
        
        return sb.ToString().Trim();
    }
}

