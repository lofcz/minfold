using System.Text;

namespace Minfold.Migrations.Phases.Down;

public static class GenerateDownPhase11RecreateProcedures
{
    public static string Generate(SchemaDiff diff)
    {
        StringBuilder content = new StringBuilder();
        
        // Recreate procedures that were dropped or modified by the migration
        // In downDiff, NewProcedures = procedures in targetSchema (before migration) but not in currentSchema (after migration)
        // These are procedures that were DROPPED by the migration, so we need to RECREATE them in the down script
        foreach (SqlStoredProcedure newProcedure in diff.NewProcedures.OrderBy(p => p.Name))
        {
            content.Append(GenerateProcedures.GenerateCreateProcedureStatement(newProcedure));
            content.AppendLine();
        }
        // For Modify: restore the old procedure (NewProcedure - before modification)
        foreach (ProcedureChange procedureChange in diff.ModifiedProcedures)
        {
            if (procedureChange.ChangeType == ProcedureChangeType.Modify && procedureChange.NewProcedure != null)
            {
                content.Append(GenerateProcedures.GenerateCreateProcedureStatement(procedureChange.NewProcedure));
                content.AppendLine();
            }
        }
        
        return content.ToString().Trim();
    }
}

