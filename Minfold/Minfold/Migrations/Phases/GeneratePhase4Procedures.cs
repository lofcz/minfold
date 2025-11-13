using System.Text;

namespace Minfold;

public static class GeneratePhase4Procedures
{
    public static string Generate(SchemaDiff diff)
    {
        StringBuilder sb = new StringBuilder();
        
        // Create procedures (after constraints)
        foreach (SqlStoredProcedure newProcedure in diff.NewProcedures.OrderBy(p => p.Name))
        {
            sb.Append(GenerateProcedures.GenerateCreateProcedureStatement(newProcedure));
            sb.AppendLine();
        }
        
        // For modifications, create the new procedure (old one was already dropped in phase 0)
        foreach (ProcedureChange procedureChange in diff.ModifiedProcedures)
        {
            if (procedureChange.ChangeType == ProcedureChangeType.Modify && procedureChange.NewProcedure != null)
            {
                sb.Append(GenerateProcedures.GenerateCreateProcedureStatement(procedureChange.NewProcedure));
                sb.AppendLine();
            }
        }
        
        return sb.ToString().Trim();
    }
}

