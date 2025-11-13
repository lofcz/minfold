using System.Collections.Concurrent;
using Minfold.Migrations.Phases.Down;

namespace Minfold;

public static class GenerateIncrementalDownScript
{
    public static string Generate(
        SchemaDiff downDiff,
        ConcurrentDictionary<string, SqlTable> currentSchema,
        ConcurrentDictionary<string, SqlSequence> currentSequences,
        ConcurrentDictionary<string, SqlStoredProcedure> currentProcedures,
        ConcurrentDictionary<string, SqlTable> targetSchema,
        ConcurrentDictionary<string, SqlSequence> targetSequences,
        ConcurrentDictionary<string, SqlStoredProcedure> targetProcedures)
    {
        SchemaDiff diff = downDiff;

        // Generate phases using dedicated phase generators
        string phase0IndexesContent = GenerateDownPhase0Indexes.Generate(diff);
        string phase1DropPksContent = GenerateDownPhase1DropPrimaryKeys.Generate(diff, targetSchema);
        string phase2DropFksContent = GenerateDownPhase2DropForeignKeys.Generate(diff, currentSchema, targetSchema);
        string phase3ReverseColumnsContent = GenerateDownPhase3ReverseColumns.Generate(diff, currentSchema, targetSchema);
        string phase4RestorePksContent = GenerateDownPhase4RestorePrimaryKeys.Generate(diff, targetSchema);
        string phase5DropTablesContent = GenerateDownPhase5DropTables.Generate(diff, currentSchema);
        string phase6RecreateTablesContent = GenerateDownPhase6RecreateTables.Generate(diff, targetSchema);
        string phase7RestoreFksContent = GenerateDownPhase7RestoreForeignKeys.Generate(diff, currentSchema, targetSchema);
        string phase8DropSequencesContent = GenerateDownPhase8DropSequences.Generate(diff, currentSequences);
        string phase9RecreateSequencesContent = GenerateDownPhase9RecreateSequences.Generate(diff);
        string phase10DropProceduresContent = GenerateDownPhase10DropProcedures.Generate(diff, currentProcedures);
        string phase11RecreateProceduresContent = GenerateDownPhase11RecreateProcedures.Generate(diff);
        string phase12ColumnReorderContent = GenerateDownPhase12ColumnReorder.Generate(diff, currentSchema, targetSchema);

        // Assemble phases into list (only include phases with content)
        List<PhaseContent> phases = new List<PhaseContent>();
        int phaseNumber = 1;
        
        if (!string.IsNullOrWhiteSpace(phase0IndexesContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Reverse Index Changes", phase0IndexesContent));
        }
        if (!string.IsNullOrWhiteSpace(phase1DropPksContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Drop Primary Key Constraints", phase1DropPksContent));
        }
        if (!string.IsNullOrWhiteSpace(phase2DropFksContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Drop Foreign Keys", phase2DropFksContent));
        }
        if (!string.IsNullOrWhiteSpace(phase3ReverseColumnsContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Reverse Column Modifications", phase3ReverseColumnsContent));
        }
        if (!string.IsNullOrWhiteSpace(phase4RestorePksContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Restore Primary Key Constraints", phase4RestorePksContent));
        }
        if (!string.IsNullOrWhiteSpace(phase5DropTablesContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Drop Tables", phase5DropTablesContent));
        }
        if (!string.IsNullOrWhiteSpace(phase6RecreateTablesContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Recreate Tables", phase6RecreateTablesContent));
        }
        if (!string.IsNullOrWhiteSpace(phase7RestoreFksContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Restore Foreign Keys", phase7RestoreFksContent));
        }
        if (!string.IsNullOrWhiteSpace(phase8DropSequencesContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Drop Sequences", phase8DropSequencesContent));
        }
        if (!string.IsNullOrWhiteSpace(phase9RecreateSequencesContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Recreate Sequences", phase9RecreateSequencesContent));
        }
        if (!string.IsNullOrWhiteSpace(phase10DropProceduresContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Drop Stored Procedures", phase10DropProceduresContent));
        }
        if (!string.IsNullOrWhiteSpace(phase11RecreateProceduresContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Recreate Stored Procedures", phase11RecreateProceduresContent));
        }
        if (!string.IsNullOrWhiteSpace(phase12ColumnReorderContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Reorder Columns", phase12ColumnReorderContent));
        }
        
        return BuildDownScript.Build(phases);
    }
}