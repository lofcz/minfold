using System.Collections.Concurrent;
using Minfold.Migrations.Phases.Up;

namespace Minfold;

public static class GenerateIncrementalUpScript
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable> currentSchema,
        ConcurrentDictionary<string, SqlSequence> targetSequences,
        ConcurrentDictionary<string, SqlStoredProcedure> targetProcedures,
        ConcurrentDictionary<string, SqlTable> targetSchema,
        SchemaDiff upDiff)
    {
        // Generate phases using dedicated phase generators
        string phase0DropProceduresContent = GeneratePhase0DropProcedures.Generate(diff, targetProcedures);
        string phase0DropSequencesContent = GeneratePhase0DropSequences.Generate(diff, targetSequences);
        string phase0DropFksContent = GeneratePhase0DropForeignKeys.Generate(diff, targetSchema);
        string phase0DropPksContent = GeneratePhase0DropPrimaryKeys.Generate(diff, targetSchema);
        string phase0DropTablesContent = GeneratePhase0DropTables.Generate(diff, targetSchema);
        string phase0_5SequencesContent = GeneratePhase0_5Sequences.Generate(diff);
        string phase1TablesContent = GeneratePhase1Tables.Generate(diff);
        string phase2ColumnsContent = GeneratePhase2Columns.Generate(
            diff,
            currentSchema,
            targetSchema);

        // Generate remaining phases
        string phase3ConstraintsContent = GeneratePhase3Constraints.Generate(diff, targetSchema, currentSchema);
        string phase3_5ColumnReorderContent = GeneratePhase3_5ColumnReorder.Generate(
            upDiff,
            targetSchema,
            currentSchema);
        string phase4ProceduresContent = GeneratePhase4Procedures.Generate(diff);

        // Assemble phases into list (only include phases with content)
        List<PhaseContent> phases = new List<PhaseContent>();
        int phaseNumber = 1;
        
        if (!string.IsNullOrWhiteSpace(phase0DropProceduresContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Drop Stored Procedures", phase0DropProceduresContent));
        }
        if (!string.IsNullOrWhiteSpace(phase0DropSequencesContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Drop Sequences", phase0DropSequencesContent));
        }
        if (!string.IsNullOrWhiteSpace(phase0DropFksContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Drop Foreign Keys", phase0DropFksContent));
        }
        if (!string.IsNullOrWhiteSpace(phase0DropPksContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Drop Primary Key Constraints", phase0DropPksContent));
        }
        if (!string.IsNullOrWhiteSpace(phase0DropTablesContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Drop Tables", phase0DropTablesContent));
        }
        if (!string.IsNullOrWhiteSpace(phase0_5SequencesContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Create Sequences", phase0_5SequencesContent));
        }
        if (!string.IsNullOrWhiteSpace(phase1TablesContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Create Tables", phase1TablesContent));
        }
        if (!string.IsNullOrWhiteSpace(phase2ColumnsContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Modify Columns", phase2ColumnsContent));
        }
        if (!string.IsNullOrWhiteSpace(phase3ConstraintsContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Add Foreign Key Constraints and Primary Key Constraints", phase3ConstraintsContent));
        }
        if (!string.IsNullOrWhiteSpace(phase3_5ColumnReorderContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Reorder Columns", phase3_5ColumnReorderContent));
        }
        if (!string.IsNullOrWhiteSpace(phase4ProceduresContent))
        {
            phases.Add(new PhaseContent(phaseNumber++, "Create Stored Procedures", phase4ProceduresContent));
        }
        
        return BuildUpScript.Build(phases);
    }
}

