using System.Collections.Concurrent;
using System.Text;

namespace Minfold;

public static class GenerateInitialUpScript
{
    public static string Generate(
        ConcurrentDictionary<string, SqlTable> schema,
        ConcurrentDictionary<string, SqlSequence> sequences,
        ConcurrentDictionary<string, SqlStoredProcedure> procedures)
    {
        StringBuilder phase1Tables = new StringBuilder(); // CREATE TABLE statements
        StringBuilder phase3Constraints = new StringBuilder(); // ALTER TABLE FK constraints

        // Generate CREATE TABLE statements (Phase 1) - without foreign keys
        List<KeyValuePair<string, SqlTable>> tables = schema.OrderBy(x => x.Key).ToList();

        foreach (KeyValuePair<string, SqlTable> tablePair in tables)
        {
            string createTableSql = GenerateTables.GenerateCreateTableStatement(tablePair.Value);
            phase1Tables.AppendLine(createTableSql);
            phase1Tables.AppendLine();
        }

        // Collect all foreign keys first, then generate ALTER TABLE statements (Phase 3)
        // Use NOCHECK → CHECK pattern to handle circular dependencies and improve performance
        HashSet<string> processedFks = new HashSet<string>();
        List<(List<SqlForeignKey> FkGroup, bool WasNoCheck)> allFkGroupsWithState = new List<(List<SqlForeignKey>, bool)>();
        
        foreach (KeyValuePair<string, SqlTable> tablePair in tables)
        {
            foreach (SqlTableColumn column in tablePair.Value.Columns.Values)
            {
                foreach (SqlForeignKey fk in column.ForeignKeys)
                {
                    // Avoid duplicates (multi-column FKs appear multiple times)
                    if (!processedFks.Contains(fk.Name))
                    {
                        // Group multi-column FKs
                        List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { fk };
                        foreach (KeyValuePair<string, SqlTable> otherTablePair in tables)
                        {
                            foreach (SqlTableColumn otherColumn in otherTablePair.Value.Columns.Values)
                            {
                                foreach (SqlForeignKey otherFk in otherColumn.ForeignKeys)
                                {
                                    if (otherFk.Name == fk.Name && otherFk.Table == fk.Table && otherFk != fk)
                                    {
                                        fkGroup.Add(otherFk);
                                    }
                                }
                            }
                        }

                        // Store original NotEnforced state (all FKs in a group have the same state)
                        bool wasNoCheck = fkGroup[0].NotEnforced;
                        allFkGroupsWithState.Add((fkGroup, wasNoCheck));
                        processedFks.Add(fk.Name);
                    }
                }
            }
        }

        // Create all FKs with NOCHECK first (avoids circular dependency issues and reduces lock time)
        Dictionary<string, SqlTable> tablesDict = new Dictionary<string, SqlTable>(schema);
        foreach (var (fkGroup, wasNoCheck) in allFkGroupsWithState.OrderBy(g => g.FkGroup[0].Table).ThenBy(g => g.FkGroup[0].Name))
        {
            // Force NOCHECK during creation to avoid circular dependency issues
            string fkSql = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDict, forceNoCheck: true);
            phase3Constraints.Append(fkSql);
            phase3Constraints.AppendLine();
        }

        // Restore CHECK state for FKs that weren't originally NOCHECK
        // IMPORTANT: CHECK CONSTRAINT doesn't always restore is_not_trusted correctly after WITH NOCHECK
        // So we need to drop and recreate the FK with WITH CHECK to ensure correct NotEnforced state
        foreach (var (fkGroup, wasNoCheck) in allFkGroupsWithState.OrderBy(g => g.FkGroup[0].Table).ThenBy(g => g.FkGroup[0].Name))
        {
            if (!wasNoCheck)
            {
                SqlForeignKey firstFk = fkGroup[0];
                // Drop the FK that was created with NOCHECK
                phase3Constraints.AppendLine($"ALTER TABLE [{firstFk.Schema}].[{firstFk.Table}] DROP CONSTRAINT [{firstFk.Name}];");
                
                // Recreate it with WITH CHECK to ensure correct NotEnforced state
                string fkSqlWithCheck = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDict, forceNoCheck: false);
                if (!string.IsNullOrEmpty(fkSqlWithCheck))
                {
                    phase3Constraints.Append(fkSqlWithCheck);
                    phase3Constraints.AppendLine();
                }
            }
        }

        // Build up script with phases (only include phases with content)
        // Transaction is managed by MigrationApplier.ExecuteMigrationScript using ADO.NET transactions
        StringBuilder upScript = new StringBuilder();
        upScript.AppendLine("-- Generated using Minfold, do not edit manually");
        upScript.AppendLine("SET XACT_ABORT ON;");
        upScript.AppendLine();
        
        int phaseNumber = 1;
        
        // Phase 0.5: Create Sequences (before tables, so they can be used in table defaults)
        StringBuilder phase0_5Sequences = new StringBuilder();
        foreach (SqlSequence sequence in sequences.Values.OrderBy(s => s.Name))
        {
            phase0_5Sequences.Append(GenerateSequences.GenerateCreateSequenceStatement(sequence));
            phase0_5Sequences.AppendLine();
        }
        string phase0_5Content = phase0_5Sequences.ToString().Trim();
        if (!string.IsNullOrEmpty(phase0_5Content))
        {
            upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Create Sequences"));
            upScript.AppendLine(phase0_5Content);
            upScript.AppendLine();
            phaseNumber++;
        }
        
        // Phase 1: Create Tables
        string phase1Content = phase1Tables.ToString().Trim();
        if (!string.IsNullOrEmpty(phase1Content))
        {
            upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Create Tables"));
            upScript.AppendLine(phase1Content);
            upScript.AppendLine();
            phaseNumber++;
        }
        
        // Phase 2: Modify Columns (ADD/DROP/ALTER COLUMN) - empty for initial migration
        // This phase is included for consistency but will be empty
        
        // Phase 3: Add Foreign Key Constraints
        string phase3Content = phase3Constraints.ToString().Trim();
        if (!string.IsNullOrEmpty(phase3Content))
        {
            upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Add Foreign Key Constraints"));
            upScript.AppendLine(phase3Content);
            upScript.AppendLine();
            phaseNumber++;
        }
        
        // Phase 4: Create Stored Procedures (after constraints)
        StringBuilder phase4Procedures = new StringBuilder();
        foreach (SqlStoredProcedure procedure in procedures.Values.OrderBy(p => p.Name))
        {
            phase4Procedures.Append(GenerateProcedures.GenerateCreateProcedureStatement(procedure));
            phase4Procedures.AppendLine();
        }
        string phase4Content = phase4Procedures.ToString().Trim();
        if (!string.IsNullOrEmpty(phase4Content))
        {
            upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Create Stored Procedures"));
            upScript.AppendLine(phase4Content);
            upScript.AppendLine();
        }
        
        return upScript.ToString().TrimEnd();
    }
}

