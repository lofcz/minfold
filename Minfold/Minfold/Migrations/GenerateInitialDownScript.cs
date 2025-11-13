using System.Collections.Concurrent;
using System.Text;

namespace Minfold;

public static class GenerateInitialDownScript
{
    public static string Generate(
        ConcurrentDictionary<string, SqlTable> schema,
        ConcurrentDictionary<string, SqlSequence> sequences,
        ConcurrentDictionary<string, SqlStoredProcedure> procedures)
    {
        // Generate down script (drop procedures first, then tables, then sequences - reverse order of creation)
        // Transaction is managed by MigrationApplier.ExecuteMigrationScript using ADO.NET transactions
        StringBuilder downScript = new StringBuilder();
        downScript.AppendLine("-- Generated using Minfold, do not edit manually");
        downScript.AppendLine("SET XACT_ABORT ON;");
        downScript.AppendLine();
        
        List<KeyValuePair<string, SqlTable>> tables = schema.OrderBy(x => x.Key).ToList();
        
        // Drop procedures (reverse order)
        foreach (SqlStoredProcedure procedure in procedures.Values.OrderByDescending(p => p.Name))
        {
            downScript.AppendLine(GenerateProcedures.GenerateDropProcedureStatement(procedure.Name, procedure.Schema));
        }
        
        // Drop tables (reverse order)
        for (int i = tables.Count - 1; i >= 0; i--)
        {
            SqlTable table = tables[i].Value;
            string tableName = table.Name;
            downScript.AppendLine($"DROP TABLE IF EXISTS [{table.Schema}].[{tableName}];");
        }
        
        // Drop sequences (reverse order)
        foreach (SqlSequence sequence in sequences.Values.OrderByDescending(s => s.Name))
        {
            downScript.AppendLine(GenerateSequences.GenerateDropSequenceStatement(sequence.Name, sequence.Schema));
        }
        
        downScript.AppendLine();
        
        return downScript.ToString().TrimEnd();
    }
}

