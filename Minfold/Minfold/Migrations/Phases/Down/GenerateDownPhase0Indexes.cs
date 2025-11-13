using System.Text;

namespace Minfold.Migrations.Phases.Down;

public static class GenerateDownPhase0Indexes
{
    public static string Generate(SchemaDiff diff)
    {
        StringBuilder content = new StringBuilder();
        
        // Reverse index changes
        // For Modify: OldIndex = current state (after migration), NewIndex = target state (before migration)
        // For Add: NewIndex = from targetSchema (was dropped by migration) → should be ADDED back
        // For Drop: OldIndex = from currentSchema (was added by migration) → should be DROPPED
        foreach (TableDiff tableDiff in diff.ModifiedTables.OrderByDescending(t => t.TableName))
        {
            foreach (IndexChange indexChange in tableDiff.IndexChanges.Where(c => c.ChangeType == IndexChangeType.Modify))
            {
                // Drop current index (OldIndex) and add back target index (NewIndex)
                if (indexChange.OldIndex != null)
                {
                    content.AppendLine(GenerateIndexes.GenerateDropIndexStatement(indexChange.OldIndex));
                }
                if (indexChange.NewIndex != null)
                {
                    content.Append(GenerateIndexes.GenerateCreateIndexStatement(indexChange.NewIndex));
                    content.AppendLine();
                }
            }
            
            // Drop indexes that were added by the migration
            // Drop changes: index exists in currentSchema (after migration) but not in targetSchema (before migration)
            foreach (IndexChange indexChange in tableDiff.IndexChanges.Where(c => c.ChangeType == IndexChangeType.Drop))
            {
                if (indexChange.OldIndex != null)
                {
                    content.AppendLine(GenerateIndexes.GenerateDropIndexStatement(indexChange.OldIndex));
                }
            }
            
            // Add back indexes that were dropped by the migration
            // Add changes: index exists in targetSchema (before migration) but not in currentSchema (after migration)
            foreach (IndexChange indexChange in tableDiff.IndexChanges.Where(c => c.ChangeType == IndexChangeType.Add))
            {
                if (indexChange.NewIndex != null)
                {
                    content.Append(GenerateIndexes.GenerateCreateIndexStatement(indexChange.NewIndex));
                    content.AppendLine();
                }
            }
        }
        
        return content.ToString().Trim();
    }
}

