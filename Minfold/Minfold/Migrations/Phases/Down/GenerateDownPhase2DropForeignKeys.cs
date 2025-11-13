using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Down;

public static class GenerateDownPhase2DropForeignKeys
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable> currentSchema,
        ConcurrentDictionary<string, SqlTable> targetSchema)
    {
        StringBuilder content = new StringBuilder();
        
        // Collect FK changes from modified tables for down script
        List<ForeignKeyChange> allFkChanges = DownForeignKeyHelper.CollectForeignKeyChanges(diff, currentSchema);
        
        MigrationLogger.Log($"\n=== FK Changes for down script ===");
        MigrationLogger.Log($"Total FK changes: {allFkChanges.Count}");
        foreach (var fkChange in allFkChanges)
        {
            MigrationLogger.Log($"  ChangeType: {fkChange.ChangeType}, FK: {fkChange.OldForeignKey?.Name ?? fkChange.NewForeignKey?.Name ?? "null"}");
        }
        
        // Drop FKs that need to be dropped or modified
        // For Modify: drop the new FK (current state after migration)
        foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Modify))
        {
            if (fkChange.NewForeignKey != null)
            {
                MigrationLogger.Log($"  [DROP FK] {fkChange.NewForeignKey.Name} (Modify)");
                content.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(fkChange.NewForeignKey));
            }
        }
        
        // Drop added FKs (were added by migration, need to drop in down script)
        foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Add))
        {
            if (fkChange.NewForeignKey != null)
            {
                MigrationLogger.Log($"  [DROP FK] {fkChange.NewForeignKey.Name} (Add)");
                content.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(fkChange.NewForeignKey));
            }
        }
        
        // Drop FKs that reference dropped tables (must be dropped before the referenced table)
        foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Drop))
        {
            if (fkChange.OldForeignKey != null)
            {
                // Check if this FK references a dropped table
                bool referencesDroppedTable = diff.DroppedTableNames.Any(name => 
                    name.Equals(fkChange.OldForeignKey.RefTable, StringComparison.OrdinalIgnoreCase));
                
                if (referencesDroppedTable)
                {
                    MigrationLogger.Log($"  [DROP FK] {fkChange.OldForeignKey.Name} (references dropped table {fkChange.OldForeignKey.RefTable})");
                    content.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(fkChange.OldForeignKey));
                }
            }
        }
        
        return content.ToString().Trim();
    }
    
}

