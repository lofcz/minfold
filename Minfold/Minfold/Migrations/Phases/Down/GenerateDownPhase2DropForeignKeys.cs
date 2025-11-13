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
        
        // Drop FKs that reference primary keys being dropped
        // Find tables where PKs are being dropped (in Phase 1)
        HashSet<string> tablesWithPkDropped = new HashSet<string>();
        foreach (TableDiff tableDiff in diff.ModifiedTables)
        {
            bool needsPkDropped = false;
            foreach (ColumnChange change in tableDiff.ColumnChanges)
            {
                // For down script: NewColumn is from current schema (after migration)
                // If NewColumn is a PK and we're reversing the change, we need to drop the PK
                if (change.NewColumn != null && change.NewColumn.IsPrimaryKey)
                {
                    // Drop PK if:
                    // 1. Column is being dropped (Add change in up script = Drop in down script)
                    // 2. Column is being modified and losing PK status
                    // 3. Column is being rebuilt (DROP+ADD) - PK must be dropped before column can be dropped
                    if (change.ChangeType == ColumnChangeType.Add || 
                        change.ChangeType == ColumnChangeType.Rebuild ||
                        (change.ChangeType == ColumnChangeType.Modify && change.OldColumn != null && !change.OldColumn.IsPrimaryKey))
                    {
                        needsPkDropped = true;
                        break;
                    }
                }
            }
            
            if (needsPkDropped)
            {
                tablesWithPkDropped.Add(tableDiff.TableName.ToLowerInvariant());
            }
        }
        
        // Find all FKs in current schema (after migration) that reference these tables' PK columns
        HashSet<string> processedFksDrop = new HashSet<string>();
        foreach (KeyValuePair<string, SqlTable> tablePair in currentSchema)
        {
            foreach (SqlTableColumn column in tablePair.Value.Columns.Values)
            {
                foreach (SqlForeignKey fk in column.ForeignKeys)
                {
                    // Check if this FK references a table whose PK is being dropped
                    if (tablesWithPkDropped.Contains(fk.RefTable.ToLowerInvariant()))
                    {
                        // Verify the FK references a PK column
                        if (currentSchema.TryGetValue(fk.RefTable.ToLowerInvariant(), out SqlTable? refTable))
                        {
                            if (refTable.Columns.TryGetValue(fk.RefColumn.ToLowerInvariant(), out SqlTableColumn? refColumn) && refColumn.IsPrimaryKey)
                            {
                                // Only drop if not already processed (not in diff changes)
                                if (!processedFksDrop.Contains(fk.Name) && !allFkChanges.Any(c => 
                                    (c.NewForeignKey?.Name == fk.Name) || (c.OldForeignKey?.Name == fk.Name)))
                                {
                                    MigrationLogger.Log($"  [DROP FK] {fk.Name} (references PK being dropped in {fk.RefTable})");
                                    content.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(fk));
                                    processedFksDrop.Add(fk.Name);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return content.ToString().Trim();
    }
    
}

