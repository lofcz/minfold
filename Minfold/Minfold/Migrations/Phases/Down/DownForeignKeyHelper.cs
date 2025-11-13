using System.Collections.Concurrent;

namespace Minfold.Migrations.Phases.Down;

public static class DownForeignKeyHelper
{
    public static List<ForeignKeyChange> CollectForeignKeyChanges(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable>? currentSchema)
    {
        List<ForeignKeyChange> allFkChanges = new List<ForeignKeyChange>();
        
        // Collect FK changes from modified tables
        foreach (TableDiff tableDiff in diff.ModifiedTables)
        {
            allFkChanges.AddRange(tableDiff.ForeignKeyChanges);
        }
        
        // Also collect FKs from dropped tables (they need to be dropped before the table is dropped)
        // When a table is dropped, FKs referencing it are also dropped, but FKs ON the dropped table need to be dropped first
        foreach (string droppedTableName in diff.DroppedTableNames)
        {
            if (currentSchema != null && 
                currentSchema.TryGetValue(droppedTableName.ToLowerInvariant(), out SqlTable? droppedTable))
            {
                foreach (SqlTableColumn column in droppedTable.Columns.Values)
                {
                    foreach (SqlForeignKey fk in column.ForeignKeys)
                    {
                        // Add as a Drop change (FK exists in current schema but will be dropped)
                        allFkChanges.Add(new ForeignKeyChange(ForeignKeyChangeType.Drop, fk, null));
                    }
                }
            }
        }
        
        // Also collect FKs that reference dropped tables (they need to be dropped before the referenced table is dropped)
        foreach (string droppedTableName in diff.DroppedTableNames)
        {
            foreach (TableDiff tableDiff in diff.ModifiedTables)
            {
                foreach (SqlTableColumn column in (currentSchema?[tableDiff.TableName.ToLowerInvariant()]?.Columns.Values ?? Enumerable.Empty<SqlTableColumn>()))
                {
                    foreach (SqlForeignKey fk in column.ForeignKeys)
                    {
                        if (fk.RefTable.Equals(droppedTableName, StringComparison.OrdinalIgnoreCase))
                        {
                            // FK references a dropped table, need to drop it
                            if (!allFkChanges.Any(c => c.OldForeignKey?.Name == fk.Name && c.OldForeignKey?.Table == fk.Table))
                            {
                                allFkChanges.Add(new ForeignKeyChange(ForeignKeyChangeType.Drop, fk, null));
                            }
                        }
                    }
                }
            }
        }
        
        return allFkChanges;
    }
}

