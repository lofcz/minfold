using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Up;

public static class GeneratePhase0DropForeignKeys
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable> targetSchema)
    {
        StringBuilder sb = new StringBuilder();
        HashSet<string> processedFksDrop = new HashSet<string>();
        
        // Drop FKs from tables that will be dropped
        foreach (string droppedTableName in diff.DroppedTableNames)
        {
            // Find the table schema from target schema to get its FKs
            if (targetSchema.TryGetValue(droppedTableName.ToLowerInvariant(), out SqlTable? droppedTable))
            {
                // Drop FKs for this table
                foreach (SqlTableColumn column in droppedTable.Columns.Values)
                {
                    foreach (SqlForeignKey fk in column.ForeignKeys)
                    {
                        if (!processedFksDrop.Contains(fk.Name))
                        {
                            sb.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(fk));
                            processedFksDrop.Add(fk.Name);
                        }
                    }
                }
            }
        }
        
        // Drop FKs that reference primary keys being dropped
        // Find tables where PKs are being dropped
        HashSet<string> tablesWithPkDropped = new HashSet<string>();
        foreach (TableDiff tableDiff in diff.ModifiedTables)
        {
            bool needsPkDropped = false;
            foreach (ColumnChange change in tableDiff.ColumnChanges)
            {
                if (change.OldColumn != null && change.OldColumn.IsPrimaryKey)
                {
                    // Drop PK if:
                    // 1. Column is being dropped
                    // 2. Column is being modified and losing PK status
                    // 3. Column is being rebuilt (DROP+ADD) - PK must be dropped before column can be dropped
                    if (change.ChangeType == ColumnChangeType.Drop || 
                        change.ChangeType == ColumnChangeType.Rebuild ||
                        (change.ChangeType == ColumnChangeType.Modify && change.NewColumn != null && !change.NewColumn.IsPrimaryKey))
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
        
        // Find all FKs that reference these tables' PK columns
        foreach (KeyValuePair<string, SqlTable> tablePair in targetSchema)
        {
            foreach (SqlTableColumn column in tablePair.Value.Columns.Values)
            {
                foreach (SqlForeignKey fk in column.ForeignKeys)
                {
                    // Check if this FK references a table whose PK is being dropped
                    if (tablesWithPkDropped.Contains(fk.RefTable.ToLowerInvariant()))
                    {
                        // Verify the FK references a PK column
                        if (targetSchema.TryGetValue(fk.RefTable.ToLowerInvariant(), out SqlTable? refTable))
                        {
                            if (refTable.Columns.TryGetValue(fk.RefColumn.ToLowerInvariant(), out SqlTableColumn? refColumn) && refColumn.IsPrimaryKey)
                            {
                                if (!processedFksDrop.Contains(fk.Name))
                                {
                                    sb.AppendLine(GenerateForeignKeys.GenerateDropForeignKeyStatement(fk));
                                    processedFksDrop.Add(fk.Name);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return sb.ToString().Trim();
    }
}

