using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Up;

public static class GeneratePhase0DropPrimaryKeys
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable> targetSchema)
    {
        StringBuilder sb = new StringBuilder();
        
        // Drop PRIMARY KEY constraints before column modifications
        // Collect PK changes from modified tables
        HashSet<string> tablesWithPkDropped = new HashSet<string>();
        foreach (TableDiff tableDiff in diff.ModifiedTables.OrderBy(t => t.TableName))
        {
            // Check if any column that is part of the PK is being modified
            // SQL Server requires PK to be dropped before ALTER COLUMN on any PK column
            bool needsPkDropped = false;
            foreach (ColumnChange change in tableDiff.ColumnChanges)
            {
                if (change.OldColumn != null && change.OldColumn.IsPrimaryKey)
                {
                    // Drop PK if:
                    // 1. Column is being dropped
                    // 2. Column is being rebuilt (DROP+ADD) - PK must be dropped before column can be dropped
                    // 3. Column is being modified (even if it keeps PK status) - SQL Server requires PK drop before ALTER COLUMN
                    // 4. Column is being modified and losing PK status
                    if (change.ChangeType == ColumnChangeType.Drop || 
                        change.ChangeType == ColumnChangeType.Rebuild ||
                        change.ChangeType == ColumnChangeType.Modify)
                    {
                        needsPkDropped = true;
                        break;
                    }
                }
            }
            
            if (needsPkDropped && !tablesWithPkDropped.Contains(tableDiff.TableName.ToLowerInvariant()))
            {
                // Need to drop PK constraint before dropping/modifying the column
                // Get current PK columns from target schema to determine constraint name
                if (targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTable))
                {
                    List<SqlTableColumn> pkColumns = targetTable.Columns.Values.Where(c => c.IsPrimaryKey).OrderBy(c => c.OrdinalPosition).ToList();
                    if (pkColumns.Count > 0)
                    {
                        // Generate PK constraint name (SQL Server default pattern)
                        string pkConstraintName = $"PK_{tableDiff.TableName}";
                        sb.AppendLine(GeneratePrimaryKeys.GenerateDropPrimaryKeyStatement(tableDiff.TableName, pkConstraintName, targetTable.Schema));
                        tablesWithPkDropped.Add(tableDiff.TableName.ToLowerInvariant());
                    }
                }
            }
        }
        
        return sb.ToString().Trim();
    }
}

