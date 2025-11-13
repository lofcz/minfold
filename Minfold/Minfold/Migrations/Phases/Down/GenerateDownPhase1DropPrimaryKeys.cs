using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Down;

public static class GenerateDownPhase1DropPrimaryKeys
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable> targetSchema)
    {
        StringBuilder content = new StringBuilder();
        
        // Reverse PRIMARY KEY changes - drop PKs first (before restoring columns)
        // We need to drop PK if:
        // 1. Any columns gained PK status (new PK columns)
        // 2. Any columns that are currently PKs need to be modified or dropped (losing PK status)
        foreach (TableDiff tableDiff in diff.ModifiedTables.OrderByDescending(t => t.TableName))
        {
            bool needsPkDropped = false;
            
            // Check if any columns gained PK status (need to drop new PK)
            foreach (ColumnChange change in tableDiff.ColumnChanges)
            {
                if (change.NewColumn != null && change.NewColumn.IsPrimaryKey)
                {
                    if (change.ChangeType == ColumnChangeType.Add || 
                        (change.ChangeType == ColumnChangeType.Modify && change.OldColumn != null && !change.OldColumn.IsPrimaryKey))
                    {
                        needsPkDropped = true;
                        break;
                    }
                }
            }
            
            // Also check if any columns that are currently PKs need to be modified or dropped
            // For dropped columns: OldColumn is from current schema (after migration), so check OldColumn.IsPrimaryKey
            // For modified columns: NewColumn is from current schema (after migration), so check if it's a PK that will lose PK status
            if (!needsPkDropped)
            {
                foreach (ColumnChange change in tableDiff.ColumnChanges)
                {
                    // Check if a column that is currently a PK is being dropped
                    // OldColumn represents the current state (after migration), so check if it's a PK
                    if (change.ChangeType == ColumnChangeType.Drop && change.OldColumn != null && change.OldColumn.IsPrimaryKey)
                    {
                        needsPkDropped = true;
                        break;
                    }
                    // Check if a column that is currently a PK is being modified to lose PK status
                    // NewColumn represents the current state (after migration)
                    if (change.ChangeType == ColumnChangeType.Modify && change.NewColumn != null && change.NewColumn.IsPrimaryKey && 
                        change.OldColumn != null && !change.OldColumn.IsPrimaryKey)
                    {
                        needsPkDropped = true;
                        break;
                    }
                }
            }
            
            if (needsPkDropped)
            {
                // Get current PK columns from schema after changes to drop
                ConcurrentDictionary<string, SqlTable> schemaAfterChanges = MigrationSchemaSnapshot.ApplySchemaDiffToTarget(targetSchema, diff);
                if (schemaAfterChanges.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? newTable))
                {
                    List<SqlTableColumn> newPkColumns = newTable.Columns.Values.Where(c => c.IsPrimaryKey).OrderBy(c => c.OrdinalPosition).ToList();
                    if (newPkColumns.Count > 0)
                    {
                        string pkConstraintName = $"PK_{tableDiff.TableName}";
                        content.AppendLine(GeneratePrimaryKeys.GenerateDropPrimaryKeyStatement(tableDiff.TableName, pkConstraintName, newTable.Schema));
                    }
                }
            }
        }
        
        return content.ToString().Trim();
    }
}

