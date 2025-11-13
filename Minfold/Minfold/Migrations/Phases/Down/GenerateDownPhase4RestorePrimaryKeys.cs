using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Down;

public static class GenerateDownPhase4RestorePrimaryKeys
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable> targetSchema)
    {
        StringBuilder content = new StringBuilder();
        
        // Restore PKs after columns have been restored
        // We need to restore PK if the target schema (before migration) had columns as PKs
        foreach (TableDiff tableDiff in diff.ModifiedTables.OrderByDescending(t => t.TableName))
        {
            bool needsPkRestored = false;
            
            foreach (ColumnChange change in tableDiff.ColumnChanges)
            {
                if (change.ChangeType == ColumnChangeType.Drop)
                {
                    // For dropped columns, check if they were PKs in the target schema (before migration)
                    // We need to check the target schema directly since NewColumn is null
                    if (targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTable))
                    {
                        if (targetTable.Columns.TryGetValue(change.OldColumn?.Name.ToLowerInvariant() ?? "", out SqlTableColumn? targetColumn) && targetColumn.IsPrimaryKey)
                        {
                            needsPkRestored = true;
                            break;
                        }
                    }
                }
                else if (change.ChangeType == ColumnChangeType.Modify && change.NewColumn != null)
                {
                    // NewColumn is from target schema (what we want to restore to)
                    // If it was a PK in the target schema, we need to restore it
                    if (change.NewColumn.IsPrimaryKey)
                    {
                        needsPkRestored = true;
                        break;
                    }
                }
                else if (change.ChangeType == ColumnChangeType.Rebuild && change.NewColumn != null)
                {
                    // For Rebuild changes, NewColumn is from target schema (what we want to restore to)
                    // If it was a PK in the target schema, we need to restore it
                    if (change.NewColumn.IsPrimaryKey)
                    {
                        needsPkRestored = true;
                        break;
                    }
                }
            }
            
            if (needsPkRestored)
            {
                // Get all original PK columns from target schema
                if (targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTable))
                {
                    List<SqlTableColumn> originalPkColumns = targetTable.Columns.Values.Where(c => c.IsPrimaryKey).OrderBy(c => c.OrdinalPosition).ToList();
                    if (originalPkColumns.Count > 0)
                    {
                        List<string> pkColumnNames = originalPkColumns.Select(c => c.Name).ToList();
                        string pkConstraintName = $"PK_{tableDiff.TableName}";
                        content.Append(GeneratePrimaryKeys.GenerateAddPrimaryKeyStatement(tableDiff.TableName, pkColumnNames, pkConstraintName, targetTable.Schema));
                    }
                }
            }
        }
        
        return content.ToString().Trim();
    }
}

