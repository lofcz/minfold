using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Down;

public static class GenerateDownPhase6RecreateTables
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable> targetSchema)
    {
        StringBuilder content = new StringBuilder();
        
        // Recreate dropped tables (in forward order, so dependencies are created first)
        if (diff.DroppedTableNames.Count > 0)
        {
            MigrationLogger.Log($"\n=== Recreating dropped tables: {string.Join(", ", diff.DroppedTableNames)} ===");
        }
        foreach (string droppedTableName in diff.DroppedTableNames)
        {
            // Find the table schema from target schema (before migration was applied)
            if (targetSchema.TryGetValue(droppedTableName.ToLowerInvariant(), out SqlTable? droppedTable))
            {
                // Generate CREATE TABLE statement
                string createTableSql = GenerateTables.GenerateCreateTableStatement(droppedTable);
                content.AppendLine(createTableSql);
                content.AppendLine();
                
                // Generate FK constraints for the recreated table
                HashSet<string> processedFksDown1 = new HashSet<string>();
                foreach (SqlTableColumn column in droppedTable.Columns.Values)
                {
                    foreach (SqlForeignKey fk in column.ForeignKeys)
                    {
                        if (!processedFksDown1.Contains(fk.Name))
                        {
                            // Group multi-column FKs
                            List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { fk };
                            foreach (SqlTableColumn otherColumn in droppedTable.Columns.Values)
                            {
                                foreach (SqlForeignKey otherFk in otherColumn.ForeignKeys)
                                {
                                    if (otherFk.Name == fk.Name && otherFk.Table == fk.Table && otherFk != fk)
                                    {
                                        fkGroup.Add(otherFk);
                                    }
                                }
                            }
                            string fkSql = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, new Dictionary<string, SqlTable>(targetSchema));
                            content.Append(fkSql);
                            content.AppendLine();
                            processedFksDown1.Add(fk.Name);
                        }
                    }
                }
            }
        }

        // Recreate new tables (tables that were dropped by the migration and need to be restored)
        // In downDiff, NewTables = tables in targetSchema (before migration) but not in currentSchema (after migration)
        // These are tables that were DROPPED by the migration, so we need to RECREATE them in the down script
        if (diff.NewTables.Count > 0)
        {
            MigrationLogger.Log($"\n=== Recreating new tables (were dropped by migration): {string.Join(", ", diff.NewTables.Select(t => t.Name))} ===");
        }
        foreach (SqlTable newTable in diff.NewTables.OrderBy(t => t.Name))
        {
            // Generate CREATE TABLE statement
            string createTableSql = GenerateTables.GenerateCreateTableStatement(newTable);
            content.AppendLine(createTableSql);
            content.AppendLine();
            
            // Generate FK constraints for the recreated table
            HashSet<string> processedFksDown2 = new HashSet<string>();
            foreach (SqlTableColumn column in newTable.Columns.Values)
            {
                foreach (SqlForeignKey fk in column.ForeignKeys)
                {
                    if (!processedFksDown2.Contains(fk.Name))
                    {
                        // Group multi-column FKs
                        List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { fk };
                        foreach (SqlTableColumn otherColumn in newTable.Columns.Values)
                        {
                            foreach (SqlForeignKey otherFk in otherColumn.ForeignKeys)
                            {
                                if (otherFk.Name == fk.Name && otherFk.Table == fk.Table && otherFk != fk)
                                {
                                    fkGroup.Add(otherFk);
                                }
                            }
                        }
                        string fkSql = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, new Dictionary<string, SqlTable>(targetSchema));
                        content.Append(fkSql);
                        content.AppendLine();
                        processedFksDown2.Add(fk.Name);
                    }
                }
            }
        }
        
        return content.ToString().Trim();
    }
}

