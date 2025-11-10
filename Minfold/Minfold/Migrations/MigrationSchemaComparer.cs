using System.Collections.Concurrent;

namespace Minfold;

public static class MigrationSchemaComparer
{
    public static SchemaDiff CompareSchemas(ConcurrentDictionary<string, SqlTable> currentSchema, ConcurrentDictionary<string, SqlTable> targetSchema)
    {
        List<SqlTable> newTables = new List<SqlTable>();
        List<string> droppedTableNames = new List<string>();
        List<TableDiff> modifiedTables = new List<TableDiff>();

        // Find new tables (in target but not in current)
        foreach (KeyValuePair<string, SqlTable> targetTable in targetSchema)
        {
            if (!currentSchema.ContainsKey(targetTable.Key))
            {
                newTables.Add(targetTable.Value);
            }
        }

        // Find dropped tables (in current but not in target)
        foreach (KeyValuePair<string, SqlTable> currentTable in currentSchema)
        {
            if (!targetSchema.ContainsKey(currentTable.Key))
            {
                droppedTableNames.Add(currentTable.Value.Name);
            }
        }

        // Find modified tables (exist in both but have differences)
        foreach (KeyValuePair<string, SqlTable> targetTable in targetSchema)
        {
            if (currentSchema.TryGetValue(targetTable.Key, out SqlTable? currentTable))
            {
                TableDiff? tableDiff = CompareTables(currentTable, targetTable.Value);
                if (tableDiff != null && (tableDiff.ColumnChanges.Count > 0 || tableDiff.ForeignKeyChanges.Count > 0))
                {
                    modifiedTables.Add(tableDiff);
                }
            }
        }

        return new SchemaDiff(newTables, droppedTableNames, modifiedTables);
    }

    private static TableDiff? CompareTables(SqlTable currentTable, SqlTable targetTable)
    {
        List<ColumnChange> columnChanges = new List<ColumnChange>();
        List<ForeignKeyChange> foreignKeyChanges = new List<ForeignKeyChange>();

        // Compare columns
        HashSet<string> processedColumns = new HashSet<string>();

        // Find added and modified columns
        foreach (KeyValuePair<string, SqlTableColumn> targetColumn in targetTable.Columns)
        {
            processedColumns.Add(targetColumn.Key);
            if (currentTable.Columns.TryGetValue(targetColumn.Key, out SqlTableColumn? currentColumn))
            {
                // Column exists in both - check if modified
                if (!AreColumnsEqual(currentColumn, targetColumn.Value))
                {
                    columnChanges.Add(new ColumnChange(ColumnChangeType.Modify, currentColumn, targetColumn.Value));
                }
            }
            else
            {
                // Column added
                columnChanges.Add(new ColumnChange(ColumnChangeType.Add, null, targetColumn.Value));
            }
        }

        // Find dropped columns
        foreach (KeyValuePair<string, SqlTableColumn> currentColumn in currentTable.Columns)
        {
            if (!processedColumns.Contains(currentColumn.Key))
            {
                columnChanges.Add(new ColumnChange(ColumnChangeType.Drop, currentColumn.Value, null));
            }
        }

        // Compare foreign keys
        // Collect all FKs from both tables
        Dictionary<string, SqlForeignKey> currentFks = new Dictionary<string, SqlForeignKey>();
        Dictionary<string, SqlForeignKey> targetFks = new Dictionary<string, SqlForeignKey>();

        foreach (SqlTableColumn column in currentTable.Columns.Values)
        {
            foreach (SqlForeignKey fk in column.ForeignKeys)
            {
                if (!currentFks.ContainsKey(fk.Name))
                {
                    currentFks[fk.Name] = fk;
                }
            }
        }

        foreach (SqlTableColumn column in targetTable.Columns.Values)
        {
            foreach (SqlForeignKey fk in column.ForeignKeys)
            {
                if (!targetFks.ContainsKey(fk.Name))
                {
                    targetFks[fk.Name] = fk;
                }
            }
        }

        // Find added and modified FKs
        foreach (KeyValuePair<string, SqlForeignKey> targetFk in targetFks)
        {
            if (currentFks.TryGetValue(targetFk.Key, out SqlForeignKey? currentFk))
            {
                // FK exists in both - check if modified
                if (!AreForeignKeysEqual(currentFk, targetFk.Value))
                {
                    foreignKeyChanges.Add(new ForeignKeyChange(ForeignKeyChangeType.Modify, currentFk, targetFk.Value));
                }
            }
            else
            {
                // FK added
                foreignKeyChanges.Add(new ForeignKeyChange(ForeignKeyChangeType.Add, null, targetFk.Value));
            }
        }

        // Find dropped FKs
        foreach (KeyValuePair<string, SqlForeignKey> currentFk in currentFks)
        {
            if (!targetFks.ContainsKey(currentFk.Key))
            {
                foreignKeyChanges.Add(new ForeignKeyChange(ForeignKeyChangeType.Drop, currentFk.Value, null));
            }
        }

        if (columnChanges.Count == 0 && foreignKeyChanges.Count == 0)
        {
            return null;
        }

        return new TableDiff(targetTable.Name, columnChanges, foreignKeyChanges);
    }

    private static bool AreColumnsEqual(SqlTableColumn col1, SqlTableColumn col2)
    {
        return col1.Name.Equals(col2.Name, StringComparison.OrdinalIgnoreCase) &&
               col1.IsNullable == col2.IsNullable &&
               col1.IsIdentity == col2.IsIdentity &&
               col1.IsComputed == col2.IsComputed &&
               col1.IsPrimaryKey == col2.IsPrimaryKey &&
               col1.SqlType == col2.SqlType &&
               col1.LengthOrPrecision == col2.LengthOrPrecision &&
               (col1.ComputedSql ?? string.Empty) == (col2.ComputedSql ?? string.Empty);
    }

    private static bool AreForeignKeysEqual(SqlForeignKey fk1, SqlForeignKey fk2)
    {
        return fk1.Name.Equals(fk2.Name, StringComparison.OrdinalIgnoreCase) &&
               fk1.Table.Equals(fk2.Table, StringComparison.OrdinalIgnoreCase) &&
               fk1.Column.Equals(fk2.Column, StringComparison.OrdinalIgnoreCase) &&
               fk1.RefTable.Equals(fk2.RefTable, StringComparison.OrdinalIgnoreCase) &&
               fk1.RefColumn.Equals(fk2.RefColumn, StringComparison.OrdinalIgnoreCase) &&
               fk1.NotEnforced == fk2.NotEnforced &&
               fk1.NotForReplication == fk2.NotForReplication &&
               fk1.DeleteAction == fk2.DeleteAction &&
               fk1.UpdateAction == fk2.UpdateAction;
    }
}

