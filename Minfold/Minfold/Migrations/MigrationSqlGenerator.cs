using System.Text;

namespace Minfold;

public static class MigrationSqlGenerator
{
    public static string GenerateSectionHeader(int phaseNumber, string phaseDescription)
    {
        StringBuilder sb = new StringBuilder();
        sb.AppendLine("-- =============================================");
        sb.AppendLine($"-- Phase {phaseNumber}: {phaseDescription}");
        sb.AppendLine("-- =============================================");
        sb.AppendLine();
        return sb.ToString();
    }

    public static string GenerateCreateTableStatement(SqlTable table)
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"CREATE TABLE [dbo].[{table.Name}]");
        sb.AppendLine("(");

        List<KeyValuePair<string, SqlTableColumn>> orderedColumns = table.Columns.OrderBy(x => x.Value.OrdinalPosition).ToList();

        for (int i = 0; i < orderedColumns.Count; i++)
        {
            KeyValuePair<string, SqlTableColumn> colPair = orderedColumns[i];
            SqlTableColumn col = colPair.Value;
            bool isLast = i == orderedColumns.Count - 1;

            sb.Append("    [");
            sb.Append(col.Name);
            sb.Append("] ");

            // Column type
            if (col.IsComputed && !string.IsNullOrWhiteSpace(col.ComputedSql))
            {
                sb.Append("AS ");
                sb.Append(col.ComputedSql);
            }
            else
            {
                string sqlType = col.SqlType.ToSqlDbType();
                sb.Append(sqlType.ToUpperInvariant());

                // Add length/precision for types that need it
                if (col.LengthOrPrecision.HasValue)
                {
                    if (sqlType.ToLowerInvariant() is "decimal" or "numeric")
                    {
                        // For decimal/numeric, we'd need precision and scale, but we only have one value
                        // This is a limitation - we'd need to query for precision and scale separately
                        sb.Append($"({col.LengthOrPrecision.Value})");
                    }
                    else if (sqlType.ToLowerInvariant() is "varchar" or "nvarchar" or "char" or "nchar" or "varbinary" or "binary")
                    {
                        if (col.LengthOrPrecision.Value == -1)
                        {
                            sb.Append("(MAX)");
                        }
                        else
                        {
                            sb.Append($"({col.LengthOrPrecision.Value})");
                        }
                    }
                    else if (sqlType.ToLowerInvariant() is "datetime2" or "time" or "datetimeoffset")
                    {
                        sb.Append($"({col.LengthOrPrecision.Value})");
                    }
                }

                // Nullability
                if (col.IsNullable)
                {
                    sb.Append(" NULL");
                }
                else
                {
                    sb.Append(" NOT NULL");
                }

                // Identity
                if (col.IsIdentity)
                {
                    sb.Append(" IDENTITY(1,1)");
                }
            }

            if (!isLast)
            {
                sb.Append(",");
            }
            sb.AppendLine();
        }

        // Primary key constraint
        List<SqlTableColumn> pkColumns = orderedColumns.Where(x => x.Value.IsPrimaryKey).Select(x => x.Value).OrderBy(x => x.OrdinalPosition).ToList();
        if (pkColumns.Count > 0)
        {
            sb.AppendLine(",");
            sb.Append("    CONSTRAINT [PK_");
            sb.Append(table.Name);
            sb.Append("] PRIMARY KEY (");
            sb.Append(string.Join(", ", pkColumns.Select(c => $"[{c.Name}]")));
            sb.Append(")");
        }

        sb.AppendLine();
        sb.Append(");");

        return sb.ToString();
    }

    public static string GenerateAddColumnStatement(SqlTableColumn column, string tableName)
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"ALTER TABLE [dbo].[{tableName}] ADD [");
        sb.Append(column.Name);
        sb.Append("] ");

        if (column.IsComputed && !string.IsNullOrWhiteSpace(column.ComputedSql))
        {
            sb.Append("AS ");
            sb.Append(column.ComputedSql);
        }
        else
        {
            string sqlType = column.SqlType.ToSqlDbType();
            sb.Append(sqlType.ToUpperInvariant());

            // Add length/precision for types that need it
            if (column.LengthOrPrecision.HasValue)
            {
                if (sqlType.ToLowerInvariant() is "decimal" or "numeric")
                {
                    sb.Append($"({column.LengthOrPrecision.Value})");
                }
                else if (sqlType.ToLowerInvariant() is "varchar" or "nvarchar" or "char" or "nchar" or "varbinary" or "binary")
                {
                    if (column.LengthOrPrecision.Value == -1)
                    {
                        sb.Append("(MAX)");
                    }
                    else
                    {
                        sb.Append($"({column.LengthOrPrecision.Value})");
                    }
                }
                else if (sqlType.ToLowerInvariant() is "datetime2" or "time" or "datetimeoffset")
                {
                    sb.Append($"({column.LengthOrPrecision.Value})");
                }
            }

            // Nullability
            if (column.IsNullable)
            {
                sb.Append(" NULL");
            }
            else
            {
                sb.Append(" NOT NULL");
            }

            // Identity
            if (column.IsIdentity)
            {
                sb.Append(" IDENTITY(1,1)");
            }
        }

        sb.AppendLine(";");
        return sb.ToString();
    }

    public static string GenerateDropColumnStatement(string columnName, string tableName)
    {
        // SQL Server requires dropping default constraints before dropping columns
        // Use a GUID-based variable name to avoid conflicts when multiple columns are dropped in the same batch
        string varSuffix = Guid.NewGuid().ToString("N"); // GUID without dashes
        
        StringBuilder sb = new StringBuilder();
        sb.AppendLine($"DECLARE @constraintName_{varSuffix} NVARCHAR(128);");
        sb.AppendLine($"SELECT @constraintName_{varSuffix} = name FROM sys.default_constraints ");
        sb.AppendLine($"WHERE parent_object_id = OBJECT_ID('[dbo].[{tableName}]') ");
        sb.AppendLine($"AND parent_column_id = COLUMNPROPERTY(OBJECT_ID('[dbo].[{tableName}]'), '{columnName}', 'ColumnId');");
        sb.AppendLine($"IF @constraintName_{varSuffix} IS NOT NULL");
        sb.AppendLine($"    EXEC('ALTER TABLE [dbo].[{tableName}] DROP CONSTRAINT [' + @constraintName_{varSuffix} + ']');");
        sb.AppendLine($"ALTER TABLE [dbo].[{tableName}] DROP COLUMN [{columnName}];");
        return sb.ToString();
    }

    public static string GenerateAlterColumnStatement(SqlTableColumn oldColumn, SqlTableColumn newColumn, string tableName)
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"ALTER TABLE [dbo].[{tableName}] ALTER COLUMN [");
        sb.Append(newColumn.Name);
        sb.Append("] ");

        if (newColumn.IsComputed && !string.IsNullOrWhiteSpace(newColumn.ComputedSql))
        {
            // Computed columns need to be dropped and recreated
            // This is a limitation - we'll handle it as DROP + ADD
            return string.Empty; // Signal that this needs special handling
        }

        string sqlType = newColumn.SqlType.ToSqlDbType();
        sb.Append(sqlType.ToUpperInvariant());

        // Add length/precision for types that need it
        if (newColumn.LengthOrPrecision.HasValue)
        {
            if (sqlType.ToLowerInvariant() is "decimal" or "numeric")
            {
                sb.Append($"({newColumn.LengthOrPrecision.Value})");
            }
            else if (sqlType.ToLowerInvariant() is "varchar" or "nvarchar" or "char" or "nchar" or "varbinary" or "binary")
            {
                if (newColumn.LengthOrPrecision.Value == -1)
                {
                    sb.Append("(MAX)");
                }
                else
                {
                    sb.Append($"({newColumn.LengthOrPrecision.Value})");
                }
            }
            else if (sqlType.ToLowerInvariant() is "datetime2" or "time" or "datetimeoffset")
            {
                sb.Append($"({newColumn.LengthOrPrecision.Value})");
            }
        }

        // Nullability
        if (newColumn.IsNullable)
        {
            sb.Append(" NULL");
        }
        else
        {
            sb.Append(" NOT NULL");
        }

        // Note: Identity columns cannot be altered directly - would need to recreate table
        // We'll handle this as a limitation for now

        sb.AppendLine(";");
        return sb.ToString();
    }

    public static string GenerateColumnModifications(TableDiff tableDiff)
    {
        StringBuilder sb = new StringBuilder();

        // Order operations: DROP COLUMN first, then ALTER COLUMN, then ADD COLUMN
        // This ensures we don't have dependency issues

        // Drop columns
        foreach (ColumnChange change in tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Drop))
        {
            if (change.OldColumn != null)
            {
                sb.AppendLine(GenerateDropColumnStatement(change.OldColumn.Name, tableDiff.TableName));
            }
        }

        // Alter columns (excluding computed columns which need special handling)
        foreach (ColumnChange change in tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Modify))
        {
            if (change.OldColumn != null && change.NewColumn != null)
            {
                // Check if it's a computed column change - these need DROP + ADD
                if (change.OldColumn.IsComputed || change.NewColumn.IsComputed)
                {
                    // Drop old computed column
                    if (change.OldColumn.IsComputed)
                    {
                        sb.AppendLine(GenerateDropColumnStatement(change.OldColumn.Name, tableDiff.TableName));
                    }
                    // Add new computed column
                    if (change.NewColumn.IsComputed)
                    {
                        sb.Append(GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName));
                    }
                }
                else
                {
                    string alterSql = GenerateAlterColumnStatement(change.OldColumn, change.NewColumn, tableDiff.TableName);
                    if (!string.IsNullOrEmpty(alterSql))
                    {
                        sb.Append(alterSql);
                    }
                }
            }
        }

        // Add columns
        foreach (ColumnChange change in tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Add))
        {
            if (change.NewColumn != null)
            {
                sb.Append(GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName));
            }
        }

        return sb.ToString();
    }

    public static string GenerateForeignKeyStatement(List<SqlForeignKey> fkGroup, Dictionary<string, SqlTable> allTables)
    {
        if (fkGroup.Count == 0)
        {
            return string.Empty;
        }

        SqlForeignKey firstFk = fkGroup[0];
        StringBuilder sb = new StringBuilder();

        sb.Append("ALTER TABLE [dbo].[");
        sb.Append(firstFk.Table);
        sb.Append("] WITH ");
        sb.Append(firstFk.NotEnforced ? "NOCHECK" : "CHECK");
        sb.Append(" ADD CONSTRAINT [");
        sb.Append(firstFk.Name);
        sb.Append("] FOREIGN KEY(");

        // Multi-column FK support
        List<string> columns = fkGroup.OrderBy(f => f.Column).Select(f => $"[{f.Column}]").ToList();
        List<string> refColumns = fkGroup.OrderBy(f => f.Column).Select(f => $"[{f.RefColumn}]").ToList();

        sb.Append(string.Join(", ", columns));
        sb.Append(") REFERENCES [dbo].[");
        sb.Append(firstFk.RefTable);
        sb.Append("](");
        sb.Append(string.Join(", ", refColumns));
        sb.Append(")");

        if (firstFk.NotForReplication)
        {
            sb.Append(" NOT FOR REPLICATION");
        }

        // Delete action
        switch (firstFk.DeleteAction)
        {
            case 1:
                sb.Append(" ON DELETE CASCADE");
                break;
            case 2:
                sb.Append(" ON DELETE SET NULL");
                break;
            case 3:
                sb.Append(" ON DELETE SET DEFAULT");
                break;
        }

        // Update action
        switch (firstFk.UpdateAction)
        {
            case 1:
                sb.Append(" ON UPDATE CASCADE");
                break;
            case 2:
                sb.Append(" ON UPDATE SET NULL");
                break;
            case 3:
                sb.Append(" ON UPDATE SET DEFAULT");
                break;
        }

        sb.AppendLine(";");

        // Enable constraint if it was enforced
        if (!firstFk.NotEnforced)
        {
            sb.Append("ALTER TABLE [dbo].[");
            sb.Append(firstFk.Table);
            sb.Append("] CHECK CONSTRAINT [");
            sb.Append(firstFk.Name);
            sb.AppendLine("];");
        }

        return sb.ToString();
    }

    public static string GenerateDropForeignKeyStatement(SqlForeignKey fk)
    {
        return $"ALTER TABLE [dbo].[{fk.Table}] DROP CONSTRAINT [{fk.Name}];";
    }
}

