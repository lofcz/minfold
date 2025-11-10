using System.Collections.Concurrent;
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
                    // Use actual seed and increment values from scripted SQL, default to (1,1) if not available
                    long seed = col.IdentitySeed ?? 1;
                    long increment = col.IdentityIncrement ?? 1;
                    sb.Append($" IDENTITY({seed},{increment})");
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
                // Use actual seed and increment values from scripted SQL, default to (1,1) if not available
                long seed = column.IdentitySeed ?? 1;
                long increment = column.IdentityIncrement ?? 1;
                sb.Append($" IDENTITY({seed},{increment})");
            }
        }

        sb.AppendLine(";");
        return sb.ToString();
    }

    /// <summary>
    /// Generates SQL to drop a default constraint for a column, if one exists.
    /// Returns empty string if no constraint exists.
    /// </summary>
    public static string GenerateDropDefaultConstraintStatement(string columnName, string tableName, string? variableSuffix = null)
    {
        // Use provided suffix or generate a GUID-based one to avoid conflicts
        string varSuffix = variableSuffix ?? Guid.NewGuid().ToString("N");
        
        StringBuilder sb = new StringBuilder();
        sb.AppendLine($"DECLARE @constraintName_{varSuffix} NVARCHAR(128);");
        sb.AppendLine($"SELECT @constraintName_{varSuffix} = name FROM sys.default_constraints ");
        sb.AppendLine($"WHERE parent_object_id = OBJECT_ID('[dbo].[{tableName}]') ");
        sb.AppendLine($"AND parent_column_id = COLUMNPROPERTY(OBJECT_ID('[dbo].[{tableName}]'), '{columnName}', 'ColumnId');");
        sb.AppendLine($"IF @constraintName_{varSuffix} IS NOT NULL");
        sb.AppendLine($"    EXEC('ALTER TABLE [dbo].[{tableName}] DROP CONSTRAINT [' + @constraintName_{varSuffix} + ']');");
        return sb.ToString();
    }

    public static string GenerateDropColumnStatement(string columnName, string tableName)
    {
        // SQL Server requires dropping default constraints before dropping columns
        // Use a GUID-based variable name to avoid conflicts when multiple columns are dropped in the same batch
        string varSuffix = Guid.NewGuid().ToString("N"); // GUID without dashes
        
        StringBuilder sb = new StringBuilder();
        sb.Append(GenerateDropDefaultConstraintStatement(columnName, tableName, varSuffix));
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

    public static string GenerateColumnModifications(TableDiff tableDiff, ConcurrentDictionary<string, SqlTable>? currentSchema = null)
    {
        StringBuilder sb = new StringBuilder();

        // Order operations: DROP INDEXES (on columns being dropped), DROP COLUMN, then ALTER COLUMN, then ADD COLUMN
        // This ensures we don't have dependency issues

        // Drop indexes that reference columns being dropped (must happen before dropping columns)
        if (currentSchema != null)
        {
            if (currentSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? currentTable))
            {
                HashSet<string> columnsBeingDropped = new HashSet<string>(tableDiff.ColumnChanges
                    .Where(c => c.ChangeType == ColumnChangeType.Drop && c.OldColumn != null)
                    .Select(c => c.OldColumn!.Name), StringComparer.OrdinalIgnoreCase);

                foreach (SqlIndex index in currentTable.Indexes)
                {
                    // If any column in this index is being dropped, drop the index
                    if (index.Columns.Any(col => columnsBeingDropped.Contains(col)))
                    {
                        sb.AppendLine(GenerateDropIndexStatement(index));
                    }
                }
            }
        }

        // Drop columns
        foreach (ColumnChange change in tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Drop))
        {
            if (change.OldColumn != null)
            {
                sb.AppendLine(GenerateDropColumnStatement(change.OldColumn.Name, tableDiff.TableName));
            }
        }

        // Alter columns (excluding computed columns, identity changes, and PK changes which need special handling)
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
                // Check if identity property changed - SQL Server requires DROP + ADD
                else if (change.OldColumn.IsIdentity != change.NewColumn.IsIdentity)
                {
                    // Drop old column
                    sb.AppendLine(GenerateDropColumnStatement(change.OldColumn.Name, tableDiff.TableName));
                    // Add new column with new identity setting
                    sb.Append(GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName));
                }
                // Check if only PK property changed (and no other significant changes) - handle via PK constraint changes
                // Note: PK changes are also handled via FK changes, but we need to ensure column is recreated if needed
                else if (change.OldColumn.IsPrimaryKey != change.NewColumn.IsPrimaryKey && 
                         change.OldColumn.IsIdentity == change.NewColumn.IsIdentity &&
                         change.OldColumn.IsComputed == change.NewColumn.IsComputed &&
                         change.OldColumn.SqlType == change.NewColumn.SqlType &&
                         change.OldColumn.IsNullable == change.NewColumn.IsNullable)
                {
                    // Only PK changed, no other changes - PK constraint will be handled separately
                    // But we still need to handle the column if there are other changes
                    string alterSql = GenerateAlterColumnStatement(change.OldColumn, change.NewColumn, tableDiff.TableName);
                    if (!string.IsNullOrEmpty(alterSql))
                    {
                        sb.Append(alterSql);
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

    public static string GenerateDropPrimaryKeyStatement(string tableName, string constraintName)
    {
        return $"ALTER TABLE [dbo].[{tableName}] DROP CONSTRAINT [{constraintName}];";
    }

    public static string GenerateAddPrimaryKeyStatement(string tableName, List<string> columnNames, string constraintName)
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"ALTER TABLE [dbo].[{tableName}] ADD CONSTRAINT [{constraintName}] PRIMARY KEY (");
        sb.Append(string.Join(", ", columnNames.Select(c => $"[{c}]")));
        sb.AppendLine(");");
        return sb.ToString();
    }

    public static string GenerateDropIndexStatement(SqlIndex index)
    {
        // Use dynamic SQL to check if index exists before dropping
        return $"""
            IF EXISTS (SELECT * FROM sys.indexes WHERE name = '{index.Name}' AND object_id = OBJECT_ID('[dbo].[{index.Table}]'))
            BEGIN
                DROP INDEX [{index.Name}] ON [dbo].[{index.Table}];
            END
            """;
    }

    public static string GenerateCreateIndexStatement(SqlIndex index)
    {
        StringBuilder sb = new StringBuilder();
        // Use dynamic SQL to check if index doesn't exist before creating
        sb.AppendLine($"""
            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '{index.Name}' AND object_id = OBJECT_ID('[dbo].[{index.Table}]'))
            BEGIN
                CREATE {(index.IsUnique ? "UNIQUE " : "")}NONCLUSTERED INDEX [{index.Name}] ON [dbo].[{index.Table}] ({string.Join(", ", index.Columns.Select(c => $"[{c}]"))});
            END
            """);
        return sb.ToString();
    }

    public static string GenerateCreateSequenceStatement(SqlSequence sequence)
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"CREATE SEQUENCE [dbo].[{sequence.Name}] AS {sequence.DataType}");
        
        if (sequence.StartValue.HasValue)
        {
            sb.Append($" START WITH {sequence.StartValue.Value}");
        }
        
        if (sequence.Increment.HasValue)
        {
            sb.Append($" INCREMENT BY {sequence.Increment.Value}");
        }
        
        if (sequence.MinValue.HasValue)
        {
            sb.Append($" MINVALUE {sequence.MinValue.Value}");
        }
        else
        {
            sb.Append(" NO MINVALUE");
        }
        
        if (sequence.MaxValue.HasValue)
        {
            sb.Append($" MAXVALUE {sequence.MaxValue.Value}");
        }
        else
        {
            sb.Append(" NO MAXVALUE");
        }
        
        if (sequence.Cycle)
        {
            sb.Append(" CYCLE");
        }
        else
        {
            sb.Append(" NO CYCLE");
        }
        
        if (sequence.CacheSize.HasValue)
        {
            sb.Append($" CACHE {sequence.CacheSize.Value}");
        }
        else
        {
            sb.Append(" NO CACHE");
        }
        
        sb.AppendLine(";");
        return sb.ToString();
    }

    public static string GenerateDropSequenceStatement(string sequenceName)
    {
        // Use dynamic SQL to check if sequence exists before dropping
        return $"""
            IF EXISTS (SELECT * FROM sys.sequences WHERE name = '{sequenceName}' AND schema_id = SCHEMA_ID('dbo'))
            BEGIN
                DROP SEQUENCE [dbo].[{sequenceName}];
            END
            """;
    }

    public static string GenerateAlterSequenceStatement(SqlSequence oldSequence, SqlSequence newSequence)
    {
        // SQL Server doesn't support ALTER SEQUENCE for all properties, so we'll use DROP + CREATE
        // But for incremental changes, we can try ALTER for some properties
        // For simplicity and reliability, we'll use DROP + CREATE
        StringBuilder sb = new StringBuilder();
        sb.AppendLine(GenerateDropSequenceStatement(oldSequence.Name));
        sb.Append(GenerateCreateSequenceStatement(newSequence));
        return sb.ToString();
    }

    public static string GenerateCreateProcedureStatement(SqlStoredProcedure procedure)
    {
        // CREATE PROCEDURE must be the first statement in a batch, so we use DROP IF EXISTS + GO + CREATE pattern
        // This ensures idempotency and proper batching
        return $"""
            GO
            IF EXISTS (SELECT * FROM sys.procedures WHERE name = '{procedure.Name}' AND schema_id = SCHEMA_ID('dbo'))
                DROP PROCEDURE [dbo].[{procedure.Name}];
            GO
            {procedure.Definition}
            GO
            """;
    }

    public static string GenerateDropProcedureStatement(string procedureName)
    {
        // DROP PROCEDURE can be in a batch, but we add GO for consistency and to ensure proper batching
        return $"""
            GO
            IF EXISTS (SELECT * FROM sys.procedures WHERE name = '{procedureName}' AND schema_id = SCHEMA_ID('dbo'))
                DROP PROCEDURE [dbo].[{procedureName}];
            GO
            """;
    }
}

