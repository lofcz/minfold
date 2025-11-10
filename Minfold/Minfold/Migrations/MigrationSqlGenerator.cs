using System.Collections.Concurrent;
using System.Security.Cryptography;
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
        sb.Append($"CREATE TABLE [{table.Schema}].[{table.Name}]");
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

                // Default constraint (if exists)
                // Note: When creating tables, we can use the existing constraint name if available
                // since the table doesn't exist yet, so there's no conflict
                if (!string.IsNullOrWhiteSpace(col.DefaultConstraintValue))
                {
                    string constraintName = !string.IsNullOrWhiteSpace(col.DefaultConstraintName)
                        ? col.DefaultConstraintName
                        : $"DF_{table.Name}_{col.Name}_{GenerateDeterministicSuffix(table.Name, col.Name, NormalizeDefaultConstraintValue(col.DefaultConstraintValue), "create")}";
                    // Normalize the default value (remove outer parentheses that SQL Server adds)
                    string normalizedValue = NormalizeDefaultConstraintValue(col.DefaultConstraintValue);
                    sb.Append($" CONSTRAINT [{constraintName}] DEFAULT {normalizedValue}");
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

    /// <summary>
    /// Normalizes a default constraint value by removing outer parentheses.
    /// SQL Server stores default constraint values with parentheses (e.g., "((0))"),
    /// but we want to use the normalized value in generated SQL.
    /// </summary>
    private static string NormalizeDefaultConstraintValue(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return value;
        
        string normalized = value.Trim();
        // Remove outer balanced parentheses
        while (normalized.StartsWith('(') && normalized.EndsWith(')'))
        {
            // Check if it's balanced parentheses
            int depth = 0;
            bool isBalanced = true;
            for (int i = 0; i < normalized.Length; i++)
            {
                if (normalized[i] == '(') depth++;
                else if (normalized[i] == ')') depth--;
                // If depth reaches 0 before the end, it's not fully wrapped
                if (depth == 0 && i < normalized.Length - 1)
                {
                    isBalanced = false;
                    break;
                }
            }
            if (isBalanced && depth == 0)
            {
                normalized = normalized.Substring(1, normalized.Length - 2).Trim();
            }
            else
            {
                break;
            }
        }
        return normalized;
    }

    /// <summary>
    /// Generates a deterministic suffix from input strings using SHA256 hashing.
    /// Same inputs will always produce the same suffix, ensuring idempotent migration generation.
    /// </summary>
    /// <param name="inputs">Input strings to hash (e.g., table name, column name, default value, operation context)</param>
    /// <returns>8-character hexadecimal suffix derived from the hash</returns>
    private static string GenerateDeterministicSuffix(params string[] inputs)
    {
        if (inputs == null || inputs.Length == 0)
        {
            throw new ArgumentException("At least one input is required", nameof(inputs));
        }

        // Normalize inputs: convert to lowercase and join with a delimiter
        string normalizedInput = string.Join("|", inputs.Select(s => (s ?? string.Empty).ToLowerInvariant()));
        
        // Compute SHA256 hash
        byte[] hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(normalizedInput));
        
        // Take first 8 hex characters (32 bits) for readability
        return Convert.ToHexString(hashBytes).Substring(0, 8).ToLowerInvariant();
    }

    /// <summary>
    /// Generates a default value for a NOT NULL column based on its SQL type.
    /// This is used when adding NOT NULL columns to tables that may have data.
    /// </summary>
    private static string GetDefaultValueForType(SqlDbTypeExt sqlType)
    {
        return sqlType switch
        {
            SqlDbTypeExt.Bit => "0",
            SqlDbTypeExt.TinyInt => "0",
            SqlDbTypeExt.SmallInt => "0",
            SqlDbTypeExt.Int => "0",
            SqlDbTypeExt.BigInt => "0",
            SqlDbTypeExt.Real => "0.0",
            SqlDbTypeExt.Float => "0.0",
            SqlDbTypeExt.Decimal => "0",
            SqlDbTypeExt.Numeric => "0",
            SqlDbTypeExt.Money => "0",
            SqlDbTypeExt.SmallMoney => "0",
            SqlDbTypeExt.Char => "''",
            SqlDbTypeExt.VarChar => "''",
            SqlDbTypeExt.NChar => "N''",
            SqlDbTypeExt.NVarChar => "N''",
            SqlDbTypeExt.Text => "''",
            SqlDbTypeExt.NText => "N''",
            SqlDbTypeExt.Binary => "0x00",
            SqlDbTypeExt.VarBinary => "0x00",
            SqlDbTypeExt.Image => "0x00",
            SqlDbTypeExt.Date => "CAST('1900-01-01' AS DATE)",
            SqlDbTypeExt.Time => "CAST('00:00:00' AS TIME)",
            SqlDbTypeExt.DateTime => "CAST('1900-01-01 00:00:00' AS DATETIME)",
            SqlDbTypeExt.DateTime2 => "CAST('1900-01-01 00:00:00' AS DATETIME2)",
            SqlDbTypeExt.DateTimeOffset => "CAST('1900-01-01 00:00:00' AS DATETIMEOFFSET)",
            SqlDbTypeExt.SmallDateTime => "CAST('1900-01-01 00:00:00' AS SMALLDATETIME)",
            SqlDbTypeExt.Timestamp => "DEFAULT", // Timestamp is auto-generated
            SqlDbTypeExt.UniqueIdentifier => "NEWID()",
            _ => "NULL" // Fallback - but this shouldn't be used for NOT NULL columns
        };
    }

    public static string GenerateAddColumnStatement(SqlTableColumn column, string tableName, string schema = "dbo", bool tableMayHaveData = false)
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"ALTER TABLE [{schema}].[{tableName}] ADD [");
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
                // For NOT NULL columns, we need to add a DEFAULT constraint if:
                // 1. The table may have data (required by SQL Server for NOT NULL columns)
                // 2. OR the column already has a default constraint value (preserve existing defaults)
                // SQL Server requires either NULL, DEFAULT, IDENTITY, or empty table for NOT NULL columns
                bool needsDefault = (tableMayHaveData || !string.IsNullOrWhiteSpace(column.DefaultConstraintValue)) && !column.IsIdentity;
                
                if (needsDefault)
                {
                    // Use existing default constraint if available (preserves user-defined defaults)
                    // Otherwise, generate a default value based on the column type
                    string defaultValue;
                    string constraintName;
                    
                    if (!string.IsNullOrWhiteSpace(column.DefaultConstraintValue))
                    {
                        // Column already has a default constraint - preserve the value but generate a new constraint name
                        // to avoid conflicts when adding the column (the constraint may already exist in the database)
                        // Normalize the value by removing outer parentheses if present (SQL Server stores them with parentheses)
                        defaultValue = NormalizeDefaultConstraintValue(column.DefaultConstraintValue);
                        // Always generate a new constraint name to avoid conflicts
                        constraintName = $"DF_{tableName}_{column.Name}_{GenerateDeterministicSuffix(tableName, column.Name, defaultValue, "add")}";
                    }
                    else
                    {
                        // No existing default constraint - generate one based on type
                        defaultValue = GetDefaultValueForType(column.SqlType);
                        constraintName = $"DF_{tableName}_{column.Name}_{GenerateDeterministicSuffix(tableName, column.Name, defaultValue, "add")}";
                    }
                    
                    // Add a DEFAULT constraint that will persist in the database schema
                    // This constraint:
                    // 1. Allows adding the NOT NULL column to a table with existing data (required by SQL Server)
                    // 2. Provides a default value for future INSERT statements (desired behavior)
                    // 3. Becomes part of the permanent schema (the constraint remains after migration)
                    // Note: The constraint name includes a GUID to ensure uniqueness if not preserving existing
                    sb.Append($" NOT NULL CONSTRAINT [{constraintName}] DEFAULT {defaultValue}");
                }
                else
                {
                    sb.Append(" NOT NULL");
                }
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
    public static string GenerateDropDefaultConstraintStatement(string columnName, string tableName, string? variableSuffix = null, string schema = "dbo")
    {
        // Use provided suffix or generate a deterministic one to avoid conflicts
        string varSuffix = variableSuffix ?? GenerateDeterministicSuffix(schema, tableName, columnName, "dropdefault");
        
        StringBuilder sb = new StringBuilder();
        sb.AppendLine($"DECLARE @constraintName_{varSuffix} NVARCHAR(128);");
        sb.AppendLine($"SELECT @constraintName_{varSuffix} = name FROM sys.default_constraints ");
        sb.AppendLine($"WHERE parent_object_id = OBJECT_ID('[{schema}].[{tableName}]') ");
        sb.AppendLine($"AND parent_column_id = COLUMNPROPERTY(OBJECT_ID('[{schema}].[{tableName}]'), '{columnName}', 'ColumnId');");
        sb.AppendLine($"IF @constraintName_{varSuffix} IS NOT NULL");
        sb.AppendLine($"    EXEC('ALTER TABLE [{schema}].[{tableName}] DROP CONSTRAINT [' + @constraintName_{varSuffix} + ']');");
        return sb.ToString();
    }

    public static string GenerateDropColumnStatement(string columnName, string tableName, string schema = "dbo")
    {
        // SQL Server requires dropping default constraints before dropping columns
        // Use a deterministic variable name to avoid conflicts when multiple columns are dropped in the same batch
        string varSuffix = GenerateDeterministicSuffix(schema, tableName, columnName, "dropcolumn");
        
        StringBuilder sb = new StringBuilder();
        sb.Append(GenerateDropDefaultConstraintStatement(columnName, tableName, varSuffix, schema));
        sb.AppendLine($"ALTER TABLE [{schema}].[{tableName}] DROP COLUMN [{columnName}];");
        return sb.ToString();
    }

    public static string GenerateAlterColumnStatement(SqlTableColumn oldColumn, SqlTableColumn newColumn, string tableName, string schema = "dbo")
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"ALTER TABLE [{schema}].[{tableName}] ALTER COLUMN [");
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

    private static HashSet<string> CalculateRemainingColumns(TableDiff tableDiff, ConcurrentDictionary<string, SqlTable>? currentSchema)
    {
        HashSet<string> remainingColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        
        // Start with all current columns
        if (currentSchema != null && currentSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? currentTable))
        {
            foreach (string columnName in currentTable.Columns.Keys)
            {
                remainingColumns.Add(columnName);
            }
        }
        
        // Remove columns being dropped
        foreach (ColumnChange change in tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Drop && c.OldColumn != null))
        {
            remainingColumns.Remove(change.OldColumn!.Name);
        }
        
        // Remove columns being modified (they'll be re-added if needed)
        foreach (ColumnChange change in tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Modify && c.OldColumn != null))
        {
            // Only remove if it's a DROP+ADD scenario (not ALTER COLUMN)
            if (change.OldColumn != null && change.NewColumn != null)
            {
                bool requiresDropAdd = (change.OldColumn.IsComputed || change.NewColumn.IsComputed) ||
                                      (change.OldColumn.IsIdentity != change.NewColumn.IsIdentity);
                
                if (requiresDropAdd)
                {
                    remainingColumns.Remove(change.OldColumn.Name);
                }
            }
        }
        
        // Add columns being added
        foreach (ColumnChange change in tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Add && c.NewColumn != null))
        {
            remainingColumns.Add(change.NewColumn!.Name);
        }
        
        // Add columns being modified (they'll be re-added)
        foreach (ColumnChange change in tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Modify && c.NewColumn != null))
        {
            if (change.OldColumn != null && change.NewColumn != null)
            {
                bool requiresDropAdd = (change.OldColumn.IsComputed || change.NewColumn.IsComputed) ||
                                      (change.OldColumn.IsIdentity != change.NewColumn.IsIdentity);
                
                if (requiresDropAdd)
                {
                    remainingColumns.Add(change.NewColumn.Name);
                }
            }
        }
        
        return remainingColumns;
    }
    
    private static bool WouldReduceToZeroColumns(TableDiff tableDiff, ConcurrentDictionary<string, SqlTable>? currentSchema)
    {
        HashSet<string> remainingColumns = CalculateRemainingColumns(tableDiff, currentSchema);
        return remainingColumns.Count == 0;
    }
    
    private static bool IsOnlyColumnInTable(string columnName, TableDiff tableDiff, ConcurrentDictionary<string, SqlTable>? currentSchema)
    {
        if (currentSchema == null)
        {
            return false;
        }
        
        if (!currentSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? currentTable))
        {
            return false;
        }
        
        // Count columns that will remain after drops (excluding this column if it's being modified)
        HashSet<string> remainingColumns = CalculateRemainingColumns(tableDiff, currentSchema);
        
        // If this column is being modified with DROP+ADD, temporarily exclude it from count
        ColumnChange? modifyChange = tableDiff.ColumnChanges.FirstOrDefault(c => 
            c.ChangeType == ColumnChangeType.Modify && 
            c.OldColumn != null && 
            string.Equals(c.OldColumn.Name, columnName, StringComparison.OrdinalIgnoreCase));
        
        if (modifyChange != null && modifyChange.OldColumn != null && modifyChange.NewColumn != null)
        {
            bool requiresDropAdd = (modifyChange.OldColumn.IsComputed || modifyChange.NewColumn.IsComputed) ||
                                  (modifyChange.OldColumn.IsIdentity != modifyChange.NewColumn.IsIdentity);
            
            if (requiresDropAdd)
            {
                // Temporarily remove this column from count to check if it's the only one
                HashSet<string> tempRemaining = new HashSet<string>(remainingColumns, StringComparer.OrdinalIgnoreCase);
                tempRemaining.Remove(columnName);
                return tempRemaining.Count == 0;
            }
        }
        
        return remainingColumns.Count == 1 && remainingColumns.Contains(columnName, StringComparer.OrdinalIgnoreCase);
    }
    
    public static string GenerateRenameColumnStatement(string tableName, string oldColumnName, string newColumnName, string schema = "dbo")
    {
        return $"EXEC sp_rename '[{schema}].[{tableName}].[{oldColumnName}]', '{newColumnName}', 'COLUMN';";
    }
    
    public static string GenerateSafeColumnDropAndAdd(
        SqlTableColumn oldColumn, 
        SqlTableColumn newColumn, 
        string tableName, 
        string schema,
        TableDiff tableDiff,
        ConcurrentDictionary<string, SqlTable>? currentSchema)
    {
        StringBuilder sb = new StringBuilder();
        
        // Check if this is the only column by directly checking current schema
        // This is more reliable than using CalculateRemainingColumns with a partial TableDiff
        bool isOnlyColumn = false;
        if (currentSchema != null && currentSchema.TryGetValue(tableName.ToLowerInvariant(), out SqlTable? currentTable))
        {
            // Count non-computed columns (computed columns don't count as "data columns" for SQL Server's requirement)
            int dataColumnCount = currentTable.Columns.Values.Count(c => !c.IsComputed);
            // Check if the column we're dropping is the only data column
            isOnlyColumn = dataColumnCount == 1 && 
                          currentTable.Columns.TryGetValue(oldColumn.Name.ToLowerInvariant(), out SqlTableColumn? col) &&
                          !col.IsComputed;
        }
        
        // Also check using TableDiff logic (for up script scenarios where we have full context)
        // Only use this if TableDiff has multiple changes (full context), otherwise rely on direct check
        bool isOnlyColumnByDiff = false;
        bool wouldReduceToZero = false;
        
        // Only use diff-based checks if we have full context (multiple changes or non-modify changes)
        // For single-change reversed diffs (like in down scripts), rely on direct schema check
        if (tableDiff.ColumnChanges.Count > 1 || tableDiff.ColumnChanges.Any(c => c.ChangeType != ColumnChangeType.Modify))
        {
            isOnlyColumnByDiff = IsOnlyColumnInTable(oldColumn.Name, tableDiff, currentSchema);
            wouldReduceToZero = WouldReduceToZeroColumns(tableDiff, currentSchema);
        }
        // For single-change scenarios, verify we're not incorrectly using safe wrapper
        else if (currentSchema != null && currentSchema.TryGetValue(tableName.ToLowerInvariant(), out SqlTable? tableForCheck))
        {
            // Double-check: if table has multiple columns, don't use safe wrapper
            int totalDataColumns = tableForCheck.Columns.Values.Count(c => !c.IsComputed);
            if (totalDataColumns > 1)
            {
                // Table has multiple columns, safe to use normal DROP+ADD
                isOnlyColumnByDiff = false;
                wouldReduceToZero = false;
            }
        }
        
        bool sameName = string.Equals(oldColumn.Name, newColumn.Name, StringComparison.OrdinalIgnoreCase);
        
        // Check if table exists in currentSchema (may have data)
        bool tableMayHaveData = currentSchema != null && currentSchema.ContainsKey(tableName.ToLowerInvariant());
        
        // Use direct check if available, otherwise fall back to diff-based check
        bool needsSafeHandling = isOnlyColumn || isOnlyColumnByDiff || wouldReduceToZero;
        
        if (needsSafeHandling)
        {
            // Unsafe scenario: need to add first, then drop, then rename if needed
            if (sameName)
            {
                // Same name: add with temporary name, drop old, rename
                string tempColumnName = $"{newColumn.Name}_tmp_{GenerateDeterministicSuffix(schema, tableName, newColumn.Name, "tmp")}";
                SqlTableColumn tempColumn = newColumn with { Name = tempColumnName };
                
                // Add new column with temporary name
                sb.Append(GenerateAddColumnStatement(tempColumn, tableName, schema, tableMayHaveData));
                
                // Drop old column
                sb.AppendLine(GenerateDropColumnStatement(oldColumn.Name, tableName, schema));
                
                // Rename temporary column to final name
                sb.AppendLine(GenerateRenameColumnStatement(tableName, tempColumnName, newColumn.Name, schema));
                
                // After renaming, if the new column shouldn't have a default constraint (but the temp column had one),
                // drop the temporary constraint. The constraint is now associated with the renamed column.
                // Check if temp column would have gotten a default constraint but new column shouldn't have one
                bool tempColumnNeedsDefault = tableMayHaveData && !tempColumn.IsIdentity && string.IsNullOrWhiteSpace(tempColumn.DefaultConstraintValue);
                bool newColumnShouldHaveDefault = !string.IsNullOrWhiteSpace(newColumn.DefaultConstraintValue);
                
                if (tempColumnNeedsDefault && !newColumnShouldHaveDefault)
                {
                    // Temp column got a default constraint, but new column shouldn't have one - drop it
                    // After renaming, the constraint is associated with the new column name, so use that
                    sb.Append(GenerateDropDefaultConstraintStatement(newColumn.Name, tableName, null, schema));
                }
            }
            else
            {
                // Different name: add new column first, then drop old
                sb.Append(GenerateAddColumnStatement(newColumn, tableName, schema, tableMayHaveData));
                sb.AppendLine(GenerateDropColumnStatement(oldColumn.Name, tableName, schema));
            }
        }
        else
        {
            // Safe scenario: can drop then add (existing behavior)
            sb.AppendLine(GenerateDropColumnStatement(oldColumn.Name, tableName, schema));
            sb.Append(GenerateAddColumnStatement(newColumn, tableName, schema, tableMayHaveData));
        }
        
        return sb.ToString();
    }

    public static string GenerateColumnModifications(TableDiff tableDiff, ConcurrentDictionary<string, SqlTable>? currentSchema = null, ConcurrentDictionary<string, SqlTable>? targetSchema = null)
    {
        StringBuilder sb = new StringBuilder();

        // For incremental migrations, the table must exist in the target schema
        // If it doesn't, this indicates a problem with migration ordering or the target schema
        if (targetSchema == null || !targetSchema.ContainsKey(tableDiff.TableName.ToLowerInvariant()))
        {
            throw new InvalidOperationException(
                $"Cannot generate column modifications for table '{tableDiff.TableName}': table does not exist in target schema. " +
                "This usually indicates that a previous migration that creates this table has not been applied, or there is an issue with migration ordering.");
        }

        // Get schema from current schema or target schema or default to dbo
        string schema = "dbo";
        if (currentSchema != null && currentSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? currentTable))
        {
            schema = currentTable.Schema;
        }
        else if (targetSchema != null && targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTable))
        {
            schema = targetTable.Schema;
        }

        // Order operations: DROP INDEXES (on columns being dropped), DROP COLUMN, then ALTER COLUMN, then ADD COLUMN
        // This ensures we don't have dependency issues

        // Drop indexes that reference columns being dropped (must happen before dropping columns)
        if (currentSchema != null)
        {
            if (currentSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? tableForIndexes))
            {
                HashSet<string> columnsBeingDropped = new HashSet<string>(tableDiff.ColumnChanges
                    .Where(c => c.ChangeType == ColumnChangeType.Drop && c.OldColumn != null)
                    .Select(c => c.OldColumn!.Name), StringComparer.OrdinalIgnoreCase);

                foreach (SqlIndex index in tableForIndexes.Indexes)
                {
                    // If any column in this index is being dropped, drop the index
                    if (index.Columns.Any(col => columnsBeingDropped.Contains(col)))
                    {
                        sb.AppendLine(GenerateDropIndexStatement(index));
                    }
                }
            }
        }

        // Pre-calculate lists for processing
        List<ColumnChange> dropChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Drop).ToList();
        List<ColumnChange> addChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Add).ToList();
        List<ColumnChange> modifyChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Modify).ToList();
        
        // Check if dropping columns would reduce table to 0 columns
        // If so, we need to ensure at least one column is added first (from Add operations)
        bool wouldReduceToZero = WouldReduceToZeroColumns(tableDiff, currentSchema);
        
        // Check if we have Modify changes that require DROP+ADD on the only column, AND we have Add changes
        // In this case, we need to add columns FIRST before modifying the only column
        // We check the TARGET schema (before migration) to see if it has only 1 column
        bool needsAddBeforeModify = false;
        if (modifyChanges.Count > 0 && addChanges.Count > 0 && targetSchema != null)
        {
            if (targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTableForCheck))
            {
                int dataColumnCount = targetTableForCheck.Columns.Values.Count(c => !c.IsComputed);
                
                // Check if any Modify change requires DROP+ADD and is on the only column in TARGET schema
                foreach (ColumnChange modifyChange in modifyChanges)
                {
                    if (modifyChange.OldColumn != null && modifyChange.NewColumn != null)
                    {
                        bool requiresDropAdd = (modifyChange.OldColumn.IsComputed || modifyChange.NewColumn.IsComputed) ||
                                              (modifyChange.OldColumn.IsIdentity != modifyChange.NewColumn.IsIdentity);
                        
                        if (requiresDropAdd && dataColumnCount == 1 && 
                            targetTableForCheck.Columns.TryGetValue(modifyChange.OldColumn.Name.ToLowerInvariant(), out SqlTableColumn? col) &&
                            !col.IsComputed)
                        {
                            needsAddBeforeModify = true;
                            break;
                        }
                    }
                }
            }
        }
        
        // Check if table exists in targetSchema (may have data)
        // For incremental migrations, targetSchema represents the state before this migration is applied
        // If the table exists in targetSchema, it was created by a previous migration, so it may have data
        // We check targetSchema (not currentSchema) because that's what the database state will be
        // when this migration is applied to a fresh database
        bool tableMayHaveData = targetSchema != null && targetSchema.ContainsKey(tableDiff.TableName.ToLowerInvariant());
        
        if (wouldReduceToZero && dropChanges.Count > 0)
        {
            // Need to add columns first before dropping to avoid zero-column state
            // Add columns from Add operations first
            foreach (ColumnChange change in addChanges)
            {
                if (change.NewColumn != null)
                {
                    sb.Append(GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName, schema, tableMayHaveData));
                }
            }
        }
        else if (needsAddBeforeModify)
        {
            // Need to add columns first before modifying the only column
            // This ensures we have multiple columns before attempting DROP+ADD on the only column
            foreach (ColumnChange change in addChanges)
            {
                if (change.NewColumn != null)
                {
                    sb.Append(GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName, schema, tableMayHaveData));
                }
            }
        }

        // Drop columns (safe to drop now if we've added replacement columns above, or if not zero-column scenario)
        foreach (ColumnChange change in dropChanges)
        {
            if (change.OldColumn != null)
            {
                sb.AppendLine(GenerateDropColumnStatement(change.OldColumn.Name, tableDiff.TableName, schema));
            }
        }

        // Alter columns (excluding computed columns, identity changes, and PK changes which need special handling)
        foreach (ColumnChange change in modifyChanges)
        {
            if (change.OldColumn != null && change.NewColumn != null)
            {
                // Check if this Modify change requires DROP+ADD and was the reason we added columns first
                bool requiresDropAdd = (change.OldColumn.IsComputed || change.NewColumn.IsComputed) ||
                                      (change.OldColumn.IsIdentity != change.NewColumn.IsIdentity);
                // Check TARGET schema (before migration) to see if this was the only column
                bool isSingleColumnModify = needsAddBeforeModify && requiresDropAdd && targetSchema != null &&
                    targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTableForCheck) &&
                    targetTableForCheck.Columns.Values.Count(c => !c.IsComputed) == 1 &&
                    targetTableForCheck.Columns.TryGetValue(change.OldColumn.Name.ToLowerInvariant(), out SqlTableColumn? col) &&
                    !col.IsComputed;
                
                // Check if it's a computed column change - these need DROP + ADD
                if (change.OldColumn.IsComputed || change.NewColumn.IsComputed)
                {
                    // Use safe wrapper for computed column changes (unless we've already added columns)
                    if (isSingleColumnModify)
                    {
                        // Columns already added first, safe to use normal DROP+ADD
                        sb.AppendLine(GenerateDropColumnStatement(change.OldColumn.Name, tableDiff.TableName, schema));
                        sb.Append(GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName, schema, tableMayHaveData));
                    }
                    else
                    {
                        // Use safe wrapper for computed column changes
                        if (change.OldColumn.IsComputed && change.NewColumn.IsComputed)
                        {
                            // Both computed - use safe wrapper
                            sb.Append(GenerateSafeColumnDropAndAdd(change.OldColumn, change.NewColumn, tableDiff.TableName, schema, tableDiff, currentSchema));
                        }
                        else if (change.OldColumn.IsComputed)
                        {
                            // Old is computed, new is not - drop computed, add regular
                            sb.Append(GenerateSafeColumnDropAndAdd(change.OldColumn, change.NewColumn, tableDiff.TableName, schema, tableDiff, currentSchema));
                        }
                        else
                        {
                            // New is computed, old is not - drop regular, add computed
                            sb.Append(GenerateSafeColumnDropAndAdd(change.OldColumn, change.NewColumn, tableDiff.TableName, schema, tableDiff, currentSchema));
                        }
                    }
                }
                // Check if identity property changed - SQL Server requires DROP + ADD
                else if (change.OldColumn.IsIdentity != change.NewColumn.IsIdentity)
                {
                    // Use safe wrapper for identity changes (unless we've already added columns)
                    if (isSingleColumnModify)
                    {
                        // Columns already added first, safe to use normal DROP+ADD
                        sb.AppendLine(GenerateDropColumnStatement(change.OldColumn.Name, tableDiff.TableName, schema));
                        sb.Append(GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName, schema, tableMayHaveData));
                    }
                    else
                    {
                        // Use safe wrapper for identity changes
                        sb.Append(GenerateSafeColumnDropAndAdd(change.OldColumn, change.NewColumn, tableDiff.TableName, schema, tableDiff, currentSchema));
                    }
                }
                
                // Check if default constraint changed (added, removed, or value changed)
                bool defaultConstraintChanged = (change.OldColumn.DefaultConstraintValue ?? string.Empty) != (change.NewColumn.DefaultConstraintValue ?? string.Empty);
                
                // Check if only PK property changed (and no other significant changes) - handle via PK constraint changes
                // Note: PK changes are also handled via FK changes, but we need to ensure column is recreated if needed
                if (change.OldColumn.IsPrimaryKey != change.NewColumn.IsPrimaryKey && 
                         change.OldColumn.IsIdentity == change.NewColumn.IsIdentity &&
                         change.OldColumn.IsComputed == change.NewColumn.IsComputed &&
                         change.OldColumn.SqlType == change.NewColumn.SqlType &&
                         change.OldColumn.IsNullable == change.NewColumn.IsNullable &&
                         !defaultConstraintChanged)
                {
                    // Only PK changed, no other changes - PK constraint will be handled separately
                    // But we still need to handle the column if there are other changes
                    string alterSql = GenerateAlterColumnStatement(change.OldColumn, change.NewColumn, tableDiff.TableName, schema);
                    if (!string.IsNullOrEmpty(alterSql))
                    {
                        sb.Append(alterSql);
                    }
                }
                else if (defaultConstraintChanged && 
                         change.OldColumn.IsIdentity == change.NewColumn.IsIdentity &&
                         change.OldColumn.IsComputed == change.NewColumn.IsComputed &&
                         change.OldColumn.SqlType == change.NewColumn.SqlType &&
                         change.OldColumn.IsNullable == change.NewColumn.IsNullable &&
                         change.OldColumn.IsPrimaryKey == change.NewColumn.IsPrimaryKey)
                {
                    // Only default constraint changed - drop old and add new
                    // Drop old default constraint if it exists
                    if (!string.IsNullOrWhiteSpace(change.OldColumn.DefaultConstraintValue))
                    {
                        sb.Append(GenerateDropDefaultConstraintStatement(change.OldColumn.Name, tableDiff.TableName, null, schema));
                    }
                    
                    // Add new default constraint if specified
                    if (!string.IsNullOrWhiteSpace(change.NewColumn.DefaultConstraintValue))
                    {
                        string normalizedValue = NormalizeDefaultConstraintValue(change.NewColumn.DefaultConstraintValue);
                        string constraintName = !string.IsNullOrWhiteSpace(change.NewColumn.DefaultConstraintName)
                            ? change.NewColumn.DefaultConstraintName
                            : $"DF_{tableDiff.TableName}_{change.NewColumn.Name}_{GenerateDeterministicSuffix(tableDiff.TableName, change.NewColumn.Name, normalizedValue, "modify")}";
                        sb.AppendLine($"ALTER TABLE [{schema}].[{tableDiff.TableName}] ADD CONSTRAINT [{constraintName}] DEFAULT {normalizedValue} FOR [{change.NewColumn.Name}];");
                    }
                }
                else
                {
                    string alterSql = GenerateAlterColumnStatement(change.OldColumn, change.NewColumn, tableDiff.TableName, schema);
                    if (!string.IsNullOrEmpty(alterSql))
                    {
                        sb.Append(alterSql);
                        
                        // Handle default constraint changes separately (ALTER COLUMN doesn't support DEFAULT)
                        if (defaultConstraintChanged)
                        {
                            // Drop old default constraint if it exists
                            if (!string.IsNullOrWhiteSpace(change.OldColumn.DefaultConstraintValue))
                            {
                                sb.Append(GenerateDropDefaultConstraintStatement(change.OldColumn.Name, tableDiff.TableName, null, schema));
                            }
                            
                            // Add new default constraint if specified
                            if (!string.IsNullOrWhiteSpace(change.NewColumn.DefaultConstraintValue))
                            {
                                string normalizedValue = NormalizeDefaultConstraintValue(change.NewColumn.DefaultConstraintValue);
                                string constraintName = !string.IsNullOrWhiteSpace(change.NewColumn.DefaultConstraintName)
                                    ? change.NewColumn.DefaultConstraintName
                                    : $"DF_{tableDiff.TableName}_{change.NewColumn.Name}_{GenerateDeterministicSuffix(tableDiff.TableName, change.NewColumn.Name, normalizedValue, "modify")}";
                                sb.AppendLine($"ALTER TABLE [{schema}].[{tableDiff.TableName}] ADD CONSTRAINT [{constraintName}] DEFAULT {normalizedValue} FOR [{change.NewColumn.Name}];");
                            }
                        }
                    }
                }
            }
        }

        // Add columns (skip if we already added them above for zero-column scenario or single-column modify scenario)
        if ((!wouldReduceToZero || dropChanges.Count == 0) && !needsAddBeforeModify)
        {
            foreach (ColumnChange change in addChanges)
            {
                if (change.NewColumn != null)
                {
                    sb.Append(GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName, schema, tableMayHaveData));
                }
            }
        }

        return sb.ToString();
    }

    public static string GenerateForeignKeyStatement(
        List<SqlForeignKey> fkGroup, 
        Dictionary<string, SqlTable> allTables,
        bool forceNoCheck = false)
    {
        if (fkGroup.Count == 0)
        {
            return string.Empty;
        }

        SqlForeignKey firstFk = fkGroup[0];
        StringBuilder sb = new StringBuilder();

        sb.Append($"ALTER TABLE [{firstFk.Schema}].[{firstFk.Table}] WITH ");
        
        // Use forceNoCheck if specified, otherwise use original NotEnforced value
        bool useNoCheck = forceNoCheck || firstFk.NotEnforced;
        sb.Append(useNoCheck ? "NOCHECK" : "CHECK");
        sb.Append(" ADD CONSTRAINT [");
        sb.Append(firstFk.Name);
        sb.Append("] FOREIGN KEY(");

        // Multi-column FK support
        List<string> columns = fkGroup.OrderBy(f => f.Column).Select(f => $"[{f.Column}]").ToList();
        List<string> refColumns = fkGroup.OrderBy(f => f.Column).Select(f => $"[{f.RefColumn}]").ToList();

        sb.Append(string.Join(", ", columns));
        sb.Append($") REFERENCES [{firstFk.RefSchema}].[{firstFk.RefTable}](");
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

        // Only add CHECK CONSTRAINT statement if NOT forcing NOCHECK and NOT originally NOCHECK
        if (!forceNoCheck && !firstFk.NotEnforced)
        {
            sb.Append($"ALTER TABLE [{firstFk.Schema}].[{firstFk.Table}] CHECK CONSTRAINT [{firstFk.Name}];");
            sb.AppendLine();
        }

        return sb.ToString();
    }

    public static string GenerateDropForeignKeyStatement(SqlForeignKey fk)
    {
        return $"ALTER TABLE [{fk.Schema}].[{fk.Table}] DROP CONSTRAINT [{fk.Name}];";
    }

    public static string GenerateDropPrimaryKeyStatement(string tableName, string constraintName, string schema = "dbo", string? variableSuffix = null)
    {
        // Use provided suffix or generate a deterministic one to avoid conflicts
        string varSuffix = variableSuffix ?? GenerateDeterministicSuffix(schema, tableName, constraintName, "droppk");
        
        // Use dynamic SQL to check if constraint exists before dropping
        return $"""
            DECLARE @pkConstraintName_{varSuffix} NVARCHAR(128);
            SELECT @pkConstraintName_{varSuffix} = name FROM sys.key_constraints 
            WHERE parent_object_id = OBJECT_ID('[{schema}].[{tableName}]') 
            AND type = 'PK'
            AND name = '{constraintName}';
            IF @pkConstraintName_{varSuffix} IS NOT NULL
                EXEC('ALTER TABLE [{schema}].[{tableName}] DROP CONSTRAINT [' + @pkConstraintName_{varSuffix} + ']');
            """;
    }

    public static string GenerateAddPrimaryKeyStatement(string tableName, List<string> columnNames, string constraintName, string schema = "dbo")
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"ALTER TABLE [{schema}].[{tableName}] ADD CONSTRAINT [{constraintName}] PRIMARY KEY (");
        sb.Append(string.Join(", ", columnNames.Select(c => $"[{c}]")));
        sb.AppendLine(");");
        return sb.ToString();
    }

    public static string GenerateDropIndexStatement(SqlIndex index)
    {
        // Use dynamic SQL to check if index exists before dropping
        return $"""
            IF EXISTS (SELECT * FROM sys.indexes WHERE name = '{index.Name}' AND object_id = OBJECT_ID('[{index.Schema}].[{index.Table}]'))
            BEGIN
                DROP INDEX [{index.Name}] ON [{index.Schema}].[{index.Table}];
            END
            """;
    }

    public static string GenerateCreateIndexStatement(SqlIndex index)
    {
        StringBuilder sb = new StringBuilder();
        // Use dynamic SQL to check if index doesn't exist before creating
        sb.AppendLine($"""
            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '{index.Name}' AND object_id = OBJECT_ID('[{index.Schema}].[{index.Table}]'))
            BEGIN
                CREATE {(index.IsUnique ? "UNIQUE " : "")}NONCLUSTERED INDEX [{index.Name}] ON [{index.Schema}].[{index.Table}] ({string.Join(", ", index.Columns.Select(c => $"[{c}]"))});
            END
            """);
        return sb.ToString();
    }

    public static string GenerateCreateSequenceStatement(SqlSequence sequence)
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"CREATE SEQUENCE [{sequence.Schema}].[{sequence.Name}] AS {sequence.DataType}");
        
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

    public static string GenerateDropSequenceStatement(string sequenceName, string schema = "dbo")
    {
        // Use dynamic SQL to check if sequence exists before dropping
        return $"""
            IF EXISTS (SELECT * FROM sys.sequences WHERE name = '{sequenceName}' AND schema_id = SCHEMA_ID('{schema}'))
            BEGIN
                DROP SEQUENCE [{schema}].[{sequenceName}];
            END
            """;
    }

    public static string GenerateAlterSequenceStatement(SqlSequence oldSequence, SqlSequence newSequence)
    {
        // SQL Server doesn't support ALTER SEQUENCE for all properties, so we'll use DROP + CREATE
        // But for incremental changes, we can try ALTER for some properties
        // For simplicity and reliability, we'll use DROP + CREATE
        StringBuilder sb = new StringBuilder();
        sb.AppendLine(GenerateDropSequenceStatement(oldSequence.Name, oldSequence.Schema));
        sb.Append(GenerateCreateSequenceStatement(newSequence));
        return sb.ToString();
    }

    public static string GenerateCreateProcedureStatement(SqlStoredProcedure procedure)
    {
        // CREATE PROCEDURE must be the first statement in a batch, so we use DROP IF EXISTS + GO + CREATE pattern
        // This ensures idempotency and proper batching
        return $"""
            GO
            IF EXISTS (SELECT * FROM sys.procedures WHERE name = '{procedure.Name}' AND schema_id = SCHEMA_ID('{procedure.Schema}'))
                DROP PROCEDURE [{procedure.Schema}].[{procedure.Name}];
            GO
            {procedure.Definition}
            GO
            """;
    }

    public static string GenerateDropProcedureStatement(string procedureName, string schema = "dbo")
    {
        // DROP PROCEDURE can be in a batch, but we add GO for consistency and to ensure proper batching
        return $"""
            GO
            IF EXISTS (SELECT * FROM sys.procedures WHERE name = '{procedureName}' AND schema_id = SCHEMA_ID('{schema}'))
                DROP PROCEDURE [{schema}].[{procedureName}];
            GO
            """;
    }

    /// <summary>
    /// Generates SQL to reorder columns in a table to match the target schema order.
    /// This is done by recreating the table with columns in the correct order.
    /// Returns the reorder SQL and a list of constraint/index recreation SQL statements.
    /// </summary>
    /// <param name="actualTable">Actual database state (from currentSchema - reflects actual column order in database)</param>
    /// <param name="desiredTable">Desired state (from currentSchema - columns ordered by OrdinalPosition)</param>
    /// <param name="allTablesForFk">All tables for FK generation (needed for FK references)</param>
    public static (string ReorderSql, List<string> ConstraintSql) GenerateColumnReorderStatement(
        SqlTable actualTable, 
        SqlTable desiredTable,
        ConcurrentDictionary<string, SqlTable>? allTablesForFk = null)
    {
        List<string> constraintSql = new List<string>();
        
        // Get columns ordered by actual database position (from OrdinalPosition - reflects actual DB order)
        List<SqlTableColumn> actualColumns = actualTable.Columns.Values
            .OrderBy(c => c.OrdinalPosition)
            .ToList();
        
        // Get columns ordered by desired position (from OrdinalPosition - desired order)
        // IMPORTANT: Only include columns that exist in actualTable (safety check to avoid dropped columns)
        List<SqlTableColumn> desiredColumns = desiredTable.Columns.Values
            .Where(c => actualTable.Columns.ContainsKey(c.Name.ToLowerInvariant()))
            .OrderBy(c => c.OrdinalPosition)
            .ToList();
        
        MigrationLogger.Log($"  GenerateColumnReorderStatement: actualColumns=[{string.Join(", ", actualColumns.Select(c => c.Name))}]");
        MigrationLogger.Log($"  GenerateColumnReorderStatement: desiredColumns (after filtering) =[{string.Join(", ", desiredColumns.Select(c => c.Name))}]");
        MigrationLogger.Log($"  GenerateColumnReorderStatement: desiredTable.Columns.Values (before filtering) =[{string.Join(", ", desiredTable.Columns.Values.OrderBy(c => c.OrdinalPosition).Select(c => c.Name))}]");
        
        // Safety check: if no columns match after filtering, skip reordering
        if (desiredColumns.Count == 0 || actualColumns.Count == 0)
        {
            // No columns to reorder, skip
            MigrationLogger.Log($"  GenerateColumnReorderStatement: Skipping - no columns match after filtering");
            return (string.Empty, constraintSql);
        }
        
        // Additional safety check: ensure all desiredColumns exist in actualColumns
        // This catches cases where filtering didn't work correctly
        HashSet<string> actualColumnNames = actualColumns
            .Select(c => c.Name.ToLowerInvariant())
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        
        List<string> missingColumns = desiredColumns
            .Where(dc => !actualColumnNames.Contains(dc.Name.ToLowerInvariant()))
            .Select(dc => dc.Name)
            .ToList();
        
        if (missingColumns.Count > 0)
        {
            MigrationLogger.Log($"  GenerateColumnReorderStatement: ERROR - Desired columns not found in actualTable: [{string.Join(", ", missingColumns)}]");
            MigrationLogger.Log($"    actualColumns: [{string.Join(", ", actualColumns.Select(c => c.Name))}]");
            MigrationLogger.Log($"    desiredColumns: [{string.Join(", ", desiredColumns.Select(c => c.Name))}]");
            throw new InvalidOperationException($"Cannot reorder columns: desired columns [{string.Join(", ", missingColumns)}] do not exist in actualTable. This indicates a bug in the migration generation logic.");
        }
        
        // Check if order is already correct by comparing column names in sequence
        bool orderMatches = actualColumns.Count == desiredColumns.Count &&
            actualColumns.Select(c => c.Name)
                .SequenceEqual(desiredColumns.Select(c => c.Name), StringComparer.OrdinalIgnoreCase);
        
        MigrationLogger.Log($"  GenerateColumnReorderStatement: orderMatches={orderMatches}");
        
        if (orderMatches)
        {
            // Order already matches, no need to reorder
            MigrationLogger.Log($"  GenerateColumnReorderStatement: Skipping - order already matches");
            return (string.Empty, constraintSql);
        }
        
        // Extract constraints and indexes that need to be recreated (in correct dependency order)
        // We'll add them to constraintSql in the correct order at the end
        
        // 1. Primary Key (must be first - establishes physical structure)
        List<SqlTableColumn> pkColumns = desiredTable.Columns.Values.Where(c => c.IsPrimaryKey).OrderBy(c => c.OrdinalPosition).ToList();
        string pkSql = string.Empty;
        if (pkColumns.Count > 0)
        {
            List<string> pkColumnNames = pkColumns.Select(c => c.Name).ToList();
            string pkConstraintName = $"PK_{desiredTable.Name}";
            pkSql = GenerateAddPrimaryKeyStatement(desiredTable.Name, pkColumnNames, pkConstraintName, desiredTable.Schema);
        }
        
        // 2. Foreign Keys - extract and store original CHECK/NOCHECK state
        Dictionary<string, List<SqlForeignKey>> fkGroups = new Dictionary<string, List<SqlForeignKey>>();
        List<(List<SqlForeignKey> FkGroup, bool WasNoCheck)> fkStates = new List<(List<SqlForeignKey>, bool)>();
        
        foreach (SqlTableColumn column in desiredTable.Columns.Values)
        {
            foreach (SqlForeignKey fk in column.ForeignKeys)
            {
                if (!fkGroups.ContainsKey(fk.Name))
                {
                    fkGroups[fk.Name] = new List<SqlForeignKey>();
                }
                if (!fkGroups[fk.Name].Any(f => f.Column.Equals(fk.Column, StringComparison.OrdinalIgnoreCase)))
                {
                    fkGroups[fk.Name].Add(fk);
                }
            }
        }
        
        // Store FK states (all FKs in a group have the same NotEnforced value)
        foreach (List<SqlForeignKey> fkGroup in fkGroups.Values)
        {
            bool wasNoCheck = fkGroup[0].NotEnforced;
            fkStates.Add((fkGroup, wasNoCheck));
        }
        
        // 3. Indexes
        List<string> indexSql = new List<string>();
        foreach (SqlIndex index in desiredTable.Indexes)
        {
            indexSql.Add(GenerateCreateIndexStatement(index));
        }
        
        // Build constraint SQL in correct order: PK → Indexes → FKs (NOCHECK) → Restore FK CHECK
        if (!string.IsNullOrEmpty(pkSql))
        {
            constraintSql.Add(pkSql);
        }
        
        foreach (string index in indexSql)
        {
            constraintSql.Add(index);
        }
        
        // Create FKs - use NOCHECK for all FKs during reordering to avoid circular dependency issues
        // When a table is dropped and recreated, FKs from other tables to this table are also dropped
        // If multiple tables with circular FKs are being reordered, creating FKs with CHECK can fail
        // because the referenced table's FK back to this table might not exist yet
        // 
        // Strategy: Create all FKs with NOCHECK first, then restore CHECK state for FKs that weren't originally NOCHECK
        // This ensures FKs can be created successfully even with circular dependencies
        Dictionary<string, SqlTable> tablesDict = allTablesForFk != null 
            ? new Dictionary<string, SqlTable>(allTablesForFk, StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, SqlTable> { { desiredTable.Name.ToLowerInvariant(), desiredTable } };
        
        // Create all FKs with NOCHECK first (avoids circular dependency issues)
        foreach (var (fkGroup, wasNoCheck) in fkStates)
        {
            // Force NOCHECK during creation to avoid circular dependency issues
            string fkSql = GenerateForeignKeyStatement(fkGroup, tablesDict, forceNoCheck: true);
            if (!string.IsNullOrEmpty(fkSql))
            {
                constraintSql.Add(fkSql);
            }
        }
        
        // Restore CHECK state for FKs that weren't originally NOCHECK
        // IMPORTANT: CHECK CONSTRAINT doesn't always restore is_not_trusted correctly after WITH NOCHECK
        // So we need to drop and recreate the FK with WITH CHECK to ensure correct NotEnforced state
        foreach (var (fkGroup, wasNoCheck) in fkStates)
        {
            if (!wasNoCheck)
            {
                SqlForeignKey firstFk = fkGroup[0];
                // Drop the FK that was created with NOCHECK
                constraintSql.Add($"ALTER TABLE [{firstFk.Schema}].[{firstFk.Table}] DROP CONSTRAINT [{firstFk.Name}];");
                
                // Recreate it with WITH CHECK to ensure correct NotEnforced state
                string fkSqlWithCheck = GenerateForeignKeyStatement(fkGroup, tablesDict, forceNoCheck: false);
                if (!string.IsNullOrEmpty(fkSqlWithCheck))
                {
                    constraintSql.Add(fkSqlWithCheck);
                }
            }
        }
        
        // Generate column reordering SQL
        // Approach: Create temp table with correct order, copy data, drop old, rename temp
        StringBuilder sb = new StringBuilder();
        string tempTableName = $"{actualTable.Name}_reorder_{GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, "reorder")}";
        
        sb.AppendLine("-- Reorder columns to match target schema");
        sb.AppendLine($"-- Creating temporary table with correct column order");
        
        // Check if table has IDENTITY columns for IDENTITY_INSERT handling
        bool hasIdentityColumns = desiredColumns.Any(c => c.IsIdentity);
        
        // Create temp table with columns in correct order (reuse GenerateCreateTableStatement logic)
        sb.Append($"CREATE TABLE [{actualTable.Schema}].[{tempTableName}]");
        sb.AppendLine("(");
        
        for (int i = 0; i < desiredColumns.Count; i++)
        {
            SqlTableColumn col = desiredColumns[i];
            bool isLast = i == desiredColumns.Count - 1;
            
            sb.Append("    [");
            sb.Append(col.Name);
            sb.Append("] ");
            
            // Column type (reuse logic from GenerateAddColumnStatement)
            if (col.IsComputed && !string.IsNullOrWhiteSpace(col.ComputedSql))
            {
                sb.Append("AS ");
                sb.Append(col.ComputedSql);
            }
            else
            {
                string sqlType = col.SqlType.ToSqlDbType();
                sb.Append(sqlType.ToUpperInvariant());
                
                if (col.LengthOrPrecision.HasValue)
                {
                    if (sqlType.ToLowerInvariant() is "decimal" or "numeric")
                    {
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
                
                if (!col.IsNullable)
                {
                    sb.Append(" NOT NULL");
                }
                
                // Default constraint (if exists) - preserve value but generate new constraint name when reordering
                // to avoid conflicts (the original constraint may still exist in the database)
                if (!string.IsNullOrWhiteSpace(col.DefaultConstraintValue))
                {
                    // Always generate a new constraint name to avoid conflicts during reordering
                    string normalizedValue = NormalizeDefaultConstraintValue(col.DefaultConstraintValue);
                    string constraintName = $"DF_{actualTable.Name}_{col.Name}_{GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, col.Name, normalizedValue, "reorder")}";
                    sb.Append($" CONSTRAINT [{constraintName}] DEFAULT {normalizedValue}");
                }
                
                if (col.IsIdentity)
                {
                    sb.Append(" IDENTITY");
                    if (col.IdentitySeed.HasValue && col.IdentityIncrement.HasValue)
                    {
                        sb.Append($"({col.IdentitySeed.Value},{col.IdentityIncrement.Value})");
                    }
                    else
                    {
                        sb.Append("(1,1)");
                    }
                }
            }
            
            if (!isLast)
            {
                sb.Append(",");
            }
            sb.AppendLine();
        }
        
        sb.AppendLine(");");
        sb.AppendLine();
        
        // Copy data from old table to temp table
        // Note: IDENTITY_INSERT will be set inside the IF block if columns exist
        
        // Build column list for INSERT: use all columns from desiredTable (in desired order)
        List<string> columnNames = desiredColumns
            .Select(c => $"[{c.Name}]")
            .ToList();
        
        // Build SELECT list: select columns from actualTable in the order they exist in the database
        // IMPORTANT: We must select columns in the order they exist in the actual table (actualColumns order),
        // but map them to the desired column order for INSERT
        // The SELECT list should match the INSERT list positionally, not by name order
        List<string> selectColumns = new List<string>();
        MigrationLogger.Log($"  GenerateColumnReorderStatement: Building SELECT list for INSERT");
        MigrationLogger.Log($"    actualColumns: [{string.Join(", ", actualColumns.Select(c => c.Name))}]");
        MigrationLogger.Log($"    desiredColumns: [{string.Join(", ", desiredColumns.Select(c => c.Name))}]");
        
        // Build a mapping from column name to actual column for fast lookup
        Dictionary<string, SqlTableColumn> actualColumnsByName = actualColumns
            .ToDictionary(c => c.Name.ToLowerInvariant(), c => c, StringComparer.OrdinalIgnoreCase);
        
        // Build a set of desired column names for quick lookup
        HashSet<string> desiredColumnNames = desiredColumns
            .Select(c => c.Name.ToLowerInvariant())
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        
        // First, build SELECT list in the order of desiredColumns (to match INSERT order positionally)
        // But only include columns that exist in actualTable
        foreach (SqlTableColumn desiredCol in desiredColumns)
        {
            // Find the corresponding column in actualColumns by name
            if (actualColumnsByName.TryGetValue(desiredCol.Name.ToLowerInvariant(), out SqlTableColumn? actualCol))
            {
                // Column exists in actualTable, use the actual column name (preserve case from database)
                selectColumns.Add($"[{actualCol.Name}]");
                MigrationLogger.Log($"    Mapped desired column '{desiredCol.Name}' to actual column '{actualCol.Name}'");
            }
            else
            {
                // Column doesn't exist in actualTable - this should not happen after filtering
                // But if it does, we need to handle it gracefully
                MigrationLogger.Log($"    ERROR: Column '{desiredCol.Name}' from desiredColumns doesn't exist in actualColumns!");
                MigrationLogger.Log($"      actualColumns: [{string.Join(", ", actualColumns.Select(c => c.Name))}]");
                MigrationLogger.Log($"      This indicates a bug - desiredColumns should only contain columns that exist in actualTable");
                // Throw an exception to catch this bug early
                throw new InvalidOperationException($"Column '{desiredCol.Name}' is in desiredColumns but not in actualTable. This should not happen after filtering.");
            }
        }
        
        // Safety check: ensure all columns in actualTable that are in desiredColumns are included
        // This catches any edge cases where a column exists in actualTable but wasn't found above
        foreach (SqlTableColumn actualCol in actualColumns)
        {
            if (desiredColumnNames.Contains(actualCol.Name.ToLowerInvariant()))
            {
                // Check if this column is already in selectColumns
                bool alreadyIncluded = selectColumns.Any(sc => 
                    sc.Trim('[', ']').Equals(actualCol.Name, StringComparison.OrdinalIgnoreCase));
                
                if (!alreadyIncluded)
                {
                    // This shouldn't happen, but add it to maintain consistency
                    MigrationLogger.Log($"    WARNING: Column '{actualCol.Name}' exists in actualTable and desiredColumns but wasn't included in SELECT list");
                    // Don't add it here - it would break positional matching with INSERT
                    // Instead, this indicates a bug that should be fixed
                }
            }
        }
        
        MigrationLogger.Log($"  GenerateColumnReorderStatement: INSERT columns: [{string.Join(", ", columnNames)}]");
        MigrationLogger.Log($"  GenerateColumnReorderStatement: SELECT columns: [{string.Join(", ", selectColumns)}]");
        
        if (columnNames.Count == 0 || selectColumns.Count == 0)
        {
            // No columns to copy - this shouldn't happen, but return empty to avoid errors
            MigrationLogger.Log($"  GenerateColumnReorderStatement: Skipping INSERT - no columns to copy (columnNames.Count={columnNames.Count}, selectColumns.Count={selectColumns.Count})");
            return (string.Empty, constraintSql);
        }
        
        // Safety check: ensure we have the same number of INSERT and SELECT columns
        if (columnNames.Count != selectColumns.Count)
        {
            MigrationLogger.Log($"  GenerateColumnReorderStatement: ERROR - Mismatch between INSERT columns ({columnNames.Count}) and SELECT columns ({selectColumns.Count})!");
            MigrationLogger.Log($"    INSERT columns: [{string.Join(", ", columnNames)}]");
            MigrationLogger.Log($"    SELECT columns: [{string.Join(", ", selectColumns)}]");
            throw new InvalidOperationException($"Column count mismatch: INSERT has {columnNames.Count} columns but SELECT has {selectColumns.Count} columns");
        }
        
        // Copy data from original table to temporary table
        // Check if all required columns exist before attempting INSERT
        // Use dynamic SQL to avoid parsing errors if columns don't exist
        // This handles edge cases where the table state might differ from what we expect
        sb.AppendLine($"-- Copy data from original table to temporary table");
        
        // Build a check that verifies the table exists and all columns exist
        // Store OBJECT_ID result in a variable to avoid calling it multiple times
        string tableObjectIdVar = $"@tableObjectId_{GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, "reorder", "objectid")}";
        sb.AppendLine($"DECLARE {tableObjectIdVar} INT = OBJECT_ID('[{actualTable.Schema}].[{actualTable.Name}]', 'U');");
        
        List<string> columnExistenceChecks = new List<string>();
        foreach (string selectCol in selectColumns)
        {
            string colName = selectCol.Trim('[', ']');
            // Escape single quotes in column names for SQL string literals
            string escapedColName = colName.Replace("'", "''");
            columnExistenceChecks.Add($"EXISTS (SELECT 1 FROM sys.columns WHERE object_id = {tableObjectIdVar} AND name = '{escapedColName}')");
        }
        
        string allColumnsExistCheck = $"{tableObjectIdVar} IS NOT NULL AND {string.Join(" AND ", columnExistenceChecks)}";
        
        // Build the INSERT statement as a dynamic SQL string
        // Escape single quotes in the SQL string
        string insertSql = $"INSERT INTO [{actualTable.Schema}].[{tempTableName}] ({string.Join(", ", columnNames)}) SELECT {string.Join(", ", selectColumns)} FROM [{actualTable.Schema}].[{actualTable.Name}];";
        string escapedInsertSql = insertSql.Replace("'", "''");
        
        // Generate a unique variable name to avoid conflicts when multiple tables are reordered in the same migration
        string varSuffix = GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, "reorder", "insertsql");
        string insertSqlVarName = $"@insertSql_{varSuffix}";
        
        sb.AppendLine($"IF {allColumnsExistCheck}");
        sb.AppendLine($"BEGIN");
        if (hasIdentityColumns)
        {
            sb.AppendLine($"    SET IDENTITY_INSERT [{actualTable.Schema}].[{tempTableName}] ON;");
        }
        sb.AppendLine($"    DECLARE {insertSqlVarName} NVARCHAR(MAX) = N'{escapedInsertSql}';");
        sb.AppendLine($"    EXEC sp_executesql {insertSqlVarName};");
        if (hasIdentityColumns)
        {
            sb.AppendLine($"    SET IDENTITY_INSERT [{actualTable.Schema}].[{tempTableName}] OFF;");
        }
        sb.AppendLine($"END");
        sb.AppendLine();
        
        // Drop original table (this will drop all constraints/indexes)
        sb.AppendLine($"-- Drop original table (constraints and indexes will be recreated after reordering)");
        sb.AppendLine($"DROP TABLE [{actualTable.Schema}].[{actualTable.Name}];");
        sb.AppendLine();
        
        // Rename temp table to original name
        sb.AppendLine($"-- Rename temporary table to original name");
        sb.AppendLine($"EXEC sp_rename '[{actualTable.Schema}].[{tempTableName}]', '{actualTable.Name}', 'OBJECT';");
        sb.AppendLine();
        
        return (sb.ToString(), constraintSql);
    }
}

