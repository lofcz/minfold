using System.Collections.Concurrent;
using System.Text;

namespace Minfold;

public static class GenerateColumns
{
    /// <summary>
    /// Normalizes a default constraint value by removing outer parentheses.
    /// SQL Server stores default constraint values with parentheses (e.g., "((0))"),
    /// but we want to use the normalized value in generated SQL.
    /// </summary>
    public static string NormalizeDefaultConstraintValue(string value)
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

    public static string GenerateAddColumnStatement(SqlTableColumn column, string tableName, string schema = "dbo")
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

            // Add length/precision/scale for types that need it
            if (sqlType.ToLowerInvariant() is "decimal" or "numeric")
            {
                // For DECIMAL/NUMERIC, if Scale has a value, we should always include precision/scale
                // Default precision to 18 (SQL Server default) if it's null but scale is present
                if (column.Scale.HasValue)
                {
                    int precision = column.Precision ?? 18; // Default to 18 if precision is null
                    sb.Append($"({precision},{column.Scale.Value})");
                }
                else if (column.Precision.HasValue)
                {
                    sb.Append($"({column.Precision.Value})");
                }
            }
            else if (sqlType.ToLowerInvariant() is "varchar" or "nvarchar" or "char" or "nchar" or "varbinary" or "binary")
            {
                if (column.Length.HasValue)
                {
                    if (column.Length.Value == -1)
                    {
                        sb.Append("(MAX)");
                    }
                    else
                    {
                        sb.Append($"({column.Length.Value})");
                    }
                }
            }
            else if (sqlType.ToLowerInvariant() is "datetime2" or "time" or "datetimeoffset")
            {
                if (column.Precision.HasValue)
                {
                    sb.Append($"({column.Precision.Value})");
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
                // 1. The column already has a default constraint value (preserve existing defaults)
                // 2. OR the column is not an IDENTITY (SQL Server requires DEFAULT for NOT NULL non-identity columns)
                // We always assume tables may have data, so we always add a DEFAULT constraint for NOT NULL columns
                // SQL Server requires either NULL, DEFAULT, IDENTITY, or empty table for NOT NULL columns
                bool needsDefault = !column.IsIdentity;
                
                if (needsDefault)
                {
                    // Use existing default constraint if available (preserves user-defined defaults)
                    // Otherwise, generate a default value based on the column type
                    string defaultValue;
                    string constraintName;
                    
                    if (!string.IsNullOrWhiteSpace(column.DefaultConstraintValue))
                    {
                        // Column already has a default constraint - preserve the value and name if available
                        // Normalize the value by removing outer parentheses if present (SQL Server stores them with parentheses)
                        defaultValue = NormalizeDefaultConstraintValue(column.DefaultConstraintValue);
                        // Preserve constraint name if specified (e.g., when restoring in down scripts), otherwise generate deterministic name
                        constraintName = !string.IsNullOrWhiteSpace(column.DefaultConstraintName)
                            ? column.DefaultConstraintName
                            : $"DF_{tableName}_{column.Name}_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(tableName, column.Name, defaultValue, "add")}";
                    }
                    else
                    {
                        // No existing default constraint - generate one based on type (temporary)
                        // This is needed because SQL Server requires a default when adding NOT NULL columns to existing tables
                        defaultValue = GetDefaultValueForType(column.SqlType);
                        constraintName = $"DF_{tableName}_{column.Name}_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(tableName, column.Name, defaultValue, "add", "temp")}";
                    }
                    
                    // Add a DEFAULT constraint
                    // If the column originally had a default constraint, this will persist
                    // If not, this is temporary and should be dropped after the column is added (if table is empty)
                    sb.Append($" NOT NULL CONSTRAINT [{constraintName}] DEFAULT {defaultValue}");
                    
                    // If the original column didn't have a default constraint, drop the temporary one
                    // Note: We can only drop it if the table is empty, otherwise SQL Server requires it for NOT NULL columns
                    // Check if table is empty first, then drop the constraint if it is
                    if (string.IsNullOrWhiteSpace(column.DefaultConstraintValue))
                    {
                        sb.AppendLine(";");
                        sb.AppendLine($"-- Drop temporary default constraint (original column didn't have one)");
                        sb.AppendLine($"-- Only drop if table is empty, otherwise SQL Server requires it for NOT NULL columns");
                        // Use the same suffix inputs as the constraint name for consistency
                        string dropVarSuffix = MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(tableName, column.Name, defaultValue, "add", "temp");
                        sb.AppendLine($"DECLARE @rowCount_{dropVarSuffix} INT;");
                        sb.AppendLine($"SELECT @rowCount_{dropVarSuffix} = COUNT(*) FROM [{schema}].[{tableName}];");
                        sb.AppendLine($"IF @rowCount_{dropVarSuffix} = 0");
                        sb.AppendLine($"BEGIN");
                        sb.Append(GenerateDropDefaultConstraintStatement(column.Name, tableName, dropVarSuffix, schema).TrimEnd());
                        sb.AppendLine();
                        sb.AppendLine($"END");
                        return sb.ToString();
                    }
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
        string varSuffix = variableSuffix ?? MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(schema, tableName, columnName, "dropdefault");
        
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
        string varSuffix = MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(schema, tableName, columnName, "dropcolumn");
        
        StringBuilder sb = new StringBuilder();
        sb.Append(GenerateDropDefaultConstraintStatement(columnName, tableName, varSuffix, schema));
        sb.AppendLine($"ALTER TABLE [{schema}].[{tableName}] DROP COLUMN [{columnName}];");
        return sb.ToString();
    }

    public static string GenerateAlterColumnStatement(SqlTableColumn oldColumn, SqlTableColumn newColumn, string tableName, string schema = "dbo")
    {
        StringBuilder sb = new StringBuilder();

        if (newColumn.IsComputed && !string.IsNullOrWhiteSpace(newColumn.ComputedSql))
        {
            // Computed columns need to be dropped and recreated
            // This is a limitation - we'll handle it as DROP + ADD
            return string.Empty; // Signal that this needs special handling
        }

        // Check if we're changing from nullable to NOT NULL
        bool changingToNotNull = oldColumn.IsNullable && !newColumn.IsNullable;
        bool changingToNullable = !oldColumn.IsNullable && newColumn.IsNullable;
        
        // Check if default constraint value changed
        string? oldDefaultValue = NormalizeDefaultConstraintValue(oldColumn.DefaultConstraintValue ?? string.Empty);
        string? newDefaultValue = NormalizeDefaultConstraintValue(newColumn.DefaultConstraintValue ?? string.Empty);
        bool defaultValueChanged = oldDefaultValue != newDefaultValue;
        bool defaultAdded = string.IsNullOrWhiteSpace(oldColumn.DefaultConstraintValue) && !string.IsNullOrWhiteSpace(newColumn.DefaultConstraintValue);
        bool defaultRemoved = !string.IsNullOrWhiteSpace(oldColumn.DefaultConstraintValue) && string.IsNullOrWhiteSpace(newColumn.DefaultConstraintValue);
        
        string? constraintName = null;
        bool isTemporaryConstraint = false;
        
        // Always assume tables may have data - we can't query the database during migration generation
        // SQL Server requires dropping default constraints before ALTER COLUMN, even if the default value isn't changing
        // Check if old column has a default constraint that needs to be dropped before ALTER COLUMN
        bool oldColumnHasDefault = !string.IsNullOrWhiteSpace(oldColumn.DefaultConstraintValue);
        bool newColumnHasDefault = !string.IsNullOrWhiteSpace(newColumn.DefaultConstraintValue);
        bool needsToDropDefaultBeforeAlter = oldColumnHasDefault && !changingToNotNull && !changingToNullable && !newColumn.IsIdentity;
        
        // When changing from nullable to NOT NULL, we need to:
        // 1. Drop any existing DEFAULT constraint (always drop to be safe - constraint may exist from previous migration steps)
        // 2. Add a DEFAULT constraint (temporary if column doesn't have one)
        // 3. Update existing NULL values to the default
        // 4. Alter column to NOT NULL
        // 5. Drop the temporary DEFAULT constraint if it was temporary
        // When default constraint value changes or is added/removed, we need to:
        // 1. Drop old default constraint if it exists
        // 2. Add new default constraint if needed
        if (changingToNotNull && !newColumn.IsIdentity)
        {
            // Always drop any existing default constraint first (may exist from previous migration steps or schema)
            // GenerateDropDefaultConstraintStatement uses dynamic SQL to safely check and drop if exists
            sb.Append(GenerateDropDefaultConstraintStatement(newColumn.Name, tableName, null, schema));
            
            string defaultValue;
            isTemporaryConstraint = string.IsNullOrWhiteSpace(newColumn.DefaultConstraintValue);
            
            if (!string.IsNullOrWhiteSpace(newColumn.DefaultConstraintValue))
            {
                // Column has a default constraint - use it
                defaultValue = NormalizeDefaultConstraintValue(newColumn.DefaultConstraintValue);
                // Preserve constraint name: use newColumn's name if specified, otherwise try to preserve oldColumn's name, otherwise generate deterministic
                constraintName = !string.IsNullOrWhiteSpace(newColumn.DefaultConstraintName)
                    ? newColumn.DefaultConstraintName
                    : (!string.IsNullOrWhiteSpace(oldColumn.DefaultConstraintName)
                        ? oldColumn.DefaultConstraintName
                        : $"DF_{tableName}_{newColumn.Name}_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(tableName, newColumn.Name, defaultValue, "alter")}");
            }
            else
            {
                // No default constraint - generate one based on type (temporary)
                defaultValue = GetDefaultValueForType(newColumn.SqlType);
                constraintName = $"DF_{tableName}_{newColumn.Name}_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(tableName, newColumn.Name, defaultValue, "alter", "temp")}";
            }
            
            // Add DEFAULT constraint
            sb.AppendLine($"ALTER TABLE [{schema}].[{tableName}] ADD CONSTRAINT [{constraintName}] DEFAULT {defaultValue} FOR [{newColumn.Name}];");
            
            // Update existing NULL values to the default
            sb.AppendLine($"UPDATE [{schema}].[{tableName}] SET [{newColumn.Name}] = {defaultValue} WHERE [{newColumn.Name}] IS NULL;");
        }
        
        // Drop default constraint before ALTER COLUMN if old column has one and we're not already handling it above
        // This is required by SQL Server even if the default value isn't changing
        if (needsToDropDefaultBeforeAlter)
        {
            sb.Append(GenerateDropDefaultConstraintStatement(newColumn.Name, tableName, null, schema));
        }

        sb.Append($"ALTER TABLE [{schema}].[{tableName}] ALTER COLUMN [");
        sb.Append(newColumn.Name);
        sb.Append("] ");

        string sqlType = newColumn.SqlType.ToSqlDbType();
        sb.Append(sqlType.ToUpperInvariant());

        // Add length/precision/scale for types that need it
        if (sqlType.ToLowerInvariant() is "decimal" or "numeric")
        {
            // For DECIMAL/NUMERIC, if Scale has a value, we should always include precision/scale
            // Default precision to 18 (SQL Server default) if it's null but scale is present
            if (newColumn.Scale.HasValue)
            {
                int precision = newColumn.Precision ?? 18; // Default to 18 if precision is null
                sb.Append($"({precision},{newColumn.Scale.Value})");
            }
            else if (newColumn.Precision.HasValue)
            {
                sb.Append($"({newColumn.Precision.Value})");
            }
        }
        else if (sqlType.ToLowerInvariant() is "varchar" or "nvarchar" or "char" or "nchar" or "varbinary" or "binary")
        {
            if (newColumn.Length.HasValue)
            {
                if (newColumn.Length.Value == -1)
                {
                    sb.Append("(MAX)");
                }
                else
                {
                    sb.Append($"({newColumn.Length.Value})");
                }
            }
        }
        else if (sqlType.ToLowerInvariant() is "datetime2" or "time" or "datetimeoffset")
        {
            if (newColumn.Precision.HasValue)
            {
                sb.Append($"({newColumn.Precision.Value})");
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
        
        // Drop temporary DEFAULT constraint if we added one
        if (changingToNotNull && !newColumn.IsIdentity && isTemporaryConstraint && constraintName != null)
        {
            sb.AppendLine($"ALTER TABLE [{schema}].[{tableName}] DROP CONSTRAINT [{constraintName}];");
        }
        
        // Handle default constraint changes (value changes, additions, removals) when nullability doesn't change
        // Note: If we already dropped the default constraint above (needsToDropDefaultBeforeAlter), we need to re-add it
        if (!changingToNotNull && !changingToNullable && !newColumn.IsIdentity)
        {
            if (defaultRemoved || defaultValueChanged || defaultAdded || needsToDropDefaultBeforeAlter)
            {
                // Drop old default constraint if it exists (if not already dropped above)
                if (!needsToDropDefaultBeforeAlter)
                {
                sb.Append(GenerateDropDefaultConstraintStatement(newColumn.Name, tableName, null, schema));
                }
                
                // Add new default constraint if needed (or re-add if we dropped it above)
                if (!defaultRemoved && !string.IsNullOrWhiteSpace(newColumn.DefaultConstraintValue))
                {
                    string defaultValue = NormalizeDefaultConstraintValue(newColumn.DefaultConstraintValue);
                    // Use newColumn's constraint name if specified, otherwise try to preserve oldColumn's name, otherwise generate deterministic
                    // When needsToDropDefaultBeforeAlter is true, we're just dropping/re-adding due to ALTER COLUMN requirements,
                    // so preserve the old constraint name if the default value hasn't changed
                    string newConstraintName = !string.IsNullOrWhiteSpace(newColumn.DefaultConstraintName)
                        ? newColumn.DefaultConstraintName
                        : (!string.IsNullOrWhiteSpace(oldColumn.DefaultConstraintName) && !defaultValueChanged
                            ? oldColumn.DefaultConstraintName
                            : $"DF_{tableName}_{newColumn.Name}_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(tableName, newColumn.Name, defaultValue, "alter")}");
                    
                    sb.AppendLine($"ALTER TABLE [{schema}].[{tableName}] ADD CONSTRAINT [{newConstraintName}] DEFAULT {defaultValue} FOR [{newColumn.Name}];");
                }
            }
        }
        
        // Handle default constraint removal when changing to nullable
        if (changingToNullable && !string.IsNullOrWhiteSpace(oldColumn.DefaultConstraintValue))
        {
            // Drop old default constraint when changing to nullable
            sb.Append(GenerateDropDefaultConstraintStatement(newColumn.Name, tableName, null, schema));
        }
        
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
        
        // Remove columns being rebuilt (always DROP+ADD)
        foreach (ColumnChange change in tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Rebuild && c.OldColumn != null))
        {
            remainingColumns.Remove(change.OldColumn!.Name);
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
        
        // Add columns being rebuilt (always re-added)
        foreach (ColumnChange change in tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Rebuild && c.NewColumn != null))
        {
            remainingColumns.Add(change.NewColumn!.Name);
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
        
        // Check if this column is being rebuilt
        ColumnChange? rebuildChange = tableDiff.ColumnChanges.FirstOrDefault(c => 
            c.ChangeType == ColumnChangeType.Rebuild && 
            c.OldColumn != null && 
            string.Equals(c.OldColumn.Name, columnName, StringComparison.OrdinalIgnoreCase));
        
        if (rebuildChange != null)
        {
            // Temporarily remove this column from count to check if it's the only one
            HashSet<string> tempRemaining = new HashSet<string>(remainingColumns, StringComparer.OrdinalIgnoreCase);
            tempRemaining.Remove(columnName);
            return tempRemaining.Count == 0;
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
        
        // Check if this is an identity-to-non-identity conversion that needs data preservation
        bool isIdentityToNonIdentity = oldColumn.IsIdentity && !newColumn.IsIdentity;
        
        // Use direct check if available, otherwise fall back to diff-based check
        bool needsSafeHandling = isOnlyColumn || isOnlyColumnByDiff || wouldReduceToZero;
        
        // For identity-to-non-identity conversions, we need to preserve existing ID values
        // This requires special handling even if not the only column
        if (isIdentityToNonIdentity && sameName)
        {
            // Identity to non-identity with same name: preserve existing values
            string tempColumnName = $"{newColumn.Name}_tmp_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(schema, tableName, newColumn.Name, "tmp")}";
            SqlTableColumn tempColumn = newColumn with { Name = tempColumnName };
            
            // Add temp column with DEFAULT (required for NOT NULL on non-empty table)
            sb.Append(GenerateAddColumnStatement(tempColumn, tableName, schema));
            
            // Copy values from old column to temp column (use dynamic SQL to ensure separate batch)
            sb.AppendLine($"-- Copy values from {oldColumn.Name} to {tempColumnName}");
            string updateSuffix = MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(schema, tableName, tempColumnName, "update");
            sb.AppendLine($"DECLARE @updateSql_{updateSuffix} NVARCHAR(MAX) = N'UPDATE [{schema}].[{tableName}] SET [{tempColumnName}] = [{oldColumn.Name}];';");
            sb.AppendLine($"EXEC sp_executesql @updateSql_{updateSuffix};");
            sb.AppendLine();
            
            // Drop the default constraint on temp column (no longer needed after copying values)
            string tempDefaultSuffix = MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(schema, tableName, tempColumnName, "drop_temp_default");
            sb.AppendLine($"-- Drop temporary default constraint");
            sb.AppendLine($"DECLARE @constraintName_{tempDefaultSuffix} NVARCHAR(128);");
            sb.AppendLine($"SELECT @constraintName_{tempDefaultSuffix} = name FROM sys.default_constraints ");
            sb.AppendLine($"WHERE parent_object_id = OBJECT_ID('[{schema}].[{tableName}]') ");
            sb.AppendLine($"AND parent_column_id = COLUMNPROPERTY(OBJECT_ID('[{schema}].[{tableName}]'), '{tempColumnName}', 'ColumnId');");
            sb.AppendLine($"IF @constraintName_{tempDefaultSuffix} IS NOT NULL");
            sb.AppendLine($"    EXEC('ALTER TABLE [{schema}].[{tableName}] DROP CONSTRAINT [' + @constraintName_{tempDefaultSuffix} + ']');");
            sb.AppendLine();
            
            // Drop old column
            sb.AppendLine(GenerateDropColumnStatement(oldColumn.Name, tableName, schema));
            
            // Rename temp column to final name
            sb.AppendLine(GenerateRenameColumnStatement(tableName, tempColumnName, newColumn.Name, schema));
            
            // After renaming, if the new column shouldn't have a default constraint (but the temp column had one),
            // drop the temporary constraint. The constraint is now associated with the renamed column.
            // Check if temp column would have gotten a default constraint but new column shouldn't have one
            // We always add a default constraint for NOT NULL non-identity columns, so check if temp column got one
            bool tempColumnNeedsDefault = !tempColumn.IsIdentity && string.IsNullOrWhiteSpace(tempColumn.DefaultConstraintValue);
            bool newColumnShouldHaveDefault = !string.IsNullOrWhiteSpace(newColumn.DefaultConstraintValue);
            
            if (tempColumnNeedsDefault && !newColumnShouldHaveDefault)
            {
                // Temp column got a default constraint, but new column shouldn't have one - drop it
                // After renaming, the constraint is associated with the new column name, so use that
                sb.Append(GenerateDropDefaultConstraintStatement(newColumn.Name, tableName, null, schema));
            }
        }
        else if (needsSafeHandling)
        {
            // Unsafe scenario: need to add first, then drop, then rename if needed
            if (sameName)
            {
                // Same name: add with temporary name, drop old, rename
                string tempColumnName = $"{newColumn.Name}_tmp_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(schema, tableName, newColumn.Name, "tmp")}";
                SqlTableColumn tempColumn = newColumn with { Name = tempColumnName };
                
                // Add new column with temporary name
                sb.Append(GenerateAddColumnStatement(tempColumn, tableName, schema));
                
                // Drop old column
                sb.AppendLine(GenerateDropColumnStatement(oldColumn.Name, tableName, schema));
                
                // Rename temporary column to final name
                sb.AppendLine(GenerateRenameColumnStatement(tableName, tempColumnName, newColumn.Name, schema));
                
                // After renaming, if the new column shouldn't have a default constraint (but the temp column had one),
                // drop the temporary constraint. The constraint is now associated with the renamed column.
                // Check if temp column would have gotten a default constraint but new column shouldn't have one
                // We always add a default constraint for NOT NULL non-identity columns, so check if temp column got one
                bool tempColumnNeedsDefault = !tempColumn.IsIdentity && string.IsNullOrWhiteSpace(tempColumn.DefaultConstraintValue);
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
                sb.Append(GenerateAddColumnStatement(newColumn, tableName, schema));
                sb.AppendLine(GenerateDropColumnStatement(oldColumn.Name, tableName, schema));
            }
        }
        else
        {
            // Safe scenario: can drop then add (existing behavior)
            sb.AppendLine(GenerateDropColumnStatement(oldColumn.Name, tableName, schema));
            sb.Append(GenerateAddColumnStatement(newColumn, tableName, schema));
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
                        sb.AppendLine(GenerateIndexes.GenerateDropIndexStatement(index));
                    }
                }
            }
        }

        // Pre-calculate lists for processing
        List<ColumnChange> dropChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Drop).ToList();
        List<ColumnChange> addChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Add).ToList();
        List<ColumnChange> modifyChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Modify).ToList();
        List<ColumnChange> rebuildChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Rebuild).ToList();
        
        // Check if dropping columns would reduce table to 0 columns
        // If so, we need to ensure at least one column is added first (from Add operations)
        bool wouldReduceToZero = WouldReduceToZeroColumns(tableDiff, currentSchema);
        
        // Check if we have Modify/Rebuild changes that require DROP+ADD on the only column, AND we have Add changes
        // In this case, we need to add columns FIRST before modifying the only column
        // We check the TARGET schema (before migration) to see if it has only 1 column
        bool needsAddBeforeModify = false;
        if ((modifyChanges.Count > 0 || rebuildChanges.Count > 0) && addChanges.Count > 0 && targetSchema != null)
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
                
                // Check if any Rebuild change is on the only column in TARGET schema
                if (!needsAddBeforeModify)
                {
                    foreach (ColumnChange rebuildChange in rebuildChanges)
                    {
                        if (rebuildChange.OldColumn != null && 
                            dataColumnCount == 1 && 
                            targetTableForCheck.Columns.TryGetValue(rebuildChange.OldColumn.Name.ToLowerInvariant(), out SqlTableColumn? col) &&
                            !col.IsComputed)
                        {
                            needsAddBeforeModify = true;
                            break;
                        }
                    }
                }
            }
        }
        
        if (wouldReduceToZero && dropChanges.Count > 0)
        {
            // Need to add columns first before dropping to avoid zero-column state
            // Add columns from Add operations first
            foreach (ColumnChange change in addChanges)
            {
                if (change.NewColumn != null)
                {
                    sb.Append(GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName, schema));
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
                    sb.Append(GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName, schema));
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

        // Handle Rebuild changes (require DROP+ADD)
        foreach (ColumnChange change in rebuildChanges)
        {
            if (change.OldColumn != null && change.NewColumn != null)
            {
                // Check TARGET schema (before migration) to see if this was the only column
                bool isSingleColumnRebuild = needsAddBeforeModify && targetSchema != null &&
                    targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTableForCheck) &&
                    targetTableForCheck.Columns.Values.Count(c => !c.IsComputed) == 1 &&
                    targetTableForCheck.Columns.TryGetValue(change.OldColumn.Name.ToLowerInvariant(), out SqlTableColumn? col) &&
                    !col.IsComputed;
                
                if (isSingleColumnRebuild)
                {
                    // Columns already added first, safe to use normal DROP+ADD
                    sb.AppendLine(GenerateDropColumnStatement(change.OldColumn.Name, tableDiff.TableName, schema));
                    sb.Append(GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName, schema));
                }
                else
                {
                    // Use safe wrapper for rebuild changes
                    sb.Append(GenerateSafeColumnDropAndAdd(change.OldColumn, change.NewColumn, tableDiff.TableName, schema, tableDiff, currentSchema));
                }
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
                
                bool handledViaDropAdd = false;
                
                // Check if it's a computed column change - these need DROP + ADD
                if (change.OldColumn.IsComputed || change.NewColumn.IsComputed)
                {
                    handledViaDropAdd = true;
                    // Use safe wrapper for computed column changes (unless we've already added columns)
                    if (isSingleColumnModify)
                    {
                        // Columns already added first, safe to use normal DROP+ADD
                        sb.AppendLine(GenerateDropColumnStatement(change.OldColumn.Name, tableDiff.TableName, schema));
                        sb.Append(GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName, schema));
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
                    handledViaDropAdd = true;
                    // Use safe wrapper for identity changes (unless we've already added columns)
                    if (isSingleColumnModify)
                    {
                        // Columns already added first, safe to use normal DROP+ADD
                        sb.AppendLine(GenerateDropColumnStatement(change.OldColumn.Name, tableDiff.TableName, schema));
                        sb.Append(GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName, schema));
                    }
                    else
                    {
                        // Use safe wrapper for identity changes
                        sb.Append(GenerateSafeColumnDropAndAdd(change.OldColumn, change.NewColumn, tableDiff.TableName, schema, tableDiff, currentSchema));
                    }
                }
                
                // If we handled this via DROP+ADD, skip the ALTER COLUMN logic (column was already recreated with correct properties)
                if (handledViaDropAdd)
                {
                    continue;
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
                            : $"DF_{tableDiff.TableName}_{change.NewColumn.Name}_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(tableDiff.TableName, change.NewColumn.Name, normalizedValue, "modify")}";
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
                        // BUT: Skip if we're changing nullable to NOT NULL - GenerateAlterColumnStatement already handles that
                        bool changingToNotNull = change.OldColumn.IsNullable && !change.NewColumn.IsNullable;
                        bool changingToNullable = !change.OldColumn.IsNullable && change.NewColumn.IsNullable;
                        
                        if (defaultConstraintChanged && !changingToNotNull && !changingToNullable)
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
                                    : $"DF_{tableDiff.TableName}_{change.NewColumn.Name}_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(tableDiff.TableName, change.NewColumn.Name, normalizedValue, "modify")}";
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
                    sb.Append(GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName, schema));
                }
            }
        }

        return sb.ToString();
    }
}

