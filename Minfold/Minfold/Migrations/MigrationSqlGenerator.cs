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
    public static string GenerateDropDefaultConstraintStatement(string columnName, string tableName, string? variableSuffix = null, string schema = "dbo")
    {
        // Use provided suffix or generate a GUID-based one to avoid conflicts
        string varSuffix = variableSuffix ?? Guid.NewGuid().ToString("N");
        
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
        // Use a GUID-based variable name to avoid conflicts when multiple columns are dropped in the same batch
        string varSuffix = Guid.NewGuid().ToString("N"); // GUID without dashes
        
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
        
        // Use direct check if available, otherwise fall back to diff-based check
        bool needsSafeHandling = isOnlyColumn || isOnlyColumnByDiff || wouldReduceToZero;
        
        if (needsSafeHandling)
        {
            // Unsafe scenario: need to add first, then drop, then rename if needed
            if (sameName)
            {
                // Same name: add with temporary name, drop old, rename
                string tempColumnName = $"{newColumn.Name}_tmp_{Guid.NewGuid():N}";
                SqlTableColumn tempColumn = newColumn with { Name = tempColumnName };
                
                // Add new column with temporary name
                sb.Append(GenerateAddColumnStatement(tempColumn, tableName, schema));
                
                // Drop old column
                sb.AppendLine(GenerateDropColumnStatement(oldColumn.Name, tableName, schema));
                
                // Rename temporary column to final name
                sb.AppendLine(GenerateRenameColumnStatement(tableName, tempColumnName, newColumn.Name, schema));
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
                    string alterSql = GenerateAlterColumnStatement(change.OldColumn, change.NewColumn, tableDiff.TableName, schema);
                    if (!string.IsNullOrEmpty(alterSql))
                    {
                        sb.Append(alterSql);
                    }
                }
                else
                {
                    string alterSql = GenerateAlterColumnStatement(change.OldColumn, change.NewColumn, tableDiff.TableName, schema);
                    if (!string.IsNullOrEmpty(alterSql))
                    {
                        sb.Append(alterSql);
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

    public static string GenerateForeignKeyStatement(List<SqlForeignKey> fkGroup, Dictionary<string, SqlTable> allTables)
    {
        if (fkGroup.Count == 0)
        {
            return string.Empty;
        }

        SqlForeignKey firstFk = fkGroup[0];
        StringBuilder sb = new StringBuilder();

        sb.Append($"ALTER TABLE [{firstFk.Schema}].[{firstFk.Table}] WITH ");
        sb.Append(firstFk.NotEnforced ? "NOCHECK" : "CHECK");
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

        // Enable constraint if it was enforced
        if (!firstFk.NotEnforced)
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
        // Use provided suffix or generate a GUID-based one to avoid conflicts
        string varSuffix = variableSuffix ?? Guid.NewGuid().ToString("N");
        
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
}

