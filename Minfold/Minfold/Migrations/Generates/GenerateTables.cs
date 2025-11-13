using System.Collections.Concurrent;
using System.Text;

namespace Minfold;

public static class GenerateTables
{
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

                // Add length/precision/scale for types that need it
                if (sqlType.ToLowerInvariant() is "decimal" or "numeric")
                {
                    MigrationLogger.Log($"  GenerateCreateTableStatement: Column {col.Name} (DECIMAL/NUMERIC) - Precision={col.Precision?.ToString() ?? "null"}, Scale={col.Scale?.ToString() ?? "null"}");
                    
                    // For DECIMAL/NUMERIC, if Scale has a value, we should always include precision/scale
                    // Default precision to 18 (SQL Server default) if it's null but scale is present
                    if (col.Scale.HasValue)
                    {
                        int precision = col.Precision ?? 18; // Default to 18 if precision is null
                        sb.Append($"({precision},{col.Scale.Value})");
                        MigrationLogger.Log($"    Added precision/scale: ({precision},{col.Scale.Value})");
                    }
                    else if (col.Precision.HasValue)
                    {
                        sb.Append($"({col.Precision.Value})");
                        MigrationLogger.Log($"    Added precision only: ({col.Precision.Value})");
                    }
                    else
                    {
                        MigrationLogger.Log($"    WARNING: No precision/scale for DECIMAL column {col.Name} - SQL Server will default to DECIMAL(18,0)");
                    }
                }
                else if (sqlType.ToLowerInvariant() is "varchar" or "nvarchar" or "char" or "nchar" or "varbinary" or "binary")
                {
                    if (col.Length.HasValue)
                    {
                        if (col.Length.Value == -1)
                        {
                            sb.Append("(MAX)");
                        }
                        else
                        {
                            sb.Append($"({col.Length.Value})");
                        }
                    }
                }
                else if (sqlType.ToLowerInvariant() is "datetime2" or "time" or "datetimeoffset")
                {
                    if (col.Precision.HasValue)
                    {
                        sb.Append($"({col.Precision.Value})");
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
                        : $"DF_{table.Name}_{col.Name}_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(table.Name, col.Name, GenerateColumns.NormalizeDefaultConstraintValue(col.DefaultConstraintValue), "create")}";
                    // Normalize the default value (remove outer parentheses that SQL Server adds)
                    string normalizedValue = GenerateColumns.NormalizeDefaultConstraintValue(col.DefaultConstraintValue);
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
            pkSql = GeneratePrimaryKeys.GenerateAddPrimaryKeyStatement(desiredTable.Name, pkColumnNames, pkConstraintName, desiredTable.Schema);
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
            indexSql.Add(GenerateIndexes.GenerateCreateIndexStatement(index));
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
            string fkSql = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDict, forceNoCheck: true);
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
                string fkSqlWithCheck = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDict, forceNoCheck: false);
                if (!string.IsNullOrEmpty(fkSqlWithCheck))
                {
                    constraintSql.Add(fkSqlWithCheck);
                }
            }
        }
        
        // Generate column reordering SQL
        // Approach: Create temp table with correct order, copy data, drop old, rename temp
        StringBuilder sb = new StringBuilder();
        string tempTableName = $"{actualTable.Name}_reorder_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, "reorder")}";
        
        sb.AppendLine("-- Reorder columns to match target schema");
        sb.AppendLine($"-- Creating temporary table with correct column order");
        
        // Check if table has IDENTITY columns for IDENTITY_INSERT handling
        bool hasIdentityColumns = desiredColumns.Any(c => c.IsIdentity);
        
        // Build CREATE TABLE statement - use actual columns from database to preserve precision/scale
        Dictionary<string, SqlTableColumn> actualColumnsByName = actualColumns
            .ToDictionary(c => c.Name.ToLowerInvariant(), c => c, StringComparer.OrdinalIgnoreCase);
        
        // Build CREATE TABLE statement - use actual columns to preserve precision/scale for DECIMAL
        sb.AppendLine($"CREATE TABLE [{actualTable.Schema}].[{tempTableName}] (");
        
        List<string> columnDefParts = new List<string>();
        for (int i = 0; i < desiredColumns.Count; i++)
        {
            SqlTableColumn desiredCol = desiredColumns[i];
            
            // Use actual column from database for DECIMAL/NUMERIC to preserve precision/scale
            SqlTableColumn col = desiredCol;
            if (actualColumnsByName.TryGetValue(desiredCol.Name.ToLowerInvariant(), out SqlTableColumn? actualCol))
            {
                // For DECIMAL/NUMERIC, use actual column to preserve precision/scale
                if (desiredCol.SqlType == SqlDbTypeExt.Decimal || desiredCol.SqlType == SqlDbTypeExt.Numeric)
                {
                    col = actualCol; // Use actual column completely to preserve precision/scale
                }
            }
            
            StringBuilder colDef = new StringBuilder();
            colDef.Append($"    [{col.Name}] ");
            
            if (col.IsComputed && !string.IsNullOrWhiteSpace(col.ComputedSql))
            {
                colDef.Append("AS ");
                colDef.Append(col.ComputedSql);
            }
            else
            {
                string sqlType = col.SqlType.ToSqlDbType();
                colDef.Append(sqlType.ToUpperInvariant());
                
                // Add length/precision/scale for types that need it
                if (sqlType.ToLowerInvariant() is "decimal" or "numeric")
                {
                    // For DECIMAL/NUMERIC, if Scale has a value, we should always include precision/scale
                    // Default precision to 18 (SQL Server default) if it's null but scale is present
                    if (col.Scale.HasValue)
                    {
                        int precision = col.Precision ?? 18; // Default to 18 if precision is null
                        colDef.Append($"({precision},{col.Scale.Value})");
                    }
                    else if (col.Precision.HasValue)
                    {
                        colDef.Append($"({col.Precision.Value})");
                    }
                }
                else if (sqlType.ToLowerInvariant() is "varchar" or "nvarchar" or "char" or "nchar" or "varbinary" or "binary")
                {
                    if (col.Length.HasValue)
                    {
                        if (col.Length.Value == -1)
                        {
                            colDef.Append("(MAX)");
                        }
                        else
                        {
                            colDef.Append($"({col.Length.Value})");
                        }
                    }
                }
                else if (sqlType.ToLowerInvariant() is "datetime2" or "time" or "datetimeoffset")
                {
                    if (col.Precision.HasValue)
                    {
                        colDef.Append($"({col.Precision.Value})");
                    }
                }
                
                if (!col.IsNullable)
                {
                    colDef.Append(" NOT NULL");
                }
                
                if (!string.IsNullOrWhiteSpace(col.DefaultConstraintValue))
                {
                    string normalizedValue = GenerateColumns.NormalizeDefaultConstraintValue(col.DefaultConstraintValue);
                    string tempConstraintName = $"DF_{actualTable.Name}_{col.Name}_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, col.Name, normalizedValue, "reorder", "temp")}";
                    colDef.Append($" CONSTRAINT [{tempConstraintName}] DEFAULT {normalizedValue}");
                }
                
                if (col.IsIdentity)
                {
                    colDef.Append(" IDENTITY");
                    if (col.IdentitySeed.HasValue && col.IdentityIncrement.HasValue)
                    {
                        colDef.Append($"({col.IdentitySeed.Value},{col.IdentityIncrement.Value})");
                    }
                    else
                    {
                        colDef.Append("(1,1)");
                    }
                }
            }
            
            columnDefParts.Add(colDef.ToString());
        }
        
        sb.AppendLine(string.Join(",\r\n", columnDefParts));
        sb.AppendLine(");");
        sb.AppendLine();
        
        // Before creating the temp table, drop any default constraints that might conflict
        // This handles cases where constraint names from targetSchema might already exist
        foreach (SqlTableColumn col in desiredColumns)
        {
            if (!string.IsNullOrWhiteSpace(col.DefaultConstraintName) && !string.IsNullOrWhiteSpace(col.DefaultConstraintValue))
            {
                string constraintNameVar = $"@constraintName_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, col.Name, "reorder", "check")}";
                sb.AppendLine($"-- Drop default constraint if it exists (may conflict with temp table creation)");
                sb.AppendLine($"DECLARE {constraintNameVar} NVARCHAR(128);");
                sb.AppendLine($"SELECT {constraintNameVar} = name FROM sys.default_constraints WHERE name = '{col.DefaultConstraintName.Replace("'", "''")}';");
                sb.AppendLine($"IF {constraintNameVar} IS NOT NULL");
                sb.AppendLine($"BEGIN");
                sb.AppendLine($"    -- Find the table that owns this constraint and drop it");
                sb.AppendLine($"    DECLARE @dropConstraintSql_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, col.Name, "reorder", "drop")} NVARCHAR(MAX);");
                sb.AppendLine($"    SELECT @dropConstraintSql_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, col.Name, "reorder", "drop")} = 'ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + '].[' + OBJECT_NAME(parent_object_id) + '] DROP CONSTRAINT [' + name + ']'");
                sb.AppendLine($"    FROM sys.default_constraints WHERE name = {constraintNameVar};");
                sb.AppendLine($"    IF @dropConstraintSql_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, col.Name, "reorder", "drop")} IS NOT NULL");
                sb.AppendLine($"        EXEC sp_executesql @dropConstraintSql_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, col.Name, "reorder", "drop")};");
                sb.AppendLine($"END");
            }
        }
        
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
        string tableObjectIdVar = $"@tableObjectId_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, "reorder", "objectid")}";
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
        string varSuffix = MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, "reorder", "insertsql");
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
        
        // Restore original constraint names if they differ from temp names
        // This ensures the final table has the same constraint names as the target schema
        foreach (SqlTableColumn col in desiredColumns)
        {
            if (!string.IsNullOrWhiteSpace(col.DefaultConstraintName) && !string.IsNullOrWhiteSpace(col.DefaultConstraintValue))
            {
                string normalizedValue = GenerateColumns.NormalizeDefaultConstraintValue(col.DefaultConstraintValue);
                string tempConstraintName = $"DF_{actualTable.Name}_{col.Name}_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, col.Name, normalizedValue, "reorder", "temp")}";
                
                // Only rename if the temp name differs from the original name
                if (!tempConstraintName.Equals(col.DefaultConstraintName, StringComparison.OrdinalIgnoreCase))
                {
                    sb.AppendLine($"-- Restore original constraint name for {col.Name}");
                    sb.AppendLine($"DECLARE @tempConstraintName_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, col.Name, "reorder", "rename")} NVARCHAR(128) = '{tempConstraintName}';");
                    sb.AppendLine($"DECLARE @originalConstraintName_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, col.Name, "reorder", "rename")} NVARCHAR(128) = '{col.DefaultConstraintName.Replace("'", "''")}';");
                    sb.AppendLine($"IF EXISTS (SELECT 1 FROM sys.default_constraints WHERE name = @tempConstraintName_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, col.Name, "reorder", "rename")})");
                    sb.AppendLine($"BEGIN");
                    sb.AppendLine($"    EXEC sp_rename @objname = @tempConstraintName_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, col.Name, "reorder", "rename")}, @newname = @originalConstraintName_{MigrationSqlGeneratorUtilities.GenerateDeterministicSuffix(actualTable.Schema, actualTable.Name, col.Name, "reorder", "rename")}, @objtype = 'OBJECT';");
                    sb.AppendLine($"END");
                }
            }
        }
        
        return (sb.ToString(), constraintSql);
    }
}


