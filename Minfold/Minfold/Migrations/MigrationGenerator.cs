using System.Collections.Concurrent;
using System.Text;

namespace Minfold;

public static class MigrationGenerator
{
    public static string GetMigrationsPath(string codePath)
    {
        return MigrationUtilities.GetMigrationsPath(codePath);
    }

    public static string GetNextMigrationTimestamp()
    {
        return MigrationUtilities.GetNextMigrationTimestamp();
    }

    public static string FormatMigrationScript(string script)
    {
        return MigrationUtilities.FormatMigrationScript(script);
    }

    public static async Task<ResultOrException<MigrationGenerationResult>> GenerateInitialMigration(string sqlConn, string dbName, string codePath, string description)
    {
        try
        {
            string migrationsPath = MigrationUtilities.GetMigrationsPath(codePath);

            // Check if migrations folder exists and has files
            if (Directory.Exists(migrationsPath))
            {
                string[] existingMigrations = Directory.GetFiles(migrationsPath, "*.sql", SearchOption.TopDirectoryOnly);
                if (existingMigrations.Length > 0)
                {
                    return new ResultOrException<MigrationGenerationResult>(null, new InvalidOperationException("Migrations already exist. Cannot generate initial migration."));
                }
            }

            Directory.CreateDirectory(migrationsPath);

            SqlService sqlService = new SqlService(sqlConn);
            ResultOrException<ConcurrentDictionary<string, SqlTable>> schemaResult = await sqlService.GetSchema(dbName, null, ["__MinfoldMigrations"]);

            if (schemaResult.Exception is not null || schemaResult.Result is null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, schemaResult.Exception ?? new Exception("Failed to get database schema"));
            }

            // Get foreign keys with full metadata
            ResultOrException<Dictionary<string, List<SqlForeignKey>>> fksResult = await sqlService.GetForeignKeys(schemaResult.Result.Keys.ToList());
            if (fksResult.Exception is not null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, fksResult.Exception);
            }

            // Attach foreign keys to tables
            foreach (KeyValuePair<string, List<SqlForeignKey>> fkList in fksResult.Result ?? new Dictionary<string, List<SqlForeignKey>>())
            {
                if (schemaResult.Result.TryGetValue(fkList.Key, out SqlTable? table))
                {
                    foreach (SqlForeignKey fk in fkList.Value)
                    {
                        if (table.Columns.TryGetValue(fk.Column.ToLowerInvariant(), out SqlTableColumn? column))
                        {
                            column.ForeignKeys.Add(fk);
                        }
                    }
                }
            }

            StringBuilder phase1Tables = new StringBuilder(); // CREATE TABLE statements
            StringBuilder phase2Columns = new StringBuilder(); // ALTER TABLE ADD/DROP/ALTER COLUMN statements
            StringBuilder phase3Constraints = new StringBuilder(); // ALTER TABLE FK constraints
            StringBuilder downScript = new StringBuilder();

            // Generate CREATE TABLE statements (Phase 1) - without foreign keys
            List<KeyValuePair<string, SqlTable>> tables = schemaResult.Result.OrderBy(x => x.Key).ToList();

            foreach (KeyValuePair<string, SqlTable> tablePair in tables)
            {
                string createTableSql = MigrationSqlGenerator.GenerateCreateTableStatement(tablePair.Value);
                phase1Tables.AppendLine(createTableSql);
                phase1Tables.AppendLine();
            }

            // Collect all foreign keys first, then generate ALTER TABLE statements (Phase 3)
            HashSet<string> processedFks = new HashSet<string>();
            List<List<SqlForeignKey>> allFkGroups = new List<List<SqlForeignKey>>();
            
            foreach (KeyValuePair<string, SqlTable> tablePair in tables)
            {
                foreach (SqlTableColumn column in tablePair.Value.Columns.Values)
                {
                    foreach (SqlForeignKey fk in column.ForeignKeys)
                    {
                        // Avoid duplicates (multi-column FKs appear multiple times)
                        if (!processedFks.Contains(fk.Name))
                        {
                            // Group multi-column FKs
                            List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { fk };
                            foreach (KeyValuePair<string, SqlTable> otherTablePair in tables)
                            {
                                foreach (SqlTableColumn otherColumn in otherTablePair.Value.Columns.Values)
                                {
                                    foreach (SqlForeignKey otherFk in otherColumn.ForeignKeys)
                                    {
                                        if (otherFk.Name == fk.Name && otherFk.Table == fk.Table && otherFk != fk)
                                        {
                                            fkGroup.Add(otherFk);
                                        }
                                    }
                                }
                            }

                            allFkGroups.Add(fkGroup);
                            processedFks.Add(fk.Name);
                        }
                    }
                }
            }

            // Generate all ALTER TABLE FK statements
            foreach (List<SqlForeignKey> fkGroup in allFkGroups.OrderBy(g => g[0].Table).ThenBy(g => g[0].Name))
            {
                string fkSql = MigrationSqlGenerator.GenerateForeignKeyStatement(fkGroup, new Dictionary<string, SqlTable>(schemaResult.Result));
                phase3Constraints.Append(fkSql);
                phase3Constraints.AppendLine();
            }

            // Build up script with transaction and phases (only include phases with content)
            StringBuilder upScript = new StringBuilder();
            upScript.AppendLine("SET XACT_ABORT ON;");
            upScript.AppendLine("BEGIN TRANSACTION;");
            upScript.AppendLine();
            
            int phaseNumber = 1;
            
            // Phase 1: Create Tables
            string phase1Content = phase1Tables.ToString().Trim();
            if (!string.IsNullOrEmpty(phase1Content))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Create Tables"));
                upScript.AppendLine(phase1Content);
                upScript.AppendLine();
                phaseNumber++;
            }
            
            // Phase 2: Modify Columns (ADD/DROP/ALTER COLUMN)
            string phase2Content = phase2Columns.ToString().Trim();
            if (!string.IsNullOrEmpty(phase2Content))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Modify Columns"));
                upScript.AppendLine(phase2Content);
                upScript.AppendLine();
                phaseNumber++;
            }
            
            // Phase 3: Add Foreign Key Constraints
            string phase3Content = phase3Constraints.ToString().Trim();
            if (!string.IsNullOrEmpty(phase3Content))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Add Foreign Key Constraints"));
                upScript.AppendLine(phase3Content);
                upScript.AppendLine();
            }
            
            upScript.AppendLine("COMMIT TRANSACTION;");

            // Generate DROP TABLE statements for down script (in reverse order, wrapped in transaction)
            downScript.AppendLine("SET XACT_ABORT ON;");
            downScript.AppendLine("BEGIN TRANSACTION;");
            downScript.AppendLine();
            for (int i = tables.Count - 1; i >= 0; i--)
            {
                string tableName = tables[i].Value.Name;
                downScript.AppendLine($"DROP TABLE IF EXISTS [dbo].[{tableName}];");
            }
            downScript.AppendLine();
            downScript.AppendLine("COMMIT TRANSACTION;");

            string timestamp = MigrationUtilities.GetNextMigrationTimestamp();
            string migrationName = $"{timestamp}_{description}";
            string upScriptPath = Path.Combine(migrationsPath, $"{migrationName}.sql");
            string downScriptPath = Path.Combine(migrationsPath, $"{migrationName}.down.sql");

            string finalUpScript = upScript.ToString();
            // Ensure transaction wrapper is present
            if (!finalUpScript.Contains("BEGIN TRANSACTION", StringComparison.OrdinalIgnoreCase))
            {
                finalUpScript = "SET XACT_ABORT ON;\r\nBEGIN TRANSACTION;\r\n\r\n" + finalUpScript + "\r\n\r\nCOMMIT TRANSACTION;";
            }
            
            await File.WriteAllTextAsync(upScriptPath, finalUpScript.TrimEnd());
            
            string finalDownScript = downScript.ToString();
            // Ensure transaction wrapper is present
            if (!finalDownScript.Contains("BEGIN TRANSACTION", StringComparison.OrdinalIgnoreCase))
            {
                finalDownScript = "SET XACT_ABORT ON;\r\nBEGIN TRANSACTION;\r\n\r\n" + finalDownScript + "\r\n\r\nCOMMIT TRANSACTION;";
            }
            
            await File.WriteAllTextAsync(downScriptPath, finalDownScript.TrimEnd());

            // Save schema snapshot for incremental migrations
            await MigrationSchemaSnapshot.SaveSchemaSnapshot(schemaResult.Result, migrationName, codePath);

            return new ResultOrException<MigrationGenerationResult>(
                new MigrationGenerationResult(migrationName, upScriptPath, downScriptPath, description),
                null
            );
        }
        catch (Exception ex)
        {
            return new ResultOrException<MigrationGenerationResult>(null, ex);
        }
    }

    public static async Task<ResultOrException<MigrationGenerationResult>> GenerateIncrementalMigration(string sqlConn, string dbName, string codePath, string description)
    {
        try
        {
            string migrationsPath = MigrationUtilities.GetMigrationsPath(codePath);
            Directory.CreateDirectory(migrationsPath);

            // Get current database schema
            SqlService sqlService = new SqlService(sqlConn);
            ResultOrException<ConcurrentDictionary<string, SqlTable>> currentSchemaResult = await sqlService.GetSchema(dbName, null, ["__MinfoldMigrations"]);

            if (currentSchemaResult.Exception is not null || currentSchemaResult.Result is null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, currentSchemaResult.Exception ?? new Exception("Failed to get current database schema"));
            }

            // Get applied migrations
            ResultOrException<List<string>> appliedMigrationsResult = await MigrationApplier.GetAppliedMigrations(sqlConn, dbName);
            if (appliedMigrationsResult.Exception is not null || appliedMigrationsResult.Result is null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, appliedMigrationsResult.Exception ?? new Exception("Failed to get applied migrations"));
            }

            // Get target schema from last migration snapshot
            ResultOrException<ConcurrentDictionary<string, SqlTable>> targetSchemaResult = await MigrationSchemaSnapshot.GetTargetSchemaFromMigrations(codePath, appliedMigrationsResult.Result);
            if (targetSchemaResult.Exception is not null || targetSchemaResult.Result is null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, targetSchemaResult.Exception ?? new Exception("Failed to get target schema from migrations"));
            }

            // Attach foreign keys to current schema for comparison
            ResultOrException<Dictionary<string, List<SqlForeignKey>>> currentFksResult = await sqlService.GetForeignKeys(currentSchemaResult.Result.Keys.ToList());
            if (currentFksResult.Exception is not null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, currentFksResult.Exception);
            }

            foreach (KeyValuePair<string, List<SqlForeignKey>> fkList in currentFksResult.Result ?? new Dictionary<string, List<SqlForeignKey>>())
            {
                if (currentSchemaResult.Result.TryGetValue(fkList.Key, out SqlTable? table))
                {
                    foreach (SqlForeignKey fk in fkList.Value)
                    {
                        if (table.Columns.TryGetValue(fk.Column.ToLowerInvariant(), out SqlTableColumn? column))
                        {
                            column.ForeignKeys.Add(fk);
                        }
                    }
                }
            }

            // Compare schemas: we want to find what changed from target (snapshot) to current (database)
            // So we compare target -> current, which means we pass (target, current) to CompareSchemas
            // But CompareSchemas expects (current, target) and finds what's needed to go from current to target
            // So we swap the parameters: CompareSchemas(target, current) finds what's in current but not in target (new tables)
            SchemaDiff diff = MigrationSchemaComparer.CompareSchemas(targetSchemaResult.Result, currentSchemaResult.Result);

            // Check if there are any changes
            if (diff.NewTables.Count == 0 && diff.DroppedTableNames.Count == 0 && diff.ModifiedTables.Count == 0)
            {
                return new ResultOrException<MigrationGenerationResult>(null, new InvalidOperationException("No schema changes detected. Database is already up to date."));
            }

            // Generate migration scripts
            StringBuilder phase0DropFks = new StringBuilder(); // DROP FK constraints for tables that will be dropped
            StringBuilder phase0DropTables = new StringBuilder(); // DROP TABLE statements
            StringBuilder phase1Tables = new StringBuilder(); // CREATE TABLE statements
            StringBuilder phase2Columns = new StringBuilder(); // ALTER TABLE column modifications
            StringBuilder phase3Constraints = new StringBuilder(); // ALTER TABLE FK constraints
            StringBuilder downScript = new StringBuilder();

            // Phase 0: Drop tables (drop FKs first, then tables)
            // First, collect FKs from tables that will be dropped
            foreach (string droppedTableName in diff.DroppedTableNames)
            {
                // Find the table schema from target schema to get its FKs
                if (targetSchemaResult.Result.TryGetValue(droppedTableName.ToLowerInvariant(), out SqlTable? droppedTable))
                {
                    // Drop FKs for this table
                    HashSet<string> processedFks = new HashSet<string>();
                    foreach (SqlTableColumn column in droppedTable.Columns.Values)
                    {
                        foreach (SqlForeignKey fk in column.ForeignKeys)
                        {
                            if (!processedFks.Contains(fk.Name))
                            {
                                phase0DropFks.AppendLine(MigrationSqlGenerator.GenerateDropForeignKeyStatement(fk));
                                processedFks.Add(fk.Name);
                            }
                        }
                    }
                }
            }
            
            // Then drop the tables themselves
            foreach (string droppedTableName in diff.DroppedTableNames.OrderByDescending(t => t))
            {
                phase0DropTables.AppendLine($"DROP TABLE IF EXISTS [dbo].[{droppedTableName}];");
            }

            // Phase 1: Create new tables
            foreach (SqlTable newTable in diff.NewTables.OrderBy(t => t.Name))
            {
                string createTableSql = MigrationSqlGenerator.GenerateCreateTableStatement(newTable);
                phase1Tables.AppendLine(createTableSql);
                phase1Tables.AppendLine();
            }

            // Phase 2: Column modifications
            foreach (TableDiff tableDiff in diff.ModifiedTables.OrderBy(t => t.TableName))
            {
                string columnModifications = MigrationSqlGenerator.GenerateColumnModifications(tableDiff);
                if (!string.IsNullOrWhiteSpace(columnModifications))
                {
                    phase2Columns.Append(columnModifications);
                    phase2Columns.AppendLine();
                }
            }

            // Phase 3: Foreign key constraints
            // Collect FK changes from modified tables
            List<ForeignKeyChange> allFkChanges = new List<ForeignKeyChange>();
            foreach (TableDiff tableDiff in diff.ModifiedTables)
            {
                allFkChanges.AddRange(tableDiff.ForeignKeyChanges);
            }

            // Group FK changes by constraint name for multi-column FKs
            Dictionary<string, List<ForeignKeyChange>> fkGroups = new Dictionary<string, List<ForeignKeyChange>>();
            foreach (ForeignKeyChange fkChange in allFkChanges)
            {
                string fkName = (fkChange.NewForeignKey ?? fkChange.OldForeignKey)?.Name ?? string.Empty;
                if (!fkGroups.ContainsKey(fkName))
                {
                    fkGroups[fkName] = new List<ForeignKeyChange>();
                }
                fkGroups[fkName].Add(fkChange);
            }

            // Generate FK statements: Drop first, then Add
            // Drop FKs that are being dropped or modified (need to drop old before adding new)
            foreach (KeyValuePair<string, List<ForeignKeyChange>> fkGroup in fkGroups.OrderBy(g => g.Key))
            {
                ForeignKeyChange firstChange = fkGroup.Value[0];
                if (firstChange.ChangeType == ForeignKeyChangeType.Drop && firstChange.OldForeignKey != null)
                {
                    phase3Constraints.AppendLine(MigrationSqlGenerator.GenerateDropForeignKeyStatement(firstChange.OldForeignKey));
                }
                else if (firstChange.ChangeType == ForeignKeyChangeType.Modify && firstChange.OldForeignKey != null)
                {
                    // For modifications, drop the old FK before adding the new one
                    phase3Constraints.AppendLine(MigrationSqlGenerator.GenerateDropForeignKeyStatement(firstChange.OldForeignKey));
                }
            }

            // Add new FKs from new tables
            foreach (SqlTable newTable in diff.NewTables)
            {
                HashSet<string> processedFks = new HashSet<string>();
                foreach (SqlTableColumn column in newTable.Columns.Values)
                {
                    foreach (SqlForeignKey fk in column.ForeignKeys)
                    {
                        if (!processedFks.Contains(fk.Name))
                        {
                            List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { fk };
                            // Find other columns with same FK (multi-column FK)
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
                            string fkSql = MigrationSqlGenerator.GenerateForeignKeyStatement(fkGroup, new Dictionary<string, SqlTable>(targetSchemaResult.Result));
                            phase3Constraints.Append(fkSql);
                            phase3Constraints.AppendLine();
                            processedFks.Add(fk.Name);
                        }
                    }
                }
            }

            // Add new/modified FKs from modified tables
            foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Add || c.ChangeType == ForeignKeyChangeType.Modify))
            {
                if (fkChange.NewForeignKey != null)
                {
                    // Find all columns with this FK name
                    TableDiff? tableDiff = diff.ModifiedTables.FirstOrDefault(t => t.TableName.Equals(fkChange.NewForeignKey.Table, StringComparison.OrdinalIgnoreCase));
                    if (tableDiff != null)
                    {
                        // Group multi-column FKs
                        List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { fkChange.NewForeignKey };
                        foreach (ForeignKeyChange otherFkChange in allFkChanges)
                        {
                            if (otherFkChange.NewForeignKey != null &&
                                otherFkChange.NewForeignKey.Name == fkChange.NewForeignKey.Name &&
                                otherFkChange.NewForeignKey.Table == fkChange.NewForeignKey.Table &&
                                otherFkChange.NewForeignKey != fkChange.NewForeignKey)
                            {
                                fkGroup.Add(otherFkChange.NewForeignKey);
                            }
                        }
                        string fkSql = MigrationSqlGenerator.GenerateForeignKeyStatement(fkGroup, new Dictionary<string, SqlTable>(targetSchemaResult.Result));
                        phase3Constraints.Append(fkSql);
                        phase3Constraints.AppendLine();
                    }
                }
            }

            // Generate down script
            downScript.AppendLine("SET XACT_ABORT ON;");
            downScript.AppendLine("BEGIN TRANSACTION;");
            downScript.AppendLine();

            // Reverse FK changes
            // For Modify: drop the new FK and add back the old FK
            foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Modify))
            {
                if (fkChange.NewForeignKey != null)
                {
                    downScript.AppendLine(MigrationSqlGenerator.GenerateDropForeignKeyStatement(fkChange.NewForeignKey));
                }
                if (fkChange.OldForeignKey != null)
                {
                    // Group multi-column FKs for the old FK
                    List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { fkChange.OldForeignKey };
                    foreach (ForeignKeyChange otherFkChange in allFkChanges)
                    {
                        if (otherFkChange.OldForeignKey != null &&
                            otherFkChange.OldForeignKey.Name == fkChange.OldForeignKey.Name &&
                            otherFkChange.OldForeignKey.Table == fkChange.OldForeignKey.Table &&
                            otherFkChange.OldForeignKey != fkChange.OldForeignKey)
                        {
                            fkGroup.Add(otherFkChange.OldForeignKey);
                        }
                    }
                    string fkSql = MigrationSqlGenerator.GenerateForeignKeyStatement(fkGroup, new Dictionary<string, SqlTable>(targetSchemaResult.Result));
                    downScript.Append(fkSql);
                    downScript.AppendLine();
                }
            }
            
            // Drop added FKs
            foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Add))
            {
                if (fkChange.NewForeignKey != null)
                {
                    downScript.AppendLine(MigrationSqlGenerator.GenerateDropForeignKeyStatement(fkChange.NewForeignKey));
                }
            }
            
            // Add back dropped FKs
            foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Drop))
            {
                if (fkChange.OldForeignKey != null)
                {
                    // Group multi-column FKs
                    List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { fkChange.OldForeignKey };
                    foreach (ForeignKeyChange otherFkChange in allFkChanges)
                    {
                        if (otherFkChange.OldForeignKey != null &&
                            otherFkChange.OldForeignKey.Name == fkChange.OldForeignKey.Name &&
                            otherFkChange.OldForeignKey.Table == fkChange.OldForeignKey.Table &&
                            otherFkChange.OldForeignKey != fkChange.OldForeignKey)
                        {
                            fkGroup.Add(otherFkChange.OldForeignKey);
                        }
                    }
                    string fkSql = MigrationSqlGenerator.GenerateForeignKeyStatement(fkGroup, new Dictionary<string, SqlTable>(targetSchemaResult.Result));
                    downScript.Append(fkSql);
                    downScript.AppendLine();
                }
            }

            // Reverse column modifications (in reverse order)
            foreach (TableDiff tableDiff in diff.ModifiedTables.OrderByDescending(t => t.TableName))
            {
                // Reverse: ADD becomes DROP, DROP becomes ADD, MODIFY reversed
                foreach (ColumnChange change in tableDiff.ColumnChanges.OrderByDescending(c => c.ChangeType))
                {
                    if (change.ChangeType == ColumnChangeType.Add && change.NewColumn != null)
                    {
                        downScript.AppendLine(MigrationSqlGenerator.GenerateDropColumnStatement(change.NewColumn.Name, tableDiff.TableName));
                    }
                    else if (change.ChangeType == ColumnChangeType.Drop && change.OldColumn != null)
                    {
                        downScript.Append(MigrationSqlGenerator.GenerateAddColumnStatement(change.OldColumn, tableDiff.TableName));
                    }
                    else if (change.ChangeType == ColumnChangeType.Modify && change.OldColumn != null && change.NewColumn != null)
                    {
                        string reverseAlter = MigrationSqlGenerator.GenerateAlterColumnStatement(change.NewColumn, change.OldColumn, tableDiff.TableName);
                        if (!string.IsNullOrEmpty(reverseAlter))
                        {
                            downScript.Append(reverseAlter);
                        }
                    }
                }
            }

            // Recreate dropped tables (in forward order, so dependencies are created first)
            foreach (string droppedTableName in diff.DroppedTableNames)
            {
                // Find the table schema from target schema (before migration was applied)
                if (targetSchemaResult.Result.TryGetValue(droppedTableName.ToLowerInvariant(), out SqlTable? droppedTable))
                {
                    // Generate CREATE TABLE statement
                    string createTableSql = MigrationSqlGenerator.GenerateCreateTableStatement(droppedTable);
                    downScript.AppendLine(createTableSql);
                    downScript.AppendLine();
                    
                    // Generate FK constraints for the recreated table
                    HashSet<string> processedFks = new HashSet<string>();
                    foreach (SqlTableColumn column in droppedTable.Columns.Values)
                    {
                        foreach (SqlForeignKey fk in column.ForeignKeys)
                        {
                            if (!processedFks.Contains(fk.Name))
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
                                string fkSql = MigrationSqlGenerator.GenerateForeignKeyStatement(fkGroup, new Dictionary<string, SqlTable>(targetSchemaResult.Result));
                                downScript.Append(fkSql);
                                downScript.AppendLine();
                                processedFks.Add(fk.Name);
                            }
                        }
                    }
                }
            }

            // Drop new tables (in reverse order)
            for (int i = diff.NewTables.Count - 1; i >= 0; i--)
            {
                downScript.AppendLine($"DROP TABLE IF EXISTS [dbo].[{diff.NewTables[i].Name}];");
            }

            downScript.AppendLine();
            downScript.AppendLine("COMMIT TRANSACTION;");

            // Build up script with transaction and phases
            StringBuilder upScript = new StringBuilder();
            upScript.AppendLine("SET XACT_ABORT ON;");
            upScript.AppendLine("BEGIN TRANSACTION;");
            upScript.AppendLine();

            int phaseNumber = 1;

            // Phase 0: Drop Foreign Keys for tables that will be dropped
            string phase0DropFksContent = phase0DropFks.ToString().Trim();
            if (!string.IsNullOrEmpty(phase0DropFksContent))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Drop Foreign Keys"));
                upScript.AppendLine(phase0DropFksContent);
                upScript.AppendLine();
                phaseNumber++;
            }

            // Phase 0: Drop Tables
            string phase0DropTablesContent = phase0DropTables.ToString().Trim();
            if (!string.IsNullOrEmpty(phase0DropTablesContent))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Drop Tables"));
                upScript.AppendLine(phase0DropTablesContent);
                upScript.AppendLine();
                phaseNumber++;
            }

            // Phase 1: Create Tables
            string phase1Content = phase1Tables.ToString().Trim();
            if (!string.IsNullOrEmpty(phase1Content))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Create Tables"));
                upScript.AppendLine(phase1Content);
                upScript.AppendLine();
                phaseNumber++;
            }

            // Phase 2: Modify Columns
            string phase2Content = phase2Columns.ToString().Trim();
            if (!string.IsNullOrEmpty(phase2Content))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Modify Columns"));
                upScript.AppendLine(phase2Content);
                upScript.AppendLine();
                phaseNumber++;
            }

            // Phase 3: Add Foreign Key Constraints
            string phase3Content = phase3Constraints.ToString().Trim();
            if (!string.IsNullOrEmpty(phase3Content))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Add Foreign Key Constraints"));
                upScript.AppendLine(phase3Content);
                upScript.AppendLine();
            }

            upScript.AppendLine("COMMIT TRANSACTION;");

            string timestamp = MigrationUtilities.GetNextMigrationTimestamp();
            string migrationName = $"{timestamp}_{description}";
            string upScriptPath = Path.Combine(migrationsPath, $"{migrationName}.sql");
            string downScriptPath = Path.Combine(migrationsPath, $"{migrationName}.down.sql");

            string finalUpScript = upScript.ToString();
            if (!finalUpScript.Contains("BEGIN TRANSACTION", StringComparison.OrdinalIgnoreCase))
            {
                finalUpScript = "SET XACT_ABORT ON;\r\nBEGIN TRANSACTION;\r\n\r\n" + finalUpScript + "\r\n\r\nCOMMIT TRANSACTION;";
            }

            await File.WriteAllTextAsync(upScriptPath, finalUpScript.TrimEnd());

            string finalDownScript = downScript.ToString();
            if (!finalDownScript.Contains("BEGIN TRANSACTION", StringComparison.OrdinalIgnoreCase))
            {
                finalDownScript = "SET XACT_ABORT ON;\r\nBEGIN TRANSACTION;\r\n\r\n" + finalDownScript + "\r\n\r\nCOMMIT TRANSACTION;";
            }

            await File.WriteAllTextAsync(downScriptPath, finalDownScript.TrimEnd());

            // Save schema snapshot (target schema represents the state after this migration is applied)
            // We need to apply the changes to target schema to get the new target
            ConcurrentDictionary<string, SqlTable> newTargetSchema = MigrationSchemaSnapshot.ApplySchemaDiffToTarget(targetSchemaResult.Result, diff);
            await MigrationSchemaSnapshot.SaveSchemaSnapshot(newTargetSchema, migrationName, codePath);

            return new ResultOrException<MigrationGenerationResult>(
                new MigrationGenerationResult(migrationName, upScriptPath, downScriptPath, description),
                null
            );
        }
        catch (Exception ex)
        {
            return new ResultOrException<MigrationGenerationResult>(null, ex);
        }
    }

    public static async Task<ResultOrException<MigrationGenerationResult>> CreateEmptyMigration(string codePath, string description)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(description))
            {
                description = "EmptyMigration";
            }

            string migrationsPath = MigrationUtilities.GetMigrationsPath(codePath);
            Directory.CreateDirectory(migrationsPath);

            string timestamp = MigrationUtilities.GetNextMigrationTimestamp();
            string migrationName = $"{timestamp}_{description}";
            string upScriptPath = Path.Combine(migrationsPath, $"{migrationName}.sql");
            string downScriptPath = Path.Combine(migrationsPath, $"{migrationName}.down.sql");

            // Create empty migration files
            await File.WriteAllTextAsync(upScriptPath, "-- Add your migration SQL here");
            await File.WriteAllTextAsync(downScriptPath, "-- Add your rollback SQL here");

            return new ResultOrException<MigrationGenerationResult>(
                new MigrationGenerationResult(migrationName, upScriptPath, downScriptPath, description),
                null
            );
        }
        catch (Exception ex)
        {
            return new ResultOrException<MigrationGenerationResult>(null, ex);
        }
    }
}
