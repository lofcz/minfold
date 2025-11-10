using System.Collections.Concurrent;
using System.Text;

namespace Minfold;

public class MinfoldMigrationDbUpToDateException : Exception
{
    public MinfoldMigrationDbUpToDateException() : base("Database is already up to date.")
    {
    }
}

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

    public static async Task<ResultOrException<MigrationGenerationResult>> GenerateInitialMigration(string sqlConn, string dbName, string codePath, string description, List<string>? schemas = null)
    {
        try
        {
            string migrationsPath = MigrationUtilities.GetMigrationsPath(codePath);

            // Check if migrations folder exists and has folders
            if (Directory.Exists(migrationsPath))
            {
                string[] existingMigrations = Directory.GetDirectories(migrationsPath);
                if (existingMigrations.Length > 0)
                {
                    return new ResultOrException<MigrationGenerationResult>(null, new InvalidOperationException("Migrations already exist. Cannot generate initial migration."));
                }
            }

            Directory.CreateDirectory(migrationsPath);

            List<string> allowedSchemas = schemas ?? ["dbo"];
            SqlService sqlService = new SqlService(sqlConn);
            ResultOrException<ConcurrentDictionary<string, SqlTable>> schemaResult = await sqlService.GetSchema(dbName, null, ["__MinfoldMigrations"], allowedSchemas);

            if (schemaResult.Exception is not null || schemaResult.Result is null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, schemaResult.Exception ?? new Exception("Failed to get database schema"));
            }

            // Get sequences and procedures
            ResultOrException<ConcurrentDictionary<string, SqlSequence>> sequencesResult = await sqlService.GetSequences(dbName, allowedSchemas);
            if (sequencesResult.Exception is not null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, sequencesResult.Exception);
            }

            ResultOrException<ConcurrentDictionary<string, SqlStoredProcedure>> proceduresResult = await sqlService.GetStoredProcedures(dbName, allowedSchemas);
            if (proceduresResult.Exception is not null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, proceduresResult.Exception);
            }

            ConcurrentDictionary<string, SqlSequence> sequences = sequencesResult.Result ?? new ConcurrentDictionary<string, SqlSequence>(StringComparer.OrdinalIgnoreCase);
            ConcurrentDictionary<string, SqlStoredProcedure> procedures = proceduresResult.Result ?? new ConcurrentDictionary<string, SqlStoredProcedure>(StringComparer.OrdinalIgnoreCase);

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
            downScript.AppendLine("-- Generated using Minfold, do not edit manually");

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

            // Build up script with phases (only include phases with content)
            // Transaction is managed by MigrationApplier.ExecuteMigrationScript using ADO.NET transactions
            StringBuilder upScript = new StringBuilder();
            upScript.AppendLine("-- Generated using Minfold, do not edit manually");
            upScript.AppendLine("SET XACT_ABORT ON;");
            upScript.AppendLine();
            
            int phaseNumber = 1;
            
            // Phase 0.5: Create Sequences (before tables, so they can be used in table defaults)
            StringBuilder phase0_5Sequences = new StringBuilder();
            foreach (SqlSequence sequence in sequences.Values.OrderBy(s => s.Name))
            {
                phase0_5Sequences.Append(MigrationSqlGenerator.GenerateCreateSequenceStatement(sequence));
                phase0_5Sequences.AppendLine();
            }
            string phase0_5Content = phase0_5Sequences.ToString().Trim();
            if (!string.IsNullOrEmpty(phase0_5Content))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Create Sequences"));
                upScript.AppendLine(phase0_5Content);
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
                phaseNumber++;
            }
            
            // Phase 4: Create Stored Procedures (after constraints)
            StringBuilder phase4Procedures = new StringBuilder();
            foreach (SqlStoredProcedure procedure in procedures.Values.OrderBy(p => p.Name))
            {
                phase4Procedures.Append(MigrationSqlGenerator.GenerateCreateProcedureStatement(procedure));
                phase4Procedures.AppendLine();
            }
            string phase4Content = phase4Procedures.ToString().Trim();
            if (!string.IsNullOrEmpty(phase4Content))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Create Stored Procedures"));
                upScript.AppendLine(phase4Content);
                upScript.AppendLine();
            }
            
            // Generate down script (drop procedures first, then tables, then sequences - reverse order of creation)
            // Transaction is managed by MigrationApplier.ExecuteMigrationScript using ADO.NET transactions
            downScript.AppendLine("SET XACT_ABORT ON;");
            downScript.AppendLine();
            
            // Drop procedures (reverse order)
            foreach (SqlStoredProcedure procedure in procedures.Values.OrderByDescending(p => p.Name))
            {
                downScript.AppendLine(MigrationSqlGenerator.GenerateDropProcedureStatement(procedure.Name, procedure.Schema));
            }
            
            // Drop tables (reverse order)
            for (int i = tables.Count - 1; i >= 0; i--)
            {
                SqlTable table = tables[i].Value;
                string tableName = table.Name;
                downScript.AppendLine($"DROP TABLE IF EXISTS [{table.Schema}].[{tableName}];");
            }
            
            // Drop sequences (reverse order)
            foreach (SqlSequence sequence in sequences.Values.OrderByDescending(s => s.Name))
            {
                downScript.AppendLine(MigrationSqlGenerator.GenerateDropSequenceStatement(sequence.Name, sequence.Schema));
            }
            
            downScript.AppendLine();

            string timestamp = MigrationUtilities.GetNextMigrationTimestamp();
            string migrationName = string.IsNullOrWhiteSpace(description) ? timestamp : $"{timestamp}_{description}";
            string migrationFolder = Path.Combine(migrationsPath, migrationName);
            Directory.CreateDirectory(migrationFolder);
            
            string upScriptPath = Path.Combine(migrationFolder, "up.sql");
            string downScriptPath = Path.Combine(migrationFolder, "down.sql");

            await File.WriteAllTextAsync(upScriptPath, upScript.ToString().TrimEnd());
            await File.WriteAllTextAsync(downScriptPath, downScript.ToString().TrimEnd());

            // Save schema snapshot for incremental migrations (including sequences and procedures)
            await MigrationSchemaSnapshot.SaveSchemaSnapshot(schemaResult.Result, migrationName, codePath, sequences, procedures);

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

    public static async Task<ResultOrException<MigrationGenerationResult>> GenerateIncrementalMigration(string sqlConn, string dbName, string codePath, string description, List<string>? schemas = null)
    {
        try
        {
            string migrationsPath = MigrationUtilities.GetMigrationsPath(codePath);
            Directory.CreateDirectory(migrationsPath);

            List<string> allowedSchemas = schemas ?? ["dbo"];
            // Get current database schema
            SqlService sqlService = new SqlService(sqlConn);
            ResultOrException<ConcurrentDictionary<string, SqlTable>> currentSchemaResult = await sqlService.GetSchema(dbName, null, ["__MinfoldMigrations"], allowedSchemas);

            if (currentSchemaResult.Exception is not null || currentSchemaResult.Result is null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, currentSchemaResult.Exception ?? new Exception("Failed to get current database schema"));
            }

            // Get current sequences and procedures
            ResultOrException<ConcurrentDictionary<string, SqlSequence>> currentSequencesResult = await sqlService.GetSequences(dbName, allowedSchemas);
            if (currentSequencesResult.Exception is not null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, currentSequencesResult.Exception);
            }

            ResultOrException<ConcurrentDictionary<string, SqlStoredProcedure>> currentProceduresResult = await sqlService.GetStoredProcedures(dbName, allowedSchemas);
            if (currentProceduresResult.Exception is not null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, currentProceduresResult.Exception);
            }

            ConcurrentDictionary<string, SqlSequence> currentSequences = currentSequencesResult.Result ?? new ConcurrentDictionary<string, SqlSequence>(StringComparer.OrdinalIgnoreCase);
            ConcurrentDictionary<string, SqlStoredProcedure> currentProcedures = currentProceduresResult.Result ?? new ConcurrentDictionary<string, SqlStoredProcedure>(StringComparer.OrdinalIgnoreCase);

            // Get applied migrations
            ResultOrException<List<string>> appliedMigrationsResult = await MigrationApplier.GetAppliedMigrations(sqlConn, dbName);
            if (appliedMigrationsResult.Exception is not null || appliedMigrationsResult.Result is null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, appliedMigrationsResult.Exception ?? new Exception("Failed to get applied migrations"));
            }

            // Get target schema from last migration snapshot (including sequences and procedures)
            ResultOrException<(ConcurrentDictionary<string, SqlTable> Tables, ConcurrentDictionary<string, SqlSequence> Sequences, ConcurrentDictionary<string, SqlStoredProcedure> Procedures)> targetSchemaResult = await MigrationSchemaSnapshot.GetTargetSchemaFromMigrations(codePath, appliedMigrationsResult.Result);
            if (targetSchemaResult.Exception is not null)
            {
                return new ResultOrException<MigrationGenerationResult>(null, targetSchemaResult.Exception);
            }

            ConcurrentDictionary<string, SqlTable> targetSchema = targetSchemaResult.Result.Tables;
            ConcurrentDictionary<string, SqlSequence> targetSequences = targetSchemaResult.Result.Sequences;
            ConcurrentDictionary<string, SqlStoredProcedure> targetProcedures = targetSchemaResult.Result.Procedures;

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

            // Compare schemas: we need TWO diffs - one for up script, one for down script
            // For incremental migrations:
            //   - targetSchema = state AFTER all applied migrations (from snapshot)
            //   - currentSchema = current database state (may have manual changes)
            //   - We want to generate a migration: targetSchema → currentSchema
            //
            // For UP script: CompareSchemas(target=BEFORE, current=AFTER) finds what to add/change
            //   - Add changes = columns in current but not in target → ADD them
            //   - Drop changes = columns in target but not in current → DROP them
            //
            // For DOWN script: CompareSchemas(current=AFTER, target=BEFORE) finds what to reverse
            //   - Add changes = columns in target but not in current → ADD them back
            //   - Drop changes = columns in current but not in target → DROP them back
            
            // Up script diff: what changed from target (BEFORE) to current (AFTER)
            SchemaDiff upDiff = MigrationSchemaComparer.CompareSchemas(targetSchema, currentSchemaResult.Result, targetSequences, currentSequences, targetProcedures, currentProcedures);
            
            // Down script diff: what changed from current (AFTER) back to target (BEFORE) - reverse of up diff
            SchemaDiff downDiff = MigrationSchemaComparer.CompareSchemas(currentSchemaResult.Result, targetSchema, currentSequences, targetSequences, currentProcedures, targetProcedures);
            
            // Debug logging for sequences and procedures
            MigrationLogger.Log($"\n=== Sequence comparison for down script ===");
            MigrationLogger.Log($"Current sequences: {string.Join(", ", currentSequences.Keys)}");
            MigrationLogger.Log($"Target sequences: {string.Join(", ", targetSequences.Keys)}");
            MigrationLogger.Log($"DownDiff.NewSequences: {string.Join(", ", downDiff.NewSequences.Select(s => s.Name))}");
            MigrationLogger.Log($"DownDiff.DroppedSequenceNames: {string.Join(", ", downDiff.DroppedSequenceNames)}");
            MigrationLogger.Log($"DownDiff.ModifiedSequences: {downDiff.ModifiedSequences.Count}");
            if (downDiff.DroppedSequenceNames.Count > 0)
            {
                MigrationLogger.Log("Detailed dropped sequence classification:");
                foreach (string sequenceName in downDiff.DroppedSequenceNames)
                {
                    bool inCurrent = currentSequences.Keys.Any(k => k.Equals(sequenceName, StringComparison.OrdinalIgnoreCase));
                    bool inTarget = targetSequences.Keys.Any(k => k.Equals(sequenceName, StringComparison.OrdinalIgnoreCase));
                    MigrationLogger.Log($"  {sequenceName} -> inCurrent={inCurrent}, inTarget={inTarget}");
                }
            }
            if (downDiff.NewSequences.Count > 0)
            {
                MigrationLogger.Log("Detailed new sequence classification:");
                foreach (SqlSequence sequence in downDiff.NewSequences)
                {
                    bool inCurrent = currentSequences.Keys.Any(k => k.Equals(sequence.Name, StringComparison.OrdinalIgnoreCase));
                    bool inTarget = targetSequences.Keys.Any(k => k.Equals(sequence.Name, StringComparison.OrdinalIgnoreCase));
                    MigrationLogger.Log($"  {sequence.Name} -> inCurrent={inCurrent}, inTarget={inTarget}");
                }
            }
            
            MigrationLogger.Log($"\n=== Procedure comparison for down script ===");
            MigrationLogger.Log($"Current procedures: {string.Join(", ", currentProcedures.Keys)}");
            MigrationLogger.Log($"Target procedures: {string.Join(", ", targetProcedures.Keys)}");
            MigrationLogger.Log($"DownDiff.NewProcedures: {string.Join(", ", downDiff.NewProcedures.Select(p => p.Name))}");
            MigrationLogger.Log($"DownDiff.DroppedProcedureNames: {string.Join(", ", downDiff.DroppedProcedureNames)}");
            MigrationLogger.Log($"DownDiff.ModifiedProcedures: {downDiff.ModifiedProcedures.Count}");
            
            // Use upDiff for up script generation, downDiff for down script generation
            SchemaDiff diff = upDiff;

            // Check if there are any changes
            if (diff.NewTables.Count == 0 && diff.DroppedTableNames.Count == 0 && diff.ModifiedTables.Count == 0 &&
                diff.NewSequences.Count == 0 && diff.DroppedSequenceNames.Count == 0 && diff.ModifiedSequences.Count == 0 &&
                diff.NewProcedures.Count == 0 && diff.DroppedProcedureNames.Count == 0 && diff.ModifiedProcedures.Count == 0)
            {
                return new ResultOrException<MigrationGenerationResult>(null, new MinfoldMigrationDbUpToDateException());
            }

            // Generate migration scripts
            StringBuilder phase0DropProcedures = new StringBuilder(); // DROP PROCEDURE statements
            StringBuilder phase0DropSequences = new StringBuilder(); // DROP SEQUENCE statements
            StringBuilder phase0DropFks = new StringBuilder(); // DROP FK constraints for tables that will be dropped
            StringBuilder phase0DropTables = new StringBuilder(); // DROP TABLE statements
            StringBuilder phase0DropPks = new StringBuilder(); // DROP PRIMARY KEY constraints
            StringBuilder phase0_5Sequences = new StringBuilder(); // CREATE SEQUENCE statements (before tables)
            StringBuilder phase1Tables = new StringBuilder(); // CREATE TABLE statements
            StringBuilder phase2Columns = new StringBuilder(); // ALTER TABLE column modifications
            StringBuilder phase3Constraints = new StringBuilder(); // ALTER TABLE FK constraints and PRIMARY KEY constraints
            StringBuilder phase4Procedures = new StringBuilder(); // CREATE PROCEDURE statements (after constraints)
            StringBuilder downScript = new StringBuilder();
            downScript.AppendLine("-- Generated using Minfold, do not edit manually");

            // Phase 0: Drop tables (drop FKs first, then tables)
            // First, collect FKs from tables that will be dropped
            foreach (string droppedTableName in diff.DroppedTableNames)
            {
                // Find the table schema from target schema to get its FKs
                if (targetSchema.TryGetValue(droppedTableName.ToLowerInvariant(), out SqlTable? droppedTable))
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
                // Get schema from target schema (before migration)
                string schema = "dbo";
                if (targetSchema.TryGetValue(droppedTableName.ToLowerInvariant(), out SqlTable? droppedTable))
                {
                    schema = droppedTable.Schema;
                }
                phase0DropTables.AppendLine($"DROP TABLE IF EXISTS [{schema}].[{droppedTableName}];");
            }

            // Phase 1: Create new tables
            foreach (SqlTable newTable in diff.NewTables.OrderBy(t => t.Name))
            {
                string createTableSql = MigrationSqlGenerator.GenerateCreateTableStatement(newTable);
                phase1Tables.AppendLine(createTableSql);
                phase1Tables.AppendLine();
            }

            // Phase 0.5: Drop PRIMARY KEY constraints before column modifications
            // Collect PK changes from modified tables
            HashSet<string> tablesWithPkDropped = new HashSet<string>();
            foreach (TableDiff tableDiff in diff.ModifiedTables.OrderBy(t => t.TableName))
            {
                // Check if any column is losing PK status
                bool needsPkDropped = false;
                foreach (ColumnChange change in tableDiff.ColumnChanges)
                {
                    if (change.OldColumn != null && change.OldColumn.IsPrimaryKey && 
                        (change.ChangeType == ColumnChangeType.Drop || 
                         (change.ChangeType == ColumnChangeType.Modify && change.NewColumn != null && !change.NewColumn.IsPrimaryKey)))
                    {
                        needsPkDropped = true;
                        break;
                    }
                }
                
                if (needsPkDropped && !tablesWithPkDropped.Contains(tableDiff.TableName.ToLowerInvariant()))
                {
                    // Need to drop PK constraint before dropping/modifying the column
                    // Get current PK columns from target schema to determine constraint name
                    if (targetSchema.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? targetTable))
                    {
                        List<SqlTableColumn> pkColumns = targetTable.Columns.Values.Where(c => c.IsPrimaryKey).OrderBy(c => c.OrdinalPosition).ToList();
                        if (pkColumns.Count > 0)
                        {
                            // Generate PK constraint name (SQL Server default pattern)
                            string pkConstraintName = $"PK_{tableDiff.TableName}";
                            phase0DropPks.AppendLine(MigrationSqlGenerator.GenerateDropPrimaryKeyStatement(tableDiff.TableName, pkConstraintName, targetTable.Schema));
                            tablesWithPkDropped.Add(tableDiff.TableName.ToLowerInvariant());
                        }
                    }
                }
            }

            // Phase 2: Column modifications
            foreach (TableDiff tableDiff in diff.ModifiedTables.OrderBy(t => t.TableName))
            {
                // Pass current schema (for dropping indexes) and target schema (for single-column detection)
                string columnModifications = MigrationSqlGenerator.GenerateColumnModifications(
                    tableDiff, 
                    currentSchemaResult.Result ?? new ConcurrentDictionary<string, SqlTable>(StringComparer.OrdinalIgnoreCase),
                    targetSchema);
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
                            string fkSql = MigrationSqlGenerator.GenerateForeignKeyStatement(fkGroup, new Dictionary<string, SqlTable>(targetSchema));
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
                        string fkSql = MigrationSqlGenerator.GenerateForeignKeyStatement(fkGroup, new Dictionary<string, SqlTable>(targetSchema));
                        phase3Constraints.Append(fkSql);
                        phase3Constraints.AppendLine();
                    }
                }
            }

            // Add PRIMARY KEY constraints for columns that are gaining PK status
            // Get current schema (after modifications) to find new PK columns
            ConcurrentDictionary<string, SqlTable> currentSchemaAfterChanges = MigrationSchemaSnapshot.ApplySchemaDiffToTarget(targetSchema, diff);
            HashSet<string> tablesWithPkAdded = new HashSet<string>();
            
            foreach (TableDiff tableDiff in diff.ModifiedTables.OrderBy(t => t.TableName))
            {
                // Check if any columns are gaining PK status
                bool hasNewPk = false;
                foreach (ColumnChange change in tableDiff.ColumnChanges)
                {
                    if (change.NewColumn != null && change.NewColumn.IsPrimaryKey)
                    {
                        if (change.ChangeType == ColumnChangeType.Add || 
                            (change.ChangeType == ColumnChangeType.Modify && change.OldColumn != null && !change.OldColumn.IsPrimaryKey))
                        {
                            hasNewPk = true;
                            break;
                        }
                    }
                }
                
                if (hasNewPk && !tablesWithPkAdded.Contains(tableDiff.TableName.ToLowerInvariant()))
                {
                    // Get all PK columns from the new schema (after changes)
                    if (currentSchemaAfterChanges.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? newTable))
                    {
                        List<SqlTableColumn> allPkColumns = newTable.Columns.Values.Where(c => c.IsPrimaryKey).OrderBy(c => c.OrdinalPosition).ToList();
                        if (allPkColumns.Count > 0)
                        {
                            List<string> pkColumnNames = allPkColumns.Select(c => c.Name).ToList();
                            string pkConstraintName = $"PK_{tableDiff.TableName}";
                            phase3Constraints.Append(MigrationSqlGenerator.GenerateAddPrimaryKeyStatement(tableDiff.TableName, pkColumnNames, pkConstraintName, newTable.Schema));
                            tablesWithPkAdded.Add(tableDiff.TableName.ToLowerInvariant());
                        }
                    }
                }
            }

            // Add PRIMARY KEY constraints for new tables (already included in CREATE TABLE, but handle separately if needed)
            foreach (SqlTable newTable in diff.NewTables)
            {
                List<SqlTableColumn> pkColumns = newTable.Columns.Values.Where(c => c.IsPrimaryKey).OrderBy(c => c.OrdinalPosition).ToList();
                // PKs for new tables are already in CREATE TABLE statement, so we don't need to add them here
            }

            // Phase 3.5: Handle index changes (drop modified/dropped indexes, add new/modified indexes)
            // Drop indexes that are being dropped or modified (need to drop old before adding new)
            foreach (TableDiff tableDiff in diff.ModifiedTables.OrderBy(t => t.TableName))
            {
                foreach (IndexChange indexChange in tableDiff.IndexChanges)
                {
                    if (indexChange.ChangeType == IndexChangeType.Drop && indexChange.OldIndex != null)
                    {
                        phase3Constraints.AppendLine(MigrationSqlGenerator.GenerateDropIndexStatement(indexChange.OldIndex));
                    }
                    else if (indexChange.ChangeType == IndexChangeType.Modify && indexChange.OldIndex != null)
                    {
                        // For modifications, drop the old index before adding the new one
                        phase3Constraints.AppendLine(MigrationSqlGenerator.GenerateDropIndexStatement(indexChange.OldIndex));
                    }
                }
            }

            // Add indexes for new tables
            foreach (SqlTable newTable in diff.NewTables)
            {
                foreach (SqlIndex index in newTable.Indexes)
                {
                    phase3Constraints.Append(MigrationSqlGenerator.GenerateCreateIndexStatement(index));
                    phase3Constraints.AppendLine();
                }
            }

            // Add new/modified indexes from modified tables
            foreach (TableDiff tableDiff in diff.ModifiedTables.OrderBy(t => t.TableName))
            {
                foreach (IndexChange indexChange in tableDiff.IndexChanges.Where(c => c.ChangeType == IndexChangeType.Add || c.ChangeType == IndexChangeType.Modify))
                {
                    if (indexChange.NewIndex != null)
                    {
                        phase3Constraints.Append(MigrationSqlGenerator.GenerateCreateIndexStatement(indexChange.NewIndex));
                        phase3Constraints.AppendLine();
                    }
                }
            }

            // Phase 0: Drop sequences that are being dropped (before dropping tables)
            foreach (string droppedSequenceName in diff.DroppedSequenceNames.OrderByDescending(s => s))
            {
                // Get schema from target schema (before migration)
                string schema = "dbo";
                if (targetSequences.TryGetValue(droppedSequenceName.ToLowerInvariant(), out SqlSequence? droppedSequence))
                {
                    schema = droppedSequence.Schema;
                }
                phase0DropSequences.Append(MigrationSqlGenerator.GenerateDropSequenceStatement(droppedSequenceName, schema));
                phase0DropSequences.AppendLine();
            }
            
            // Phase 0: Drop procedures that are being dropped or modified (before dropping tables)
            foreach (string droppedProcedureName in diff.DroppedProcedureNames.OrderByDescending(p => p))
            {
                // Get schema from target schema (before migration)
                string schema = "dbo";
                if (targetProcedures.TryGetValue(droppedProcedureName.ToLowerInvariant(), out SqlStoredProcedure? droppedProcedure))
                {
                    schema = droppedProcedure.Schema;
                }
                phase0DropProcedures.Append(MigrationSqlGenerator.GenerateDropProcedureStatement(droppedProcedureName, schema));
                phase0DropProcedures.AppendLine();
            }
            
            // Note: Modified sequences are handled in Phase 0.5 via GenerateAlterSequenceStatement which drops and recreates
            
            foreach (ProcedureChange procedureChange in diff.ModifiedProcedures)
            {
                if (procedureChange.ChangeType == ProcedureChangeType.Modify && procedureChange.OldProcedure != null)
                {
                    // For modifications, drop the old procedure before creating the new one
                    phase0DropProcedures.Append(MigrationSqlGenerator.GenerateDropProcedureStatement(procedureChange.OldProcedure.Name, procedureChange.OldProcedure.Schema));
                    phase0DropProcedures.AppendLine();
                }
            }

            // Phase 0.5: Create sequences (before tables, so they can be used in table defaults)
            foreach (SqlSequence newSequence in diff.NewSequences.OrderBy(s => s.Name))
            {
                phase0_5Sequences.Append(MigrationSqlGenerator.GenerateCreateSequenceStatement(newSequence));
                phase0_5Sequences.AppendLine();
            }
            foreach (SequenceChange sequenceChange in diff.ModifiedSequences)
            {
                if (sequenceChange.ChangeType == SequenceChangeType.Modify && sequenceChange.NewSequence != null)
                {
                    // For modifications, drop and recreate (handled by GenerateAlterSequenceStatement)
                    phase0_5Sequences.Append(MigrationSqlGenerator.GenerateAlterSequenceStatement(sequenceChange.OldSequence!, sequenceChange.NewSequence));
                    phase0_5Sequences.AppendLine();
                }
            }

            // Phase 4: Create procedures (after constraints)
            foreach (SqlStoredProcedure newProcedure in diff.NewProcedures.OrderBy(p => p.Name))
            {
                phase4Procedures.Append(MigrationSqlGenerator.GenerateCreateProcedureStatement(newProcedure));
                phase4Procedures.AppendLine();
            }
            foreach (ProcedureChange procedureChange in diff.ModifiedProcedures)
            {
                if (procedureChange.ChangeType == ProcedureChangeType.Modify && procedureChange.NewProcedure != null)
                {
                    // For modifications, create the new procedure (old one was already dropped in phase 0)
                    phase4Procedures.Append(MigrationSqlGenerator.GenerateCreateProcedureStatement(procedureChange.NewProcedure));
                    phase4Procedures.AppendLine();
                }
            }

            // Generate down script using downDiff (reverse of upDiff)
            // Transaction is managed by MigrationApplier.ExecuteMigrationScript using ADO.NET transactions
            downScript.AppendLine("SET XACT_ABORT ON;");
            downScript.AppendLine();

            // Switch to downDiff for down script generation
            SchemaDiff originalDiff = diff;
            diff = downDiff;

            // Reverse index changes
            // For Modify: OldIndex = current state (after migration), NewIndex = target state (before migration)
            // For Add: NewIndex = from targetSchema (was dropped by migration) → should be ADDED back
            // For Drop: OldIndex = from currentSchema (was added by migration) → should be DROPPED
            foreach (TableDiff tableDiff in diff.ModifiedTables.OrderByDescending(t => t.TableName))
            {
                foreach (IndexChange indexChange in tableDiff.IndexChanges.Where(c => c.ChangeType == IndexChangeType.Modify))
                {
                    // Drop current index (OldIndex) and add back target index (NewIndex)
                    if (indexChange.OldIndex != null)
                    {
                        downScript.AppendLine(MigrationSqlGenerator.GenerateDropIndexStatement(indexChange.OldIndex));
                    }
                    if (indexChange.NewIndex != null)
                    {
                        downScript.Append(MigrationSqlGenerator.GenerateCreateIndexStatement(indexChange.NewIndex));
                        downScript.AppendLine();
                    }
                }
                
                // Drop indexes that were added by the migration
                // Drop changes: index exists in currentSchema (after migration) but not in targetSchema (before migration)
                foreach (IndexChange indexChange in tableDiff.IndexChanges.Where(c => c.ChangeType == IndexChangeType.Drop))
                {
                    if (indexChange.OldIndex != null)
                    {
                        downScript.AppendLine(MigrationSqlGenerator.GenerateDropIndexStatement(indexChange.OldIndex));
                    }
                }
                
                // Add back indexes that were dropped by the migration
                // Add changes: index exists in targetSchema (before migration) but not in currentSchema (after migration)
                foreach (IndexChange indexChange in tableDiff.IndexChanges.Where(c => c.ChangeType == IndexChangeType.Add))
                {
                    if (indexChange.NewIndex != null)
                    {
                        downScript.Append(MigrationSqlGenerator.GenerateCreateIndexStatement(indexChange.NewIndex));
                        downScript.AppendLine();
                    }
                }
            }

            // Reverse PRIMARY KEY changes - drop PKs first (before restoring columns)
            // We need to drop PK if:
            // 1. Any columns gained PK status (new PK columns)
            // 2. Any columns that are currently PKs need to be modified or dropped (losing PK status)
            // We'll add back old PKs after restoring columns
            StringBuilder pkRestoreScript = new StringBuilder(); // Store PK restorations for after column restoration
            foreach (TableDiff tableDiff in diff.ModifiedTables.OrderByDescending(t => t.TableName))
            {
                bool needsPkDropped = false;
                
                // Check if any columns gained PK status (need to drop new PK)
                foreach (ColumnChange change in tableDiff.ColumnChanges)
                {
                    if (change.NewColumn != null && change.NewColumn.IsPrimaryKey)
                    {
                        if (change.ChangeType == ColumnChangeType.Add || 
                            (change.ChangeType == ColumnChangeType.Modify && change.OldColumn != null && !change.OldColumn.IsPrimaryKey))
                        {
                            needsPkDropped = true;
                            break;
                        }
                    }
                }
                
                // Also check if any columns that are currently PKs need to be modified or dropped
                // For dropped columns: OldColumn is from current schema (after migration), so check OldColumn.IsPrimaryKey
                // For modified columns: NewColumn is from current schema (after migration), so check if it's a PK that will lose PK status
                if (!needsPkDropped)
                {
                    foreach (ColumnChange change in tableDiff.ColumnChanges)
                    {
                        // Check if a column that is currently a PK is being dropped
                        // OldColumn represents the current state (after migration), so check if it's a PK
                        if (change.ChangeType == ColumnChangeType.Drop && change.OldColumn != null && change.OldColumn.IsPrimaryKey)
                        {
                            needsPkDropped = true;
                            break;
                        }
                        // Check if a column that is currently a PK is being modified to lose PK status
                        // NewColumn represents the current state (after migration)
                        if (change.ChangeType == ColumnChangeType.Modify && change.NewColumn != null && change.NewColumn.IsPrimaryKey && 
                            change.OldColumn != null && !change.OldColumn.IsPrimaryKey)
                        {
                            needsPkDropped = true;
                            break;
                        }
                    }
                }
                
                if (needsPkDropped)
                {
                    // Get current PK columns from schema after changes to drop
                    ConcurrentDictionary<string, SqlTable> schemaAfterChanges = MigrationSchemaSnapshot.ApplySchemaDiffToTarget(targetSchema, diff);
                    if (schemaAfterChanges.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? newTable))
                    {
                        List<SqlTableColumn> newPkColumns = newTable.Columns.Values.Where(c => c.IsPrimaryKey).OrderBy(c => c.OrdinalPosition).ToList();
                        if (newPkColumns.Count > 0)
                        {
                            string pkConstraintName = $"PK_{tableDiff.TableName}";
                            downScript.AppendLine(MigrationSqlGenerator.GenerateDropPrimaryKeyStatement(tableDiff.TableName, pkConstraintName, newTable.Schema));
                        }
                    }
                }
                
                // Store PK restoration for after column restoration
                // We need to restore PK if the target schema (before migration) had columns as PKs
                // For Drop: NewColumn is null, so check if OldColumn (current state) was a PK that needs to be restored
                //   Actually, for Drop: OldColumn is from current schema, NewColumn is null. We need to check targetSchema.
                // For Modify: NewColumn is from target schema (what we want), so check NewColumn.IsPrimaryKey
                bool needsPkRestored = false;
                HashSet<string> columnsThatLostPk = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                
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
                                columnsThatLostPk.Add(change.OldColumn!.Name);
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
                            columnsThatLostPk.Add(change.NewColumn.Name);
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
                            pkRestoreScript.Append(MigrationSqlGenerator.GenerateAddPrimaryKeyStatement(tableDiff.TableName, pkColumnNames, pkConstraintName, targetTable.Schema));
                        }
                    }
                }
            }

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
                    string fkSql = MigrationSqlGenerator.GenerateForeignKeyStatement(fkGroup, new Dictionary<string, SqlTable>(targetSchema));
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
                    string fkSql = MigrationSqlGenerator.GenerateForeignKeyStatement(fkGroup, new Dictionary<string, SqlTable>(targetSchema));
                    downScript.Append(fkSql);
                    downScript.AppendLine();
                }
            }

            // Reverse column modifications (in reverse order)
            // Important: Drop added identity columns BEFORE restoring identity to modified columns
            // to avoid "Multiple identity columns" error
            // Track which columns we're adding to avoid duplicates
            // Also track which columns we're dropping to ensure we don't add a column we just dropped
            HashSet<string> columnsBeingAdded = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            HashSet<string> columnsBeingDropped = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            
            foreach (TableDiff tableDiff in diff.ModifiedTables.OrderByDescending(t => t.TableName))
            {
                // Log column changes for debugging
                MigrationLogger.Log($"\n=== Processing table: {tableDiff.TableName} ===");
                foreach (ColumnChange change in tableDiff.ColumnChanges)
                {
                    MigrationLogger.Log($"  ChangeType: {change.ChangeType}, Column: {change.OldColumn?.Name ?? change.NewColumn?.Name ?? "null"}");
                    if (change.OldColumn != null) MigrationLogger.Log($"    OldColumn: IsIdentity={change.OldColumn.IsIdentity}, IsPrimaryKey={change.OldColumn.IsPrimaryKey}");
                    if (change.NewColumn != null) MigrationLogger.Log($"    NewColumn: IsIdentity={change.NewColumn.IsIdentity}, IsPrimaryKey={change.NewColumn.IsPrimaryKey}");
                }
                
                // Pre-calculate lists
                List<ColumnChange> dropChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Drop).ToList();
                List<ColumnChange> addChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Add).ToList();
                List<ColumnChange> modifyChanges = tableDiff.ColumnChanges.Where(c => c.ChangeType == ColumnChangeType.Modify).ToList();
                
                // Check if we have Modify changes that require DROP+ADD, and Drop changes that would leave the modified column as the only one
                // In this case, we need to process Modify BEFORE Drop to avoid the "only column" error
                bool needsModifyBeforeDrop = false;
                if (modifyChanges.Count > 0 && dropChanges.Count > 0 && currentSchemaResult.Result != null)
                {
                    if (currentSchemaResult.Result.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? currentTable))
                    {
                        int currentDataColumnCount = currentTable.Columns.Values.Count(c => !c.IsComputed);
                        
                        // Check if any Modify change requires DROP+ADD and would be the only column after drops
                        foreach (ColumnChange modifyChange in modifyChanges)
                        {
                            if (modifyChange.OldColumn != null && modifyChange.NewColumn != null)
                            {
                                bool requiresDropAdd = (modifyChange.OldColumn.IsComputed || modifyChange.NewColumn.IsComputed) ||
                                                      (modifyChange.OldColumn.IsIdentity != modifyChange.NewColumn.IsIdentity);
                                
                                if (requiresDropAdd && currentDataColumnCount - dropChanges.Count == 1)
                                {
                                    // After dropping other columns, this would be the only column
                                    needsModifyBeforeDrop = true;
                                    break;
                                }
                            }
                        }
                    }
                }
                
                // First pass: Drop columns that were added by the migration
                // For Drop changes: column exists in currentSchema (after migration) but not in targetSchema (before migration)
                // This means the column was ADDED by the migration, so we need to DROP it during rollback
                // BUT: if we need to process Modify before Drop, skip this for now
                if (!needsModifyBeforeDrop)
                {
                    foreach (ColumnChange change in dropChanges)
                    {
                        if (change.OldColumn != null)
                        {
                            string columnKey = $"{tableDiff.TableName}.{change.OldColumn.Name}";
                            columnsBeingDropped.Add(columnKey);
                            MigrationLogger.Log($"  [DROP] {columnKey}");
                            downScript.AppendLine(MigrationSqlGenerator.GenerateDropColumnStatement(change.OldColumn.Name, tableDiff.TableName));
                        }
                    }
                }

                // Second pass: Restore columns that were dropped by the migration
                // For Add changes: column exists in targetSchema (before migration) but not in currentSchema (after migration)
                // This means the column was DROPPED by the migration, so we need to ADD it back during rollback
                foreach (ColumnChange change in addChanges)
                {
                    if (change.NewColumn != null)
                    {
                        string columnKey = $"{tableDiff.TableName}.{change.NewColumn.Name}";
                        // Only add if we haven't already added it and we haven't dropped it in this script
                        if (!columnsBeingAdded.Contains(columnKey) && !columnsBeingDropped.Contains(columnKey))
                        {
                            columnsBeingAdded.Add(columnKey);
                            MigrationLogger.Log($"  [ADD] {columnKey}");
                            downScript.Append(MigrationSqlGenerator.GenerateAddColumnStatement(change.NewColumn, tableDiff.TableName));
                        }
                        else
                        {
                            MigrationLogger.Log($"  [SKIP ADD] {columnKey} (already added or being dropped)");
                        }
                    }
                }

                // Third pass: Reverse modifications (now safe to restore identity since added identity columns are dropped)
                // BUT: if needsModifyBeforeDrop, we process Modify BEFORE Drop to avoid "only column" error
                // For Modify changes: OldColumn = current state (after migration), NewColumn = target state (before migration)
                // We need to restore from OldColumn to NewColumn
                foreach (ColumnChange change in modifyChanges)
                {
                    if (change.OldColumn != null && change.NewColumn != null)
                    {
                        // For down script, we're restoring OldColumn, so use that for the key
                        string columnKey = $"{tableDiff.TableName}.{change.OldColumn.Name}";
                        
                        MigrationLogger.Log($"  [MODIFY] {columnKey}: OldColumn.IsIdentity={change.OldColumn.IsIdentity}, NewColumn.IsIdentity={change.NewColumn.IsIdentity}");
                        
                        // Get schema from current schema or default to dbo
                        string schema = "dbo";
                        if (currentSchemaResult.Result != null && currentSchemaResult.Result.TryGetValue(tableDiff.TableName.ToLowerInvariant(), out SqlTable? currentTable))
                        {
                            schema = currentTable.Schema;
                        }
                        
                        // Check if identity property changed - SQL Server requires DROP + ADD (same as up script)
                        if (change.OldColumn.IsIdentity != change.NewColumn.IsIdentity)
                        {
                            // For down script: reverse the change (from OldColumn back to NewColumn)
                            // Current state is OldColumn (after up migration), target is NewColumn (before up migration)
                            // We need to drop OldColumn (current) and add NewColumn (target)
                            MigrationLogger.Log($"    Reversing identity change: {change.OldColumn.Name} (IsIdentity={change.OldColumn.IsIdentity}) -> {change.NewColumn.Name} (IsIdentity={change.NewColumn.IsIdentity})");
                            
                            // If the column being dropped has a primary key constraint, drop it first
                            if (change.OldColumn.IsPrimaryKey)
                            {
                                string pkConstraintName = $"PK_{tableDiff.TableName}";
                                downScript.AppendLine(MigrationSqlGenerator.GenerateDropPrimaryKeyStatement(tableDiff.TableName, pkConstraintName, schema));
                                MigrationLogger.Log($"    Dropped PK constraint before dropping column {change.OldColumn.Name}");
                            }
                            
                            // Create a reversed TableDiff for the safe wrapper to check column state
                            // In down script, currentSchema is the state after up migration (has OldColumn)
                            // We need to check if NewColumn would be the only column when we restore it
                            // If needsModifyBeforeDrop, include Drop changes in the diff so safe wrapper can detect single-column scenario
                            List<ColumnChange> reversedDiffChanges = new List<ColumnChange> { new ColumnChange(ColumnChangeType.Modify, change.OldColumn, change.NewColumn) };
                            if (needsModifyBeforeDrop)
                            {
                                // Include Drop changes so GenerateSafeColumnDropAndAdd can detect that this would be the only column
                                reversedDiffChanges.AddRange(dropChanges);
                            }
                            TableDiff reversedDiff = new TableDiff(
                                tableDiff.TableName,
                                reversedDiffChanges,
                                new List<ForeignKeyChange>(),
                                new List<IndexChange>()
                            );
                            
                            // Use currentSchema (state after up migration) to check if safe
                            // Drop OldColumn (current state) and add NewColumn (target state)
                            string safeDropAdd = MigrationSqlGenerator.GenerateSafeColumnDropAndAdd(
                                change.OldColumn,  // Drop this (current state after migration)
                                change.NewColumn,  // Add this (target state before migration)
                                tableDiff.TableName,
                                schema,
                                reversedDiff,
                                currentSchemaResult.Result
                            );
                            
                            if (columnsBeingAdded.Add(columnKey))
                            {
                                downScript.Append(safeDropAdd);
                                MigrationLogger.Log($"    Added back {change.OldColumn.Name}");
                            }
                            else
                            {
                                MigrationLogger.Log($"    Skipped adding {change.OldColumn.Name} (already added)");
                            }
                        }
                        // Check if it's a computed column change - these need DROP + ADD
                        else if (change.OldColumn.IsComputed || change.NewColumn.IsComputed)
                        {
                            // For down script: reverse the change (from OldColumn back to NewColumn)
                            // Drop OldColumn (current state) and add NewColumn (target state)
                            MigrationLogger.Log($"    Reversing computed column change: {change.OldColumn.Name} -> {change.NewColumn.Name}");
                            
                            // If the column being dropped has a primary key constraint, drop it first
                            if (change.OldColumn.IsPrimaryKey)
                            {
                                string pkConstraintName = $"PK_{tableDiff.TableName}";
                                downScript.AppendLine(MigrationSqlGenerator.GenerateDropPrimaryKeyStatement(tableDiff.TableName, pkConstraintName, schema));
                                MigrationLogger.Log($"    Dropped PK constraint before dropping column {change.OldColumn.Name}");
                            }
                            
                            // If needsModifyBeforeDrop, include Drop changes in the diff so safe wrapper can detect single-column scenario
                            List<ColumnChange> reversedDiffChanges = new List<ColumnChange> { new ColumnChange(ColumnChangeType.Modify, change.OldColumn, change.NewColumn) };
                            if (needsModifyBeforeDrop)
                            {
                                // Include Drop changes so GenerateSafeColumnDropAndAdd can detect that this would be the only column
                                reversedDiffChanges.AddRange(dropChanges);
                            }
                            TableDiff reversedDiff = new TableDiff(
                                tableDiff.TableName,
                                reversedDiffChanges,
                                new List<ForeignKeyChange>(),
                                new List<IndexChange>()
                            );
                            
                            string safeDropAdd = MigrationSqlGenerator.GenerateSafeColumnDropAndAdd(
                                change.OldColumn,  // Drop this (current state after migration)
                                change.NewColumn,  // Add this (target state before migration)
                                tableDiff.TableName,
                                schema,
                                reversedDiff,
                                currentSchemaResult.Result
                            );
                            
                            if (columnsBeingAdded.Add(columnKey))
                            {
                                downScript.Append(safeDropAdd);
                            }
                        }
                        else
                        {
                            // Reverse the modification: change from OldColumn (current) to NewColumn (target)
                            string reverseAlter = MigrationSqlGenerator.GenerateAlterColumnStatement(change.OldColumn, change.NewColumn, tableDiff.TableName);
                        if (!string.IsNullOrEmpty(reverseAlter))
                        {
                            downScript.Append(reverseAlter);
                            }
                        }
                    }
                }
                
                // Fourth pass: If we skipped Drop changes earlier (needsModifyBeforeDrop), process them now
                if (needsModifyBeforeDrop)
                {
                    foreach (ColumnChange change in dropChanges)
                    {
                        if (change.OldColumn != null)
                        {
                            string columnKey = $"{tableDiff.TableName}.{change.OldColumn.Name}";
                            columnsBeingDropped.Add(columnKey);
                            MigrationLogger.Log($"  [DROP] {columnKey} (after Modify to avoid only-column error)");
                            downScript.AppendLine(MigrationSqlGenerator.GenerateDropColumnStatement(change.OldColumn.Name, tableDiff.TableName));
                        }
                    }
                }
            }

            // Add back old PKs (after columns have been restored)
            downScript.Append(pkRestoreScript);

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
                                string fkSql = MigrationSqlGenerator.GenerateForeignKeyStatement(fkGroup, new Dictionary<string, SqlTable>(targetSchema));
                                downScript.Append(fkSql);
                                downScript.AppendLine();
                                processedFks.Add(fk.Name);
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
                string createTableSql = MigrationSqlGenerator.GenerateCreateTableStatement(newTable);
                downScript.AppendLine(createTableSql);
                downScript.AppendLine();
                
                // Generate FK constraints for the recreated table
                HashSet<string> processedFks = new HashSet<string>();
                foreach (SqlTableColumn column in newTable.Columns.Values)
                {
                    foreach (SqlForeignKey fk in column.ForeignKeys)
                    {
                        if (!processedFks.Contains(fk.Name))
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
                            string fkSql = MigrationSqlGenerator.GenerateForeignKeyStatement(fkGroup, new Dictionary<string, SqlTable>(targetSchema));
                            downScript.Append(fkSql);
                            downScript.AppendLine();
                            processedFks.Add(fk.Name);
                        }
                    }
                }
            }
            
            // Drop tables that were added by the migration (in reverse order)
            // In downDiff, DroppedTableNames = tables in currentSchema (after migration) but not in targetSchema (before migration)
            // These are tables that were ADDED by the migration, so we need to DROP them in the down script
            for (int i = diff.DroppedTableNames.Count - 1; i >= 0; i--)
            {
                string droppedTableName = diff.DroppedTableNames[i];
                // Get schema from current schema (after migration)
                string schema = "dbo";
                if (currentSchemaResult.Result != null && currentSchemaResult.Result.TryGetValue(droppedTableName.ToLowerInvariant(), out SqlTable? droppedTable))
                {
                    schema = droppedTable.Schema;
                }
                downScript.AppendLine($"DROP TABLE IF EXISTS [{schema}].[{droppedTableName}];");
            }

            // Drop sequences that were added or modified by the migration (reverse order)
            // In downDiff, DroppedSequenceNames = sequences in currentSchema (after migration) but not in targetSchema (before migration)
            // These are sequences that were ADDED by the migration, so we need to DROP them in the down script
            if (diff.DroppedSequenceNames.Count > 0)
            {
                MigrationLogger.Log($"\n=== Dropping sequences (were added by migration): {string.Join(", ", diff.DroppedSequenceNames)} ===");
            }
            foreach (string droppedSequenceName in diff.DroppedSequenceNames.OrderByDescending(s => s))
            {
                MigrationLogger.Log($"  [DROP SEQUENCE] {droppedSequenceName}");
                // Get schema from current schema (after migration)
                string schema = "dbo";
                if (currentSequences.TryGetValue(droppedSequenceName.ToLowerInvariant(), out SqlSequence? droppedSequence))
                {
                    schema = droppedSequence.Schema;
                }
                downScript.AppendLine(MigrationSqlGenerator.GenerateDropSequenceStatement(droppedSequenceName, schema));
            }
            // For Modify: drop the current sequence (OldSequence - after modification) before recreating the old one
            foreach (SequenceChange sequenceChange in diff.ModifiedSequences)
            {
                if (sequenceChange.ChangeType == SequenceChangeType.Modify && sequenceChange.OldSequence != null)
                {
                    downScript.AppendLine(MigrationSqlGenerator.GenerateDropSequenceStatement(sequenceChange.OldSequence.Name, sequenceChange.OldSequence.Schema));
                }
            }

            // Drop procedures that were added or modified by the migration (reverse order)
            // In downDiff, DroppedProcedureNames = procedures in currentSchema (after migration) but not in targetSchema (before migration)
            // These are procedures that were ADDED by the migration, so we need to DROP them in the down script
            foreach (string droppedProcedureName in diff.DroppedProcedureNames.OrderByDescending(p => p))
            {
                // Get schema from current schema (after migration)
                string schema = "dbo";
                if (currentProcedures.TryGetValue(droppedProcedureName.ToLowerInvariant(), out SqlStoredProcedure? droppedProcedure))
                {
                    schema = droppedProcedure.Schema;
                }
                downScript.Append(MigrationSqlGenerator.GenerateDropProcedureStatement(droppedProcedureName, schema));
                downScript.AppendLine();
            }
            // For Modify: drop the current procedure (OldProcedure - after modification) before recreating the old one
            foreach (ProcedureChange procedureChange in diff.ModifiedProcedures)
            {
                if (procedureChange.ChangeType == ProcedureChangeType.Modify && procedureChange.OldProcedure != null)
                {
                    downScript.Append(MigrationSqlGenerator.GenerateDropProcedureStatement(procedureChange.OldProcedure.Name, procedureChange.OldProcedure.Schema));
                    downScript.AppendLine();
                }
            }

            // Recreate sequences that were dropped or modified by the migration
            // In downDiff, NewSequences = sequences in targetSchema (before migration) but not in currentSchema (after migration)
            // These are sequences that were DROPPED by the migration, so we need to RECREATE them in the down script
            if (diff.NewSequences.Count > 0)
            {
                MigrationLogger.Log($"\n=== Recreating sequences (were dropped by migration): {string.Join(", ", diff.NewSequences.Select(s => s.Name))} ===");
            }
            foreach (SqlSequence newSequence in diff.NewSequences.OrderBy(s => s.Name))
            {
                MigrationLogger.Log($"  [CREATE SEQUENCE] {newSequence.Name}");
                downScript.Append(MigrationSqlGenerator.GenerateCreateSequenceStatement(newSequence));
                downScript.AppendLine();
            }
            // For Modify: restore the old sequence (NewSequence - before modification)
            foreach (SequenceChange sequenceChange in diff.ModifiedSequences)
            {
                if (sequenceChange.ChangeType == SequenceChangeType.Modify && sequenceChange.NewSequence != null)
                {
                    downScript.Append(MigrationSqlGenerator.GenerateCreateSequenceStatement(sequenceChange.NewSequence));
                    downScript.AppendLine();
                }
            }

            // Recreate procedures that were dropped or modified by the migration
            // In downDiff, NewProcedures = procedures in targetSchema (before migration) but not in currentSchema (after migration)
            // These are procedures that were DROPPED by the migration, so we need to RECREATE them in the down script
            foreach (SqlStoredProcedure newProcedure in diff.NewProcedures.OrderBy(p => p.Name))
            {
                downScript.Append(MigrationSqlGenerator.GenerateCreateProcedureStatement(newProcedure));
                downScript.AppendLine();
            }
            // For Modify: restore the old procedure (NewProcedure - before modification)
            foreach (ProcedureChange procedureChange in diff.ModifiedProcedures)
            {
                if (procedureChange.ChangeType == ProcedureChangeType.Modify && procedureChange.NewProcedure != null)
                {
                    downScript.Append(MigrationSqlGenerator.GenerateCreateProcedureStatement(procedureChange.NewProcedure));
                    downScript.AppendLine();
                }
            }

            downScript.AppendLine();

            // Build up script with phases
            // Transaction is managed by MigrationApplier.ExecuteMigrationScript using ADO.NET transactions
            StringBuilder upScript = new StringBuilder();
            upScript.AppendLine("-- Generated using Minfold, do not edit manually");
            upScript.AppendLine("SET XACT_ABORT ON;");
            upScript.AppendLine();

            int phaseNumber = 1;

            // Phase 0: Drop Procedures (before dropping tables)
            string phase0DropProceduresContent = phase0DropProcedures.ToString().Trim();
            if (!string.IsNullOrEmpty(phase0DropProceduresContent))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Drop Stored Procedures"));
                upScript.AppendLine(phase0DropProceduresContent);
                upScript.AppendLine();
                phaseNumber++;
            }

            // Phase 0: Drop Sequences (before dropping tables)
            string phase0DropSequencesContent = phase0DropSequences.ToString().Trim();
            if (!string.IsNullOrEmpty(phase0DropSequencesContent))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Drop Sequences"));
                upScript.AppendLine(phase0DropSequencesContent);
                upScript.AppendLine();
                phaseNumber++;
            }

            // Phase 0: Drop Foreign Keys for tables that will be dropped
            string phase0DropFksContent = phase0DropFks.ToString().Trim();
            if (!string.IsNullOrEmpty(phase0DropFksContent))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Drop Foreign Keys"));
                upScript.AppendLine(phase0DropFksContent);
                upScript.AppendLine();
                phaseNumber++;
            }

            // Phase 0: Drop Primary Key Constraints (before column modifications)
            string phase0DropPksContent = phase0DropPks.ToString().Trim();
            if (!string.IsNullOrEmpty(phase0DropPksContent))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Drop Primary Key Constraints"));
                upScript.AppendLine(phase0DropPksContent);
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

            // Phase 0.5: Create Sequences (before tables, so they can be used in table defaults)
            string phase0_5SequencesContent = phase0_5Sequences.ToString().Trim();
            if (!string.IsNullOrEmpty(phase0_5SequencesContent))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Create Sequences"));
                upScript.AppendLine(phase0_5SequencesContent);
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

            // Phase 3: Add Foreign Key Constraints and Primary Key Constraints
            string phase3Content = phase3Constraints.ToString().Trim();
            if (!string.IsNullOrEmpty(phase3Content))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Add Foreign Key Constraints and Primary Key Constraints"));
                upScript.AppendLine(phase3Content);
                upScript.AppendLine();
                phaseNumber++;
            }

            // Phase 4: Create Stored Procedures (after constraints)
            string phase4ProceduresContent = phase4Procedures.ToString().Trim();
            if (!string.IsNullOrEmpty(phase4ProceduresContent))
            {
                upScript.Append(MigrationSqlGenerator.GenerateSectionHeader(phaseNumber, "Create Stored Procedures"));
                upScript.AppendLine(phase4ProceduresContent);
                upScript.AppendLine();
            }

            string timestamp = MigrationUtilities.GetNextMigrationTimestamp();
            string migrationName = string.IsNullOrWhiteSpace(description) ? timestamp : $"{timestamp}_{description}";
            string migrationFolder = Path.Combine(migrationsPath, migrationName);
            Directory.CreateDirectory(migrationFolder);
            
            string upScriptPath = Path.Combine(migrationFolder, "up.sql");
            string downScriptPath = Path.Combine(migrationFolder, "down.sql");

            await File.WriteAllTextAsync(upScriptPath, upScript.ToString().TrimEnd());
            await File.WriteAllTextAsync(downScriptPath, downScript.ToString().TrimEnd());

            // Restore diff to upDiff for snapshot saving (we save the state after applying the UP migration)
            diff = originalDiff;

            // Save schema snapshot (target schema represents the state after this migration is applied)
            // We need to apply the changes to target schema to get the new target
            ConcurrentDictionary<string, SqlTable> newTargetSchema = MigrationSchemaSnapshot.ApplySchemaDiffToTarget(targetSchema, diff);
            ConcurrentDictionary<string, SqlSequence> newTargetSequences = MigrationSchemaSnapshot.ApplySequenceDiffToTarget(targetSequences, diff);
            ConcurrentDictionary<string, SqlStoredProcedure> newTargetProcedures = MigrationSchemaSnapshot.ApplyProcedureDiffToTarget(targetProcedures, diff);
            await MigrationSchemaSnapshot.SaveSchemaSnapshot(newTargetSchema, migrationName, codePath, newTargetSequences, newTargetProcedures);

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
            string migrationsPath = MigrationUtilities.GetMigrationsPath(codePath);
            Directory.CreateDirectory(migrationsPath);

            string timestamp = MigrationUtilities.GetNextMigrationTimestamp();
            string migrationName = string.IsNullOrWhiteSpace(description) ? timestamp : $"{timestamp}_{description}";
            string migrationFolder = Path.Combine(migrationsPath, migrationName);
            Directory.CreateDirectory(migrationFolder);
            
            string upScriptPath = Path.Combine(migrationFolder, "up.sql");
            string downScriptPath = Path.Combine(migrationFolder, "down.sql");

            // Create empty migration files
            await File.WriteAllTextAsync(upScriptPath, "-- Generated using Minfold, do not edit manually\n-- Add your migration SQL here");
            await File.WriteAllTextAsync(downScriptPath, "-- Generated using Minfold, do not edit manually\n-- Add your rollback SQL here");

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
