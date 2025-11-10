using System.CommandLine.Builder;
using System.CommandLine.Parsing;
using System.Data;
using System.Diagnostics;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Minfold.Cli;
using System.CommandLine;

class Program
{
    public static async Task<int> Main(string[] args)
    {
        string? database = null;
        string? conn = null;
        string? codePath = null;
        bool decorateStd = false;
        
        Option<string> databaseOption = new Option<string>("--database", "The database to scaffold")
        {
            IsRequired = true
        };
        
        Option<bool?> decorateStreamsOption = new Option<bool?>("--stdDecorate", "Decorates stdout,stderror and each output line with a prefix info|warn|err");
        
        databaseOption.AddAlias("-d");
        
        Option<string> connStringOption = new Option<string>("--connection", "The database connection string")
        {
            IsRequired = true
        };
        
        connStringOption.AddAlias("-c");
        
        Option<string> codePathOption = new Option<string>("--codePath", "Path to a folder containing a single .csproj project. If multiple projects are present, use --project to choose one.")
        {
            IsRequired = true
        };
        
        codePathOption.AddAlias("-p");

        RootCommand rootCommand = new RootCommand("Minfold - Database schema synchronization and migration tool")
        {
            Name = "minfold"
        };
        
        // Make root command options optional when showing help
        databaseOption.IsRequired = false;
        connStringOption.IsRequired = false;
        codePathOption.IsRequired = false;
        
        Option<bool> rootDryRunOption = new Option<bool>("--dry-run", "Show what would be synchronized without making changes");
        
        rootCommand.AddOption(databaseOption);
        rootCommand.AddOption(connStringOption);
        rootCommand.AddOption(codePathOption);
        rootCommand.AddOption(decorateStreamsOption);
        rootCommand.AddOption(rootDryRunOption);
        
        // Migrations command group
        Command migrationsCommand = new Command("migrations", "Manage database migrations")
        {
            TreatUnmatchedTokensAsErrors = false
        };
        Option<bool> debugOption = new Option<bool>("--debug", "Enable debug logging for migrations");
        migrationsCommand.AddOption(databaseOption);
        migrationsCommand.AddOption(connStringOption);
        migrationsCommand.AddOption(codePathOption);
        migrationsCommand.AddOption(debugOption);
        
        // migrations apply
        Command applyCommand = new Command("apply", "Apply all pending migrations");
        Option<bool> dryRunOption = new Option<bool>("--dry-run", "Show what would be applied without executing");
        applyCommand.AddOption(dryRunOption);
        applyCommand.AddOption(debugOption);
        applyCommand.SetHandler(async (string? db, string? connection, string? path, bool dryRun, bool debug) =>
        {
            if (debug)
            {
                MigrationLogger.SetLogger(Console.WriteLine);
            }
            
            if (connection is null || db is null || path is null)
            {
                await Console.Error.WriteLineAsync("Missing required options: --connection, --database, --codePath");
                Environment.Exit(1);
                return;
            }
            
            ResultOrException<MigrationApplyResult> result = await MigrationApplier.ApplyMigrations(connection, db, path, dryRun);
            
            if (result.Exception is not null)
            {
                await Console.Error.WriteLineAsync($"Error applying migrations: {result.Exception.Message}");
                Environment.Exit(1);
                return;
            }
            
            if (result.Result is null)
            {
                await Console.Error.WriteLineAsync("Unknown error applying migrations");
                Environment.Exit(1);
                return;
            }
            
            if (dryRun)
            {
                Console.WriteLine($"Would apply {result.Result.AppliedMigrations.Count} migration(s):");
                foreach (string migration in result.Result.AppliedMigrations)
                {
                    Console.WriteLine($"  - {migration}");
                }
            }
            else
            {
                Console.WriteLine($"Applied {result.Result.AppliedMigrations.Count} migration(s):");
                foreach (string migration in result.Result.AppliedMigrations)
                {
                    Console.WriteLine($"  - {migration}");
                }
            }
        }, databaseOption, connStringOption, codePathOption, dryRunOption, debugOption);
        
        // migrations generate
        Command generateCommand = new Command("generate", "Generate a new migration");
        Argument<string?> descriptionArgument = new Argument<string?>(
            "description", 
            () => null, 
            "Migration description (optional, defaults to timestamp only)")
        {
            Arity = ArgumentArity.ZeroOrOne
        };
        Option<string?> descriptionOption = new Option<string?>("--description", "Migration description (optional, defaults to timestamp only)");
        Option<bool> generateDryRunOption = new Option<bool>("--dry-run", "Show what would be generated without creating files");
        Option<string?> schemasOption = new Option<string?>("--schemas", "Comma-separated list of schemas to include (defaults to 'dbo')");
        generateCommand.AddArgument(descriptionArgument);
        generateCommand.AddOption(descriptionOption);
        generateCommand.AddOption(generateDryRunOption);
        generateCommand.AddOption(schemasOption);
        generateCommand.AddOption(debugOption);
        generateCommand.SetHandler(async (string? db, string? connection, string? path, string? descArg, string? descOpt, bool dryRun, string? schemas, bool debug) =>
        {
            if (debug)
            {
                MigrationLogger.SetLogger(Console.WriteLine);
            }
            
            if (connection is null || db is null || path is null)
            {
                await Console.Error.WriteLineAsync("Missing required options: --connection, --database, --codePath");
                Environment.Exit(1);
                return;
            }
            
            string description = descArg ?? descOpt ?? string.Empty;
            
            // Parse schemas from CSV string, default to ["dbo"]
            List<string> schemasList = ["dbo"];
            if (!string.IsNullOrWhiteSpace(schemas))
            {
                schemasList = schemas.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                    .Where(s => !string.IsNullOrWhiteSpace(s))
                    .ToList();
                
                // If parsing resulted in empty list, fall back to dbo
                if (schemasList.Count == 0)
                {
                    schemasList = ["dbo"];
                }
            }
            
            // Check if migrations exist
            List<MigrationInfo> existingMigrations = MigrationApplier.GetMigrationFiles(path);
            
            if (dryRun)
            {
                if (existingMigrations.Count == 0)
                {
                    Console.WriteLine("Would generate initial migration from database schema");
                    Console.WriteLine($"  Description: {description}");
                    Console.WriteLine($"  Schemas: {string.Join(", ", schemasList)}");
                }
                else
                {
                    Console.WriteLine("Would create empty migration file");
                    Console.WriteLine($"  Description: {description}");
                    Console.WriteLine($"  Schemas: {string.Join(", ", schemasList)}");
                }
                return;
            }
            ResultOrException<MigrationGenerationResult> result;
            
            if (existingMigrations.Count == 0)
            {
                // Generate initial migration from database
                result = await MigrationGenerator.GenerateInitialMigration(connection, db, path, description, schemasList);
            }
            else
            {
                // Generate incremental migration comparing current DB to last migration
                result = await MigrationGenerator.GenerateIncrementalMigration(connection, db, path, description, schemasList);
            }
            
            if (result.Exception is not null)
            {
                if (result.Exception is MinfoldMigrationDbUpToDateException)
                {
                    Console.WriteLine("Database already up to date.");
                }
                else
                {
                    await Console.Error.WriteLineAsync($"Error generating migration: {result.Exception.Message}");
                }
                Environment.Exit(1);
                return;
            }
            
            if (result.Result is null)
            {
                await Console.Error.WriteLineAsync("Unknown error generating migration");
                Environment.Exit(1);
                return;
            }
            
            Console.WriteLine($"Created migration: {result.Result.MigrationName}");
            Console.WriteLine($"  Up script: {result.Result.UpScriptPath}");
            Console.WriteLine($"  Down script: {result.Result.DownScriptPath}");
            
            // Mark migration as applied since this is database-first (database already has the schema/changes)
            try
            {
                ResultOrException<bool> ensureResult = await MigrationApplier.EnsureMigrationsTableExists(connection, db);
                if (ensureResult.Exception is null)
                {
                    await MigrationApplier.RecordMigrationApplied(connection, result.Result.MigrationName, db);
                }
                else
                {
                    await Console.Error.WriteLineAsync($"Warning: Could not mark migration as applied: {ensureResult.Exception.Message}");
                }
            }
            catch (Exception ex)
            {
                await Console.Error.WriteLineAsync($"Warning: Could not mark migration as applied: {ex.Message}");
            }
        }, databaseOption, connStringOption, codePathOption, descriptionArgument, descriptionOption, generateDryRunOption, schemasOption, debugOption);
        
        // migrations goto
        Command gotoCommand = new Command("goto", "Apply or rollback migrations to reach a specific migration state");
        Argument<string> targetMigrationArgument = new Argument<string>("migration-name", "Target migration name");
        Option<bool> gotoDryRunOption = new Option<bool>("--dry-run", "Show what would be applied/rolled back without executing");
        gotoCommand.AddArgument(targetMigrationArgument);
        gotoCommand.AddOption(gotoDryRunOption);
        gotoCommand.AddOption(debugOption);
        gotoCommand.SetHandler(async (string? db, string? connection, string? path, string targetMigration, bool dryRun, bool debug) =>
        {
            if (debug)
            {
                MigrationLogger.SetLogger(Console.WriteLine);
            }
            
            if (connection is null || db is null || path is null)
            {
                await Console.Error.WriteLineAsync("Missing required options: --connection, --database, --codePath");
                Environment.Exit(1);
                return;
            }
            
            ResultOrException<MigrationGotoResult> result = await MigrationApplier.GotoMigration(connection, db, path, targetMigration, dryRun);
            
            if (result.Exception is not null)
            {
                await Console.Error.WriteLineAsync($"Error: {result.Exception.Message}");
                Environment.Exit(1);
                return;
            }
            
            if (result.Result is null)
            {
                await Console.Error.WriteLineAsync("Unknown error");
                Environment.Exit(1);
                return;
            }
            
            if (dryRun)
            {
                if (result.Result.AppliedMigrations.Count > 0)
                {
                    Console.WriteLine($"Would apply {result.Result.AppliedMigrations.Count} migration(s):");
                    foreach (string migration in result.Result.AppliedMigrations)
                    {
                        Console.WriteLine($"  - {migration}");
                    }
                }
                
                if (result.Result.RolledBackMigrations.Count > 0)
                {
                    Console.WriteLine($"Would rollback {result.Result.RolledBackMigrations.Count} migration(s):");
                    foreach (string migration in result.Result.RolledBackMigrations)
                    {
                        Console.WriteLine($"  - {migration}");
                    }
                }
            }
            else
            {
                if (result.Result.AppliedMigrations.Count > 0)
                {
                    Console.WriteLine($"Applied {result.Result.AppliedMigrations.Count} migration(s):");
                    foreach (string migration in result.Result.AppliedMigrations)
                    {
                        Console.WriteLine($"  - {migration}");
                    }
                }
                
                if (result.Result.RolledBackMigrations.Count > 0)
                {
                    Console.WriteLine($"Rolled back {result.Result.RolledBackMigrations.Count} migration(s):");
                    foreach (string migration in result.Result.RolledBackMigrations)
                    {
                        Console.WriteLine($"  - {migration}");
                    }
                }
            }
        }, databaseOption, connStringOption, codePathOption, targetMigrationArgument, gotoDryRunOption, debugOption);
        
        // migrations list
        Command listCommand = new Command("list", "List all migrations with their status");
        listCommand.AddOption(debugOption);
        listCommand.SetHandler(async (string? db, string? connection, string? path, bool debug) =>
        {
            if (debug)
            {
                MigrationLogger.SetLogger(Console.WriteLine);
            }
            
            if (connection is null || db is null || path is null)
            {
                await Console.Error.WriteLineAsync("Missing required options: --connection, --database, --codePath");
                Environment.Exit(1);
                return;
            }
            
            List<MigrationInfo> allMigrations = MigrationApplier.GetMigrationFiles(path);
            ResultOrException<List<string>> appliedResult = await MigrationApplier.GetAppliedMigrations(connection, db);
            
            HashSet<string> appliedSet = appliedResult.Result?.ToHashSet() ?? new HashSet<string>();
            
            // Find the last applied migration (current migration)
            string? currentMigration = null;
            if (appliedResult.Result is not null && appliedResult.Result.Count > 0)
            {
                // Find the migration that is both applied and has the highest timestamp
                foreach (MigrationInfo migration in allMigrations.OrderByDescending(m => m.Timestamp))
                {
                    if (appliedSet.Contains(migration.MigrationName))
                    {
                        currentMigration = migration.MigrationName;
                        break;
                    }
                }
            }
            
            Console.WriteLine("Migrations:");
            Console.WriteLine($"{"Status",-10} {"Migration Name",-50} {"Description"}");
            Console.WriteLine(new string('-', 100));
            
            foreach (MigrationInfo migration in allMigrations)
            {
                string status;
                if (migration.MigrationName == currentMigration)
                {
                    status = "CURRENT";
                }
                else if (appliedSet.Contains(migration.MigrationName))
                {
                    status = "APPLIED";
                }
                else
                {
                    status = "PENDING";
                }
                Console.WriteLine($"{status,-10} {migration.MigrationName,-50} {migration.Description}");
            }
        }, databaseOption, connStringOption, codePathOption, debugOption);
        
        // migrations claim
        Command claimCommand = new Command("claim", "Claim that the database is in a specific migration state without running migrations");
        Argument<string> claimMigrationArgument = new Argument<string>("migration-name", "Migration name to claim (or 'latest' for the latest migration)");
        Option<bool> forceOption = new Option<bool>("--force", "Override schema verification and claim the migration even if schemas don't match");
        claimCommand.AddArgument(claimMigrationArgument);
        claimCommand.AddOption(forceOption);
        claimCommand.AddOption(debugOption);
        claimCommand.SetHandler(async (string? db, string? connection, string? path, string migrationName, bool force, bool debug) =>
        {
            if (debug)
            {
                MigrationLogger.SetLogger(Console.WriteLine);
            }
            
            if (connection is null || db is null || path is null)
            {
                await Console.Error.WriteLineAsync("Missing required options: --connection, --database, --codePath");
                Environment.Exit(1);
                return;
            }
            
            ResultOrException<MigrationClaimResult> result = await MigrationApplier.ClaimMigration(connection, db, path, migrationName, force);
            
            if (result.Exception is not null)
            {
                await Console.Error.WriteLineAsync(result.Exception.Message);
                Environment.Exit(1);
                return;
            }
            
            if (result.Result is null)
            {
                await Console.Error.WriteLineAsync("Unknown error claiming migration");
                Environment.Exit(1);
                return;
            }
            
            if (result.Result.VerificationPassed)
            {
                Console.WriteLine($"Successfully claimed migration: {result.Result.ClaimedMigration}");
            }
            else
            {
                Console.WriteLine($"Claimed migration: {result.Result.ClaimedMigration} (with --force override)");
            }
        }, databaseOption, connStringOption, codePathOption, claimMigrationArgument, forceOption, debugOption);
        
        migrationsCommand.AddCommand(applyCommand);
        migrationsCommand.AddCommand(generateCommand);
        migrationsCommand.AddCommand(gotoCommand);
        migrationsCommand.AddCommand(listCommand);
        migrationsCommand.AddCommand(claimCommand);
        
        rootCommand.AddCommand(migrationsCommand);
        
        rootCommand.SetHandler(async (string? db, string? connection, string? path, bool? decorate, bool dryRun) =>
        {
            database = db;
            conn = connection;
            codePath = path;
            decorateStd = decorate ?? false;
            
            if (database is null || conn is null || codePath is null)
            {
                Console.Error.WriteLine("Missing required options: --connection, --database, --codePath");
                Console.Error.WriteLine("Run 'minfold --help' for more information.");
                Environment.Exit(1);
                return;
            }
            
            await ExecuteSynchronize(conn, database, codePath, decorateStd, dryRun);
        }, databaseOption, connStringOption, codePathOption, decorateStreamsOption, rootDryRunOption);
        
        CommandLineBuilder commandLineBuilder = new CommandLineBuilder(rootCommand);
        commandLineBuilder.AddMiddleware((context, next) => next(context));
        commandLineBuilder.UseDefaults();
        Parser parser = commandLineBuilder.Build();
        int result = await parser.InvokeAsync(args);

        return result;
    }
    
    private static async Task ExecuteSynchronize(string conn, string database, string codePath, bool decorateStd, bool dryRun)
    {
        if (dryRun)
        {
            Console.WriteLine("DRY RUN MODE - No changes will be made");
            Console.WriteLine("======================================");
        }

        Stopwatch sw = new Stopwatch();
        sw.Start();

        MinfoldOptions options = new MinfoldOptions { DecorateMessages = decorateStd, DryRun = dryRun };
        
        Minfold m = new Minfold();
        MinfoldResult synchronizeResult = await m.Synchronize(conn, database, codePath, options);

        //string str = Synchronizer.Synchronize();
        
        sw.Stop();

        void WriteError(string msg)
        {
            if (options.DecorateMessages)
            {
                Console.WriteLine($"<|err,err|>{msg}");
                return;
            }
            
            Console.WriteLine(msg);
        }

        void WriteSuccess(string msg)
        {
            if (options.DecorateMessages)
            {
                Console.WriteLine($"<|out,info|>{msg}");
                return;
            }
            
            Console.WriteLine(msg);
        }
        
        if (synchronizeResult.Error is not null)
        {
            WriteError(synchronizeResult.Error.Messsage);
            WriteError("Raw exception:");
            WriteError(synchronizeResult.Error.Exception.Message);
            Environment.Exit(1);
            return;
        }
        
        if (dryRun)
        {
            WriteSuccess($"Minfold Synchronize dry-run completed successfully in {sw.Elapsed.ToString()}");
            WriteSuccess("No changes were made. Remove --dry-run to apply changes.");
        }
        else
        {
            WriteSuccess($"Minfold Synchronize finished successfully in {sw.Elapsed.ToString()}");
        }
    }
}