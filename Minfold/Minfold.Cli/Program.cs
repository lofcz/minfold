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
        migrationsCommand.AddOption(databaseOption);
        migrationsCommand.AddOption(connStringOption);
        migrationsCommand.AddOption(codePathOption);
        
        // migrations apply
        Command applyCommand = new Command("apply", "Apply all pending migrations");
        Option<bool> dryRunOption = new Option<bool>("--dry-run", "Show what would be applied without executing");
        applyCommand.AddOption(dryRunOption);
        applyCommand.SetHandler(async (string? db, string? connection, string? path, bool dryRun) =>
        {
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
        }, databaseOption, connStringOption, codePathOption, dryRunOption);
        
        // migrations generate
        Command generateCommand = new Command("generate", "Generate a new migration");
        Argument<string?> descriptionArgument = new Argument<string?>("description", "Migration description (optional)");
        Option<string?> descriptionOption = new Option<string?>("--description", "Migration description");
        Option<bool> generateDryRunOption = new Option<bool>("--dry-run", "Show what would be generated without creating files");
        generateCommand.AddArgument(descriptionArgument);
        generateCommand.AddOption(descriptionOption);
        generateCommand.AddOption(generateDryRunOption);
        generateCommand.SetHandler(async (string? db, string? connection, string? path, string? descArg, string? descOpt, bool dryRun) =>
        {
            if (connection is null || db is null || path is null)
            {
                await Console.Error.WriteLineAsync("Missing required options: --connection, --database, --codePath");
                Environment.Exit(1);
                return;
            }
            
            string description = descArg ?? descOpt ?? "EmptyMigration";
            
            // Check if migrations exist
            List<MigrationInfo> existingMigrations = MigrationApplier.GetMigrationFiles(path);
            
            if (dryRun)
            {
                if (existingMigrations.Count == 0)
                {
                    Console.WriteLine("Would generate initial migration from database schema");
                    Console.WriteLine($"  Description: {description}");
                }
                else
                {
                    Console.WriteLine("Would create empty migration file");
                    Console.WriteLine($"  Description: {description}");
                }
                return;
            }
            ResultOrException<MigrationGenerationResult> result;
            
            if (existingMigrations.Count == 0)
            {
                // Generate initial migration from database
                result = await MigrationGenerator.GenerateInitialMigration(connection, db, path, description);
            }
            else
            {
                // Generate incremental migration comparing current DB to last migration
                result = await MigrationGenerator.GenerateIncrementalMigration(connection, db, path, description);
            }
            
            if (result.Exception is not null)
            {
                await Console.Error.WriteLineAsync($"Error generating migration: {result.Exception.Message}");
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
        }, databaseOption, connStringOption, codePathOption, descriptionArgument, descriptionOption, generateDryRunOption);
        
        // migrations goto
        Command gotoCommand = new Command("goto", "Apply or rollback migrations to reach a specific migration state");
        Argument<string> targetMigrationArgument = new Argument<string>("migration-name", "Target migration name");
        Option<bool> gotoDryRunOption = new Option<bool>("--dry-run", "Show what would be applied/rolled back without executing");
        gotoCommand.AddArgument(targetMigrationArgument);
        gotoCommand.AddOption(gotoDryRunOption);
        gotoCommand.SetHandler(async (string? db, string? connection, string? path, string targetMigration, bool dryRun) =>
        {
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
        }, databaseOption, connStringOption, codePathOption, targetMigrationArgument, gotoDryRunOption);
        
        // migrations list
        Command listCommand = new Command("list", "List all migrations with their status");
        listCommand.SetHandler(async (string? db, string? connection, string? path) =>
        {
            if (connection is null || db is null || path is null)
            {
                await Console.Error.WriteLineAsync("Missing required options: --connection, --database, --codePath");
                Environment.Exit(1);
                return;
            }
            
            List<MigrationInfo> allMigrations = MigrationApplier.GetMigrationFiles(path);
            ResultOrException<List<string>> appliedResult = await MigrationApplier.GetAppliedMigrations(connection, db);
            
            HashSet<string> appliedSet = appliedResult.Result?.ToHashSet() ?? new HashSet<string>();
            
            Console.WriteLine("Migrations:");
            Console.WriteLine($"{"Status",-10} {"Migration Name",-50} {"Description"}");
            Console.WriteLine(new string('-', 100));
            
            foreach (MigrationInfo migration in allMigrations)
            {
                string status = appliedSet.Contains(migration.MigrationName) ? "APPLIED" : "PENDING";
                Console.WriteLine($"{status,-10} {migration.MigrationName,-50} {migration.Description}");
            }
        }, databaseOption, connStringOption, codePathOption);
        
        migrationsCommand.AddCommand(applyCommand);
        migrationsCommand.AddCommand(generateCommand);
        migrationsCommand.AddCommand(gotoCommand);
        migrationsCommand.AddCommand(listCommand);
        
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