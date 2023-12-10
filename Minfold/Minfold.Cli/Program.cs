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
        
        Option<string> databaseOption = new("--database", "The database to scaffold")
        {
            IsRequired = true
        };
        
        databaseOption.AddAlias("-d");
        
        Option<string> connStringOption = new("--connection", "The database connection string")
        {
            IsRequired = true
        };
        
        connStringOption.AddAlias("-c");
        
        Option<string> codePathOption = new("--codePath", "Path to a folder containing a single .csproj project. If multiple projects are present, use --project to choose one.")
        {
            IsRequired = true
        };
        
        codePathOption.AddAlias("-p");

        RootCommand rootCommand = new RootCommand("Minfold")
        {
            Name = "minfold"
        };
        
        rootCommand.AddOption(databaseOption);
        rootCommand.AddOption(connStringOption);
        rootCommand.AddOption(codePathOption);
        
        rootCommand.SetHandler(ctx =>
        {
            database = ctx.ParseResult.GetValueForOption(databaseOption);
            conn = ctx.ParseResult.GetValueForOption(connStringOption);
            codePath = ctx.ParseResult.GetValueForOption(codePathOption);
            
            ctx.ExitCode = database is null || conn is null || codePath is null ? 1 : 0;
        });
        
        CommandLineBuilder commandLineBuilder = new CommandLineBuilder(rootCommand);
        commandLineBuilder.AddMiddleware((context, next) => next(context));
        commandLineBuilder.UseDefaults();
        Parser parser = commandLineBuilder.Build();
        int result = await parser.InvokeAsync(args);

        if (result is not 0)
        {
            return result;
        }

        if (conn is null || database is null || codePath is null)
        {
            return result;
        }

        Stopwatch sw = new Stopwatch();
        sw.Start();
        
        Minfold m = new Minfold();
        MinfoldResult synchronizeResult = await m.Synchronize(conn, database, codePath);

        sw.Stop();
        
        if (synchronizeResult.Error is not null)
        {
            Console.WriteLine(synchronizeResult.Error.Messsage);
            Console.WriteLine("Raw exception:");
            Console.WriteLine(synchronizeResult.Error.Exception.Message);
            return 1;
        }
        
        Console.WriteLine($"Minfold Synchronize finished successfully in {sw.Elapsed.ToString()}");
        return 0;
    }
}