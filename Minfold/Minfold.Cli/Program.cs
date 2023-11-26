using System.CommandLine.Builder;
using System.CommandLine.Parsing;
using System.Data;
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
        
        Option<string> codePathOption = new("--codePath", "Path to a folder containing Db.cs, 'Dao, Models' folders")
        {
            IsRequired = true
        };
        
        connStringOption.AddAlias("-p");

        RootCommand rootCommand = new RootCommand("Minfold")
        {
            databaseOption, connStringOption, codePathOption
        };

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
        
        Minfold m = new Minfold();
        await m.Synchronize(conn, database, codePath);
        return 0;
    }
}