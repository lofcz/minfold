using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Minfold;

public class Minfold
{
    private Dictionary<string, SqlTable> SqlChema = new Dictionary<string, SqlTable>();
    private CsSource Source = new CsSource(string.Empty, string.Empty, new List<CsModelSource>());
    
    public async Task<Dictionary<string, SqlTable>> AnalyzeSqlSchema(string sqlConn, string dbName)
    {
        SqlService ss = new SqlService(sqlConn);
        Dictionary<string, SqlTable> schema = await ss.GetSchema(dbName);
        
        foreach (KeyValuePair<string, List<SqlForeignKey>> foreignKeyList in await ss.GetForeignKeys(schema.Keys.ToList()))
        {
            if (schema.TryGetValue(foreignKeyList.Key, out SqlTable? owner))
            {
                foreach (SqlForeignKey fk in foreignKeyList.Value)
                {
                    owner.Columns.FirstOrDefault(x => x.Name == fk.Column)?.ForeignKeys.Add(fk);
                }
            }
        }

        SqlChema = schema;
        return schema;
    }

    public async Task<CsSource?> LoadCsCode(string folder)
    {
        string? dbCode = File.Exists($"{folder}\\Db.cs") ? await File.ReadAllTextAsync($"{folder}\\Db.cs") : null;

        if (dbCode is null)
        {
            return null;
        }

        if (!Directory.Exists($"{folder}\\Dao"))
        {
            return null;
        }
        
        if (!Directory.Exists($"{folder}\\Models"))
        {
            return null;
        }

        CsSource source = new CsSource("", "", new List<CsModelSource>());
        ConcurrentDictionary<string, string> daoPaths = new ConcurrentDictionary<string, string>();

        Parallel.ForEach(Directory.GetFiles($"{folder}\\Dao", string.Empty, SearchOption.AllDirectories), daoPath =>
        {
            string fn = Path.GetFileNameWithoutExtension(daoPath);

            if (fn.EndsWith("Dao"))
            {
                fn = fn[..^3];
            }

            daoPaths.TryAdd(fn, daoPath);
        });

        await Parallel.ForEachAsync(Directory.GetFiles($"{folder}\\Models", string.Empty, SearchOption.AllDirectories), async (path, token) =>
        {
            string fn = Path.GetFileNameWithoutExtension(path);

            if (daoPaths.TryGetValue(fn, out string? daoPath))
            {
                string modelCode = await File.ReadAllTextAsync(path, token);
                string daoCode = await File.ReadAllTextAsync(daoPath, token);
                SyntaxTree modelAst = CSharpSyntaxTree.ParseText(modelCode, cancellationToken: token);
                SyntaxTree daoAst = CSharpSyntaxTree.ParseText(daoCode, cancellationToken: token);
                source.Models.Add(new CsModelSource(fn, path, daoPath, modelCode, daoCode, modelAst, daoAst));
            }
        });

        Source = source;
        return source;
    }

    public async Task<string> UpdateModel(CsModelSource tree)
    {
        ClassRewriter classVisitor = new ClassRewriter();
        CompilationUnitSyntax newNode = (CompilationUnitSyntax)classVisitor.Visit(await tree.ModelAst.GetRootAsync());
        return newNode.NormalizeWhitespace().ToFullString();
    }

    public async Task Synchronize(string sqlConn, string dbName, string codePath)
    {
        await AnalyzeSqlSchema(sqlConn, dbName);
        await LoadCsCode(codePath);

        string str = await UpdateModel(Source.Models.FirstOrDefault(x =>x.Name is "User")!);
        int z = 0;
    }
}