using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

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

    public async Task<string> UpdateModel(CsModelSource tree, SqlTable table)
    {
        /*DocumentId docId;
        
        AdhocWorkspace TestWorkspace()
        {
            var workspace = new AdhocWorkspace();

            string projName = "NewProject";
            var projectId = ProjectId.CreateNewId();
            var versionStamp = VersionStamp.Create();
            var projectInfo = ProjectInfo.Create(projectId, versionStamp, projName, projName, LanguageNames.CSharp);
            var newProject = workspace.AddProject(projectInfo);
            var sourceText = SourceText.From(tree.ModelSourceCode);
            Document? newDocument = workspace.AddDocument(newProject.Id, "NewFile.cs", sourceText);
            docId = newDocument.Id;
            return workspace;
        }

        AdhocWorkspace wrks = TestWorkspace();
        Document? docu = wrks.CurrentSolution.GetDocument(docId);
        SemanticModel? model = await docu.GetSemanticModelAsync();*/
        
        /*
            var references = new List<MetadataReference>
            {
                MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(Console).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(System.Runtime.AssemblyTargetedPatchBandAttribute).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(Microsoft.CSharp.RuntimeBinder.CSharpArgumentInfo).Assembly.Location),
            };
         
         CSharpCompilation compilation = CSharpCompilation.Create("MyCompilation")
            .WithOptions(new CSharpCompilationOptions(Microsoft.CodeAnalysis.OutputKind.DynamicallyLinkedLibrary))
            .AddReferences(references)
            .AddSyntaxTrees(tree.ModelAst);
        ImmutableArray<Diagnostic> diag = compilation.GetDiagnostics();*/

        SyntaxNode root = await tree.ModelAst.GetRootAsync();
        ModelClassRewriter modelClassVisitor = new ModelClassRewriter(table.Name, table, null);
        CompilationUnitSyntax newNode = (CompilationUnitSyntax)modelClassVisitor.Visit(root);
        return newNode.NormalizeWhitespace().ToFullString();
    }

    public async Task Synchronize(string sqlConn, string dbName, string codePath)
    {
        await AnalyzeSqlSchema(sqlConn, dbName);
        await LoadCsCode(codePath);

        string str = await UpdateModel(Source.Models.FirstOrDefault(x => x.Name is "User")!, SqlChema.FirstOrDefault(x => x.Key is "User").Value);
        await File.WriteAllTextAsync(Source.Models.FirstOrDefault(x => x.Name is "User")!.ModelPath, str);
        int z = 0;
    }
}