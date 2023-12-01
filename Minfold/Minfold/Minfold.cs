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
    private Dictionary<string, SqlTable> SqlSchema = [];
    private CsSource Source = new CsSource(string.Empty, string.Empty, []);
    internal readonly Dictionary<string, CsModelSource> tablesToModelsMap = [];
    internal readonly Dictionary<string, SqlTable> modelsToTablesMap = [];
    
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
                    if (owner.Columns.TryGetValue(fk.Column.ToLowerInvariant(), out SqlTableColumn? column))
                    {
                        column.ForeignKeys.Add(fk);
                    }
                }
            }
        }

        SqlSchema = schema;
        return schema;
    }

    private static object sLock;
    
    public SqlTable? MapModelToTable(string modelName)
    {
        string modelInvariant = modelName.ToLowerInvariant();
    
        // 1. same name
        if (SqlSchema.TryGetValue(modelInvariant, out SqlTable? tbl))
        {
            return tbl;
        }
    
        // 2. rules
        foreach (string pluralCandidate in Inflector.Plural(modelInvariant))
        {
            if (SqlSchema.TryGetValue($"{pluralCandidate}", out SqlTable? tbl3))
            {
                return tbl3;
            }
        }
    
        // 3. generic "s"
        if (SqlSchema.TryGetValue($"{modelInvariant}s", out SqlTable? tbl2))
        {
            return tbl2;
        }
    
        // 4. generic suffixes
        foreach (string suffix in Inflector.Suffixes)
        {
            if (SqlSchema.TryGetValue($"{modelInvariant}{suffix}", out SqlTable? tbl3))
            {
                return tbl3;
            }
        }

        return null;   
        
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

        CsSource source = new CsSource("", "", new ConcurrentDictionary<string, CsModelSource>());
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

            if (daoPaths.TryGetValue(fn, out string? daoPath) && !string.IsNullOrWhiteSpace(fn))
            {
                string modelCode = await File.ReadAllTextAsync(path, token);
                string daoCode = await File.ReadAllTextAsync(daoPath, token);
                SyntaxTree modelAst = CSharpSyntaxTree.ParseText(modelCode, cancellationToken: token);
                SyntaxTree daoAst = CSharpSyntaxTree.ParseText(daoCode, cancellationToken: token);
                SyntaxNode root = await modelAst.GetRootAsync(token);

                CsModelSource modelSource = new CsModelSource(fn, path, daoPath, modelCode, daoCode, modelAst, daoAst, "", MapModelToTable(fn.ToLowerInvariant()), root, []);
                PropertyMapper modelClassVisitor = new PropertyMapper(fn, modelSource);
                modelClassVisitor.Visit(root);
                
                source.Models.TryAdd(fn.ToLowerInvariant(), modelSource);
            }
        });

        Source = source;
        return source;
    }

    public async Task<string> UpdateModel(CsModelSource tree, SqlTable table, Dictionary<string, CsModelSource> tablesMap)
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
        
        ModelClassRewriter modelClassVisitor = new ModelClassRewriter(table.Name, table, null, tablesMap, tree);
        CompilationUnitSyntax newNode = (CompilationUnitSyntax)modelClassVisitor.Visit(tree.ModelRootNode);

        bool usingAnnotations = newNode.Usings.Any(x => x.NamespaceOrType is QualifiedNameSyntax { Left: IdentifierNameSyntax { Identifier.ValueText: "Minfold" }, Right.Identifier.ValueText: "Annotations" });

        if (!usingAnnotations)
        {
            QualifiedNameSyntax name = SyntaxFactory.QualifiedName(SyntaxFactory.IdentifierName("Minfold"), SyntaxFactory.IdentifierName("Annotations"));
            newNode = newNode.AddUsings(SyntaxFactory.UsingDirective(name).NormalizeWhitespace());   
        }
        
        return newNode.NormalizeWhitespace().ToFullString();
    }

    void MapTables()
    {
        tablesToModelsMap.Clear();
        modelsToTablesMap.Clear();
        
        foreach (KeyValuePair<string, CsModelSource> source in Source.Models)
        {
            if (string.IsNullOrWhiteSpace(source.Value.Name))
            {
                return;
            }

            if (source.Value.Table is not null)
            {
                tablesToModelsMap.TryAdd(source.Value.Table.Name.ToLowerInvariant(), source.Value);
                modelsToTablesMap.TryAdd(source.Value.Name.ToLowerInvariant(), source.Value.Table);
            }
        }
    }

    public async Task Synchronize(string sqlConn, string dbName, string codePath)
    {
        await AnalyzeSqlSchema(sqlConn, dbName);
        await LoadCsCode(codePath);
        MapTables();

        await Parallel.ForEachAsync(Source.Models, async (source, token) =>
        {
            SqlTable? table = null;

            if (source.Value.Name is not "User")
            {
                return;
            }
            
            if (string.IsNullOrWhiteSpace(source.Value.Name))
            {
                return;
            }

            SqlTable? mappedTable = null;

            if (modelsToTablesMap.TryGetValue(source.Key, out SqlTable? tbl))
            {
                mappedTable = tbl;
            }

            if (mappedTable is null)
            {
                return;
            }
            
            string str = await UpdateModel(source.Value, mappedTable, tablesToModelsMap);
            await File.WriteAllTextAsync(source.Value.ModelPath, str, token);   
        });
        
        int z = 0;
    }
}