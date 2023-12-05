using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Minfold;

public class Minfold
{
    private Dictionary<string, SqlTable> SqlSchema = [];
    private CsSource Source = new CsSource(string.Empty, string.Empty, [], string.Empty, string.Empty);
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
        const bool enableSlowChecks = false;
        
        string modelInvariant = modelName.ToLowerInvariant();
    
        // 1. same name
        if (SqlSchema.TryGetValue(modelInvariant, out SqlTable? tbl))
        {
            return tbl;
        }
    
        // 2. rules
        string? plural = modelInvariant.Plural();

        if (plural is not null)
        {
            if (SqlSchema.TryGetValue($"{plural}", out SqlTable? tbl3))
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
        
        // 5. table wrongly named in singular and model in plural
        if (enableSlowChecks)
        {
            foreach (string suffix in Inflector.Suffixes)
            {
                foreach (KeyValuePair<string, SqlTable> pair in SqlSchema)
                {
                    if (pair.Key.EndsWith(suffix))
                    {
                        string needle = pair.Key.ReplaceLast(suffix, string.Empty);

                        if (SqlSchema.TryGetValue(needle, out SqlTable? tbl4))
                        {
                            return tbl4;
                        }
                    }
                }
            }   
        }
        
        return null;   
    }
    
    public async Task<CsSource?> LoadCsCode(string folder)
    {
        string[] projFiles = Directory.GetFiles(folder, "*.csproj");

        if (projFiles.Length is not 1)
        {
            return null;
        }
        
        string prjName = Path.GetFileNameWithoutExtension(projFiles[0]);
        
        string? dbCode = File.Exists($"{folder}\\Dao\\Db.cs") ? await File.ReadAllTextAsync($"{folder}\\Dao\\Db.cs") : null;

        if (dbCode is null)
        {
            return null;
        }

        if (!Directory.Exists($"{folder}\\Dao\\Dao"))
        {
            return null;
        }
        
        if (!Directory.Exists($"{folder}\\Dao\\Models"))
        {
            return null;
        }

        CsSource source = new CsSource("", "", new ConcurrentDictionary<string, CsModelSource>(), prjName, folder);
        ConcurrentDictionary<string, string> daoPaths = new ConcurrentDictionary<string, string>();

        Parallel.ForEach(Directory.GetFiles($"{folder}\\Dao\\Dao", string.Empty, SearchOption.AllDirectories), daoPath =>
        {
            string fn = Path.GetFileNameWithoutExtension(daoPath);

            if (fn.EndsWith("Dao"))
            {
                fn = fn[..^3];
            }

            daoPaths.TryAdd(fn, daoPath);
        });

        await Parallel.ForEachAsync(Directory.GetFiles($"{folder}\\Dao\\Models", string.Empty, SearchOption.AllDirectories), async (path, token) =>
        {
            string fn = Path.GetFileNameWithoutExtension(path);
            string? daoCode = null;
            SyntaxTree? daoAst = null;
            
            if (daoPaths.TryGetValue(fn, out string? daoPath) && !string.IsNullOrWhiteSpace(fn))
            {
                daoCode = await File.ReadAllTextAsync(daoPath, token);
                daoAst = CSharpSyntaxTree.ParseText(daoCode, cancellationToken: token);
            }
            
            string modelCode = await File.ReadAllTextAsync(path, token);
            SyntaxTree modelAst = CSharpSyntaxTree.ParseText(modelCode, cancellationToken: token);
            SyntaxNode root = await modelAst.GetRootAsync(token);
            CsModelSource modelSource = new CsModelSource(fn, path, daoPath, modelCode, daoCode, modelAst, daoAst, "", MapModelToTable(fn.ToLowerInvariant()), root, []);
            PropertyMapper modelClassVisitor = new PropertyMapper(fn, modelSource);
            modelClassVisitor.Visit(root);
            source.Models.TryAdd(fn.ToLowerInvariant(), modelSource);
        });

        Source = source;
        return source;
    }
    
    public static ColumnDefaultVal? ColumnDefaultValue(string colName, SqlTableColumn? column)
    {
        if (column is null)
        {
            return null;
        }
        
        if (colName is "datecreated")
        {
            return new ColumnDefaultVal(column, "DateTime.Now", ColumnDefaultValTypes.Value);
        }
            
        return new ColumnDefaultVal(column, null, ColumnDefaultValTypes.UserAssigned);
    }

    const string propPrefix = "    ";
    const string propPrefix2 = "        ";
    
    static string Ident(string str, int level = 1)
    {
        return level is 2 ? $"{propPrefix2}{str}" : $"{propPrefix}{str}";
    }

    public static string? DumpCtor(string className, SqlTable table, CsPropertiesInfo? properties)
    {
        List<KeyValuePair<string, SqlTableColumn>> ctorCols = table.Columns.Where(x => x.Value is { IsComputed: false, IsIdentity: false }).ToList();
        List<ColumnDefaultVal> ctorDeclCols = ctorCols.OrderBy(x => x.Value.OrdinalPosition).Select(x => ColumnDefaultValue(x.Key, x.Value)).ToList();
        List<ColumnDefaultVal> ctorAssignCols = ctorDeclCols.Where(x => x.Type is not ColumnDefaultValTypes.Value).ToList();

        if (ctorCols.Count > 0)
        {
            return $$"""
             
                 public {{className}}({{DumpCtorPars()}})
                 {
             {{DumpCtorAssignments()}}
                 }
             """;
        }
        
        string DumpCtorAssignPar(ColumnDefaultVal par, bool last)
        {
            return Ident($"{ColumnName(par.Column).FirstCharToUpper()} = {(par.Type is ColumnDefaultValTypes.Value ? par.DefaultValue : PrefixReservedKeyword(ColumnName(par.Column).FirstCharToLower()))};", 2);
        } 

        string DumpCtorAssignments()
        {
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < ctorDeclCols.Count; i++)
            {
                bool last = i == ctorDeclCols.Count - 1;
                sb.Append(DumpCtorAssignPar(ctorDeclCols[i], last));

                if (!last)
                {
                    sb.Append(Environment.NewLine);   
                }
            }
            
            return sb.ToString();
        }

        string ColumnName(SqlTableColumn column)
        {
            if (properties is not null)
            {
                if (properties.Properties.TryGetValue(column.Name.ToLowerInvariant(), out CsPropertyInfo? info))
                {
                    return info.Name;
                }
            }

            return column.Name;
        }

        string DumpCtorParType(ColumnDefaultVal par)
        {
            if (properties?.Properties.TryGetValue(par.Column.Name.ToLowerInvariant(), out CsPropertyInfo? info) ?? false)
            {
                if (info.Decl.Token is not null && info.Decl.Type is SqlDbTypeExt.CsIdentifier && par.Column.SqlType is SqlDbTypeExt.Int or SqlDbTypeExt.SmallInt or SqlDbTypeExt.TinyInt)
                {
                    return $"{info.Decl.Token}{(par.Column.IsNullable ? "?" : string.Empty)}";
                }
            }
            
            return par.Column.SqlType.ToTypeSyntax(par.Column.IsNullable).ToFullString();
        }

        string? PrefixReservedKeyword(string? str)
        {
            if (str is "ref")
            {
                return "@ref";
            }

            return str;
        }

        string DumpCtorPar(ColumnDefaultVal par, bool last)
        {
            return $"{DumpCtorParType(par)} {PrefixReservedKeyword(ColumnName(par.Column).FirstCharToLower())}{(par.Type is ColumnDefaultValTypes.Optional ? $" = {par.DefaultValue}" : string.Empty)}{(last ? string.Empty : ", ")}";
        }
        
        string DumpCtorPars()
        {
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < ctorAssignCols.Count; i++)
            {
                sb.Append(DumpCtorPar(ctorAssignCols[i], i == ctorAssignCols.Count - 1));
            }
            
            return sb.ToString();
        }
        
        return null;
    }

    public async Task<string> GenerateModel(string className, SqlTable table, Dictionary<string, CsModelSource> tablesMap)
    {
        if (tablesMap.TryGetValue(table.Name.ToLowerInvariant(), out CsModelSource? model))
        {
            className = model.Name;
        }

        List<KeyValuePair<string, SqlTableColumn>> ctorCols = table.Columns.Where(x => !x.Value.IsComputed).ToList();
  
        void DumpProperty(StringBuilder sb, SqlTableColumn column, bool last, bool first, bool nextAnyFks)
        {
            if (!first && column.ForeignKeys.Count > 0)
            {
                sb.Append(Environment.NewLine);
            }
            
            foreach (SqlForeignKey fk in column.ForeignKeys)
            {
                string tblName = fk.RefTable.FirstCharToUpper() ?? string.Empty;
                string colName = fk.RefColumn.FirstCharToUpper() ?? string.Empty;
                
                if (tablesMap.TryGetValue(fk.RefTable.ToLowerInvariant(), out CsModelSource? tbl))
                {
                    tblName = tbl.Name;
                    
                    if (tbl.Columns.TryGetValue(colName.ToLowerInvariant(), out string? tblColName))
                    {
                        colName = tblColName;
                    }
                }

                string memberPrefix = string.Empty;

                if (!string.Equals(tblName, table.Name, StringComparison.InvariantCultureIgnoreCase))
                {
                    memberPrefix = $"{tblName}.";
                }
                
                sb.Append($$"""{{propPrefix}}[ReferenceKey(typeof({{tblName}}), nameof({{memberPrefix}}{{colName}}), {{(fk.NotEnforced ? "false" : "true")}})]""");
                sb.Append(Environment.NewLine);
            }
            
            sb.Append($$"""{{propPrefix}}public {{column.SqlType.ToTypeSyntax(column.IsNullable).ToFullString()}} {{column.Name.FirstCharToUpper()}} { get;{{(column.IsComputed ? "private" : string.Empty)}} set; }""");
            
            if (!nextAnyFks && !last && column.ForeignKeys.Count > 0)
            {
                sb.Append(Environment.NewLine);
            }
        }
        

        string DumpProperties()
        {
            StringBuilder sb = new StringBuilder();
 
            List<KeyValuePair<string, SqlTableColumn>> orderedCols = table.Columns.OrderBy(x => x.Value.OrdinalPosition).ToList();

            for (int i = 0; i < table.Columns.Count; i++)
            {
                KeyValuePair<string, SqlTableColumn> column = orderedCols[i];
                DumpProperty(sb, column.Value, i == table.Columns.Count - 1, i is 0, i < table.Columns.Count - 2 && orderedCols[i + 1].Value.ForeignKeys.Count > 0);
                sb.Append(Environment.NewLine);
            }

            return sb.ToString();
        }
            
        string str = $$"""
        using System;
        using System.Collections.Generic;
        using System.ComponentModel.DataAnnotations;
        using System.ComponentModel.DataAnnotations.Schema;
        using Microsoft.EntityFrameworkCore;
        using Minfold.Annotations;           
                       
        namespace {{Source.ProjectNamespace}};
        public class {{className}}
        {
        {{DumpProperties()}}
            public {{className}}()
            {
        
            }
        {{DumpCtor(className, table, null)}}
        }
        """;
        
        return str;
    }

    public async Task<ModelClassRewriteResult> UpdateModel(CsModelSource tree, SqlTable table, Dictionary<string, CsModelSource> tablesMap)
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

        string modelName = table.Name;
        
        if (tablesMap.TryGetValue(table.Name.ToLowerInvariant(), out CsModelSource? model))
        {
            modelName = model.Name;
        }
        
        ModelClassRewriter modelClassVisitor = new ModelClassRewriter(modelName, table, tablesMap, tree);
        CompilationUnitSyntax newNode = (CompilationUnitSyntax)modelClassVisitor.Visit(tree.ModelRootNode);

        bool usingAnnotations = newNode.Usings.Any(x => x.NamespaceOrType is QualifiedNameSyntax { Left: IdentifierNameSyntax { Identifier.ValueText: "Minfold" }, Right.Identifier.ValueText: "Annotations" });

        if (!usingAnnotations)
        {
            QualifiedNameSyntax name = SyntaxFactory.QualifiedName(SyntaxFactory.IdentifierName("Minfold"), SyntaxFactory.IdentifierName("Annotations"));
            newNode = newNode.AddUsings(SyntaxFactory.UsingDirective(name).NormalizeWhitespace());   
        }

        return new ModelClassRewriteResult(modelClassVisitor.ClassRewritten, newNode.NormalizeWhitespace().ToFullString());
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

        ConcurrentDictionary<string, bool> synchronizedTables = [];
        
        await Parallel.ForEachAsync(Source.Models, async (source, token) =>
        {
            SqlTable? table = null;
            
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
            
            ModelClassRewriteResult updateResult = await UpdateModel(source.Value, mappedTable, tablesToModelsMap);

            if (source.Value.Name.ToLowerInvariant() is "tenant")
            {
                int z = 0;
            }

            if (updateResult.Rewritten)
            {
                await File.WriteAllTextAsync(source.Value.ModelPath, updateResult.Text, token);
                synchronizedTables.TryAdd(mappedTable.Name.ToLowerInvariant(), true);   
            }
        });

        List<SqlTable> notSynchronized = []; 
        
        await Parallel.ForEachAsync(SqlSchema, async (source, token) =>
        {
            if (synchronizedTables.TryGetValue(source.Key, out bool synchronized) && synchronized)
            {
                return;
            }

            string className = source.Value.Name.FirstCharToUpper() ?? string.Empty;

            if (className is "LlmContextPairUserReviews")
            {
                int z = 0;
            }
            
            if (tablesToModelsMap.TryGetValue(source.Value.Name.ToLowerInvariant(), out CsModelSource? tbl))
            {
                className = tbl.Name.FirstCharToUpper() ?? string.Empty;
            }
            else
            {
                string? singular = className.Singular();

                if (singular is not null)
                {
                    className = singular;
                }
            }
            
            string str = await GenerateModel(className, source.Value, tablesToModelsMap);
            await File.WriteAllTextAsync($"{Source.ProjectPath}\\Dao\\Models\\{className}.cs", str, token);
        });

        int z = 0;
    }
}