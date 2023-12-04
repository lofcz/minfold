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

    record ColumnDefaultVal(SqlTableColumn Column, string? DefaultValue, ColumnDefaultValTypes Type);

    enum ColumnDefaultValTypes
    {
        UserAssigned,
        Optional,
        Value
    }
    
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
    
    public async Task<string> GenerateModel(SqlTable table, Dictionary<string, CsModelSource> tablesMap)
    {
        const string propPrefix = "    ";
        const string propPrefix2 = "        ";
        List<KeyValuePair<string, SqlTableColumn>> ctorCols = table.Columns.Where(x => !x.Value.IsComputed).ToList();
        List<ColumnDefaultVal> ctorDeclCols = ctorCols.OrderBy(x => x.Value.OrdinalPosition).Select(x => ColumnDefaultValue(x.Key, x.Value)).ToList();
        List<ColumnDefaultVal> ctorAssignCols = ctorDeclCols.Where(x => x.Type is not ColumnDefaultValTypes.Value).ToList();

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

        string Ident(string str, int level = 1)
        {
            return level is 2 ? $"{propPrefix2}{str}" : $"{propPrefix}{str}";
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
        
        ColumnDefaultVal ColumnDefaultValue(string colName, SqlTableColumn column)
        {
            if (colName is "datecreated")
            {
                return new ColumnDefaultVal(column, "DateTime.Now", ColumnDefaultValTypes.Value);
            }
            
            return new ColumnDefaultVal(column, null, ColumnDefaultValTypes.UserAssigned);
        }
        
        string DumpCtorAssignPar(ColumnDefaultVal par, bool last)
        {
            return Ident($"{par.Column.Name.FirstCharToUpper()} = {(par.Type is ColumnDefaultValTypes.Value ? par.DefaultValue : par.Column.Name.FirstCharToLower())};", 2);
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

        string DumpCtorPar(ColumnDefaultVal par, bool last)
        {
            return $"{par.Column.SqlType.ToTypeSyntax(par.Column.IsNullable).ToFullString()} {par.Column.Name.FirstCharToLower()}{(par.Type is ColumnDefaultValTypes.Optional ? $" = {par.DefaultValue}" : string.Empty)}{(last ? string.Empty : ", ")}";
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

        string ctor = string.Empty;
   
        if (ctorCols.Count > 0)
        {
            ctor = $$"""
            
                public {{table.Name}}({{DumpCtorPars()}})
                {
            {{DumpCtorAssignments()}}
                }
            """;
        }
            
        string str = $$"""
        using System;
        using System.Collections.Generic;
        using System.ComponentModel.DataAnnotations;
        using System.ComponentModel.DataAnnotations.Schema;
        using Microsoft.EntityFrameworkCore;
        using Minfold.Annotations;           
                       
        namespace {{Source.ProjectNamespace}};
        public class {{table.Name}}
        {
        {{DumpProperties()}}
            public {{table.Name}}()
            {
        
            }
        {{ctor}}
        }
        """;
        
        return str;
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
            
            string str = await UpdateModel(source.Value, mappedTable, tablesToModelsMap);
            await File.WriteAllTextAsync(source.Value.ModelPath, str, token);
            synchronizedTables.TryAdd(mappedTable.Name.ToLowerInvariant(), true);
        });

        List<SqlTable> notSynchronized = []; 
        
        await Parallel.ForEachAsync(SqlSchema, async (source, token) =>
        {
            if (synchronizedTables.TryGetValue(source.Key, out bool synchronized) && synchronized)
            {
                return;
            }
            
            string str = await GenerateModel(source.Value, tablesToModelsMap);
            await File.WriteAllTextAsync($"{Source.ProjectPath}\\Dao\\Models\\{source.Value.Name.FirstCharToUpper()}.cs", str, token);
        });

        int z = 0;
    }
}