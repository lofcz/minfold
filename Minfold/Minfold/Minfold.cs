using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Text;
using Microsoft.Build.Locator;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.MSBuild;
using Microsoft.CodeAnalysis.Rename;
using Microsoft.CodeAnalysis.Text;

namespace Minfold;

public class Minfold
{
    private Dictionary<string, SqlTable> SqlSchema = [];
    private CsSource Source = new CsSource(string.Empty, string.Empty, [], [], string.Empty, string.Empty, []);
    private readonly Dictionary<string, CsModelSource> tablesToModelsMap = [];
    private readonly Dictionary<string, SqlTable> modelsToTablesMap = [];
    private readonly ConcurrentDictionary<string, string> daoPaths = new ConcurrentDictionary<string, string>();
    private MinfoldCfg cfg = new MinfoldCfg(false);
    
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

        CsSource source = new CsSource("", "", new ConcurrentDictionary<string, CsModelSource>(), new ConcurrentDictionary<string, string>(), prjName, folder, []);

        Parallel.ForEach(Directory.GetFiles($"{folder}\\Dao\\Dao", string.Empty, SearchOption.AllDirectories), daoPath =>
        {
            string fn = Path.GetFileNameWithoutExtension(daoPath);
            source.Daos.TryAdd(fn.ToLowerInvariant(), daoPath);
            
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
            SyntaxNode? daoRootNode = null;
            
            if (daoPaths.TryGetValue(fn, out string? daoPath) && !string.IsNullOrWhiteSpace(fn))
            {
                daoCode = await File.ReadAllTextAsync(daoPath, token);
                daoAst = CSharpSyntaxTree.ParseText(daoCode, cancellationToken: token);
                daoRootNode = await daoAst.GetRootAsync(token);
            }
            
            string modelCode = await File.ReadAllTextAsync(path, token);
            SyntaxTree modelAst = CSharpSyntaxTree.ParseText(modelCode, cancellationToken: token);
            SyntaxNode root = await modelAst.GetRootAsync(token);
            CsModelSource modelSource = new CsModelSource(fn, path, daoPath, modelCode, daoCode, modelAst, daoAst, "", MapModelToTable(fn.ToLowerInvariant()), root, daoRootNode, [], new ModelInfo());
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
            return $"{ColumnName(par.Column).FirstCharToUpper()} = {(par.Type is ColumnDefaultValTypes.Value ? par.DefaultValue : PrefixReservedKeyword(ColumnName(par.Column).FirstCharToLower()))};".Indent(2);
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

    public async Task<CsModelGenerateResult> GenerateModel(string className, SqlTable table, Dictionary<string, CsModelSource> tablesMap)
    {
        Dictionary<string, string> propertiesMap = [];
        CsPropertiesInfo properties = new CsPropertiesInfo();
        
        if (tablesMap.TryGetValue(table.Name.ToLowerInvariant(), out CsModelSource? model))
        {
            className = model.Name;
        }
        
        void DumpProperty(StringBuilder sb, SqlTableColumn column, bool last, bool first, bool nextAnyFks)
        {
            List<CsForeignKey> foreignKeys = [];
            
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
                
                sb.Append($$"""[ReferenceKey(typeof({{tblName}}), nameof({{memberPrefix}}{{colName}}), {{(fk.NotEnforced ? "false" : "true")}})]""".Indent());
                sb.Append(Environment.NewLine);
                foreignKeys.Add(new CsForeignKey(tblName, !fk.NotEnforced));
            }
            
            sb.Append($$"""public {{column.SqlType.ToTypeSyntax(column.IsNullable).ToFullString()}} {{column.Name.FirstCharToUpper()}} { get;{{(column.IsComputed ? "private" : string.Empty)}} set; }""".Indent());
            
            if (!nextAnyFks && !last && column.ForeignKeys.Count > 0)
            {
                sb.Append(Environment.NewLine);
            }

            propertiesMap.TryAdd(column.Name.ToLowerInvariant(), column.Name.FirstCharToUpper() ?? string.Empty);
            properties.Properties.TryAdd(column.Name.ToLowerInvariant(), new CsPropertyInfo(column.Name.FirstCharToUpper() ?? string.Empty, true, foreignKeys, null, new CsPropertyDecl(column.Name.FirstCharToUpper() ?? string.Empty, column.SqlType, column.IsNullable, column.ForeignKeys, null), null, column, true));
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
        
        return new CsModelGenerateResult(className, str, Source.ProjectNamespace, propertiesMap, properties);
    }

    public static string GenerateDaoGetWhereId(string modelName, string dbSetMappedTableName, string identityColumnId, string identityColumnType)
    {
        return $$"""
            public Task<{{modelName}}?> GetWhereId({{identityColumnType}} {{identityColumnId.FirstCharToLower()}})
            {
        	    return db.{{dbSetMappedTableName}}.FirstOrDefaultAsync(x => x.{{identityColumnId}} == {{identityColumnId.FirstCharToLower()}});
            }    
        """;
    }

    private static readonly HashSet<string> defaultUsings = ["System.Collections", "System.Data", "System.Text", "Dapper", "System.Threading.Tasks", "Microsoft.EntityFrameworkCore", "Microsoft.Extensions.Primitives"];
    
    string GenerateDaoCode(string daoName, string modelName, string modelNamespace, string dbSetMappedTableName, string? identityColumnId, string? identityColumnType, bool generateGetWhereId, Dictionary<string, string>? customUsings)
    {
        string usings = $"""
          using System.Collections;
          using System.Data;
          using System.Text;
          using Dapper;
          using System.Threading.Tasks;
          using Microsoft.EntityFrameworkCore;
          using Microsoft.Extensions.Primitives;
          using {Source.ProjectNamespace}.Dao.Base;
          using {modelNamespace};
          """;
        
        if (customUsings?.Count > 0)
        {
            HashSet<string> cpy =
            [
                ..defaultUsings,
                $"{Source.ProjectNamespace}.Dao.Base",
                $"{modelNamespace}"
            ];

            StringBuilder sb = new StringBuilder();
            bool first = true;

            foreach (KeyValuePair<string, string> x in customUsings.Where(x => !cpy.Contains(x.Key)))
            {
                if (first)
                {
                    sb.Append(Environment.NewLine);
                    first = false;
                }
                
                sb.Append($"using {x.Value};{Environment.NewLine}");
            }

            if (!first)
            {
                sb.Remove(sb.Length - Environment.NewLine.Length, Environment.NewLine.Length);
            }

            usings += sb.ToString();
        }
        
        
        return $$"""
        {{usings}}
        
        namespace {{modelNamespace}}.Dao;
        public class {{daoName}} : DaoBase<{{modelName}}>
        {
        {{(generateGetWhereId && identityColumnId is not null && identityColumnType is not null ? GenerateDaoGetWhereId(modelName, dbSetMappedTableName, identityColumnId, identityColumnType) : string.Empty)}}
        }
        """;
    }

    public async Task<ClassRewriteResult> UpdateOrCreateDao(CsModelSource tree, SqlTable table, Dictionary<string, CsModelSource> tablesMap, CsPropertiesInfo? properties)
    {
        string daoName = $"{tree.Name}Dao";
        string modelName = tree.Name;
        
        if (tablesMap.TryGetValue(table.Name.ToLowerInvariant(), out CsModelSource? model))
        {
            daoName = $"{model.Name}Dao";
            modelName = model.Name;
        }

        if (modelName.ToLowerInvariant() is "userrole")
        {
            int z = 0;
        }
        
        string path = $"{Source.ProjectPath}\\Dao\\Dao\\{daoName}.cs";

        string? identColName = null;
        string? identColType = null;
            
        KeyValuePair<string, SqlTableColumn> pkCol = table.Columns.FirstOrDefault(x => x.Value.IsPrimaryKey);
        Dictionary<string, string>? customUsings = null;

        if (pkCol.Value is not null)
        {
            identColName = pkCol.Value.Name;

            if (pkCol.Value.SqlType is not SqlDbTypeExt.CsIdentifier)
            {
                identColType = pkCol.Value.SqlType.ToTypeSyntax(false).ToFullString();
            }

            if (properties?.Properties.TryGetValue(identColName.ToLowerInvariant(), out CsPropertyInfo? propInfo) ?? false)
            {
                identColName = propInfo.Name;
                    
                if (propInfo.TypeAlias is not null)
                {
                    identColType = propInfo.TypeAlias.Symbol;
                    customUsings = propInfo.TypeAlias.Usings;
                }
            }
        }

        string dbSet = table.Name;

        if (Source.DbSetMap.TryGetValue(modelName.ToLowerInvariant(), out CsDbSetDecl? dbSetName))
        {
            dbSet = dbSetName.SetName;
        }
        else
        {
            Source.DbSetMap.TryAdd(modelName.ToLowerInvariant(), new CsDbSetDecl(modelName, table.Name, null));
        }

        string modelNamespace = tree.ModelInfo.Namespace ?? $"{Source.ProjectNamespace}.Dao";
        
        if (tree.DaoPath is null || tree.DaoAst is null || tree.DaoRootNode is null)
        {
            string daoText = GenerateDaoCode(daoName, modelName, modelNamespace, dbSet, identColName, identColType, !cfg.UniformPk, customUsings);
            await File.WriteAllTextAsync(path, daoText);
            synchronizedDaoFiles.TryAdd(daoName.ToLowerInvariant(), true);
            return new ClassRewriteResult(false, null);
        }
        
        DaoClassRewriter daoClassVisitor = new DaoClassRewriter(daoName, modelName, table, tablesMap, tree, dbSet, identColName, identColType, !cfg.UniformPk, customUsings);
        CompilationUnitSyntax newNode = (CompilationUnitSyntax)daoClassVisitor.Visit(tree.DaoRootNode);

        List<UsingDirectiveSyntax> addUsings = [];
        HashSet<string> declaredUsings = [];

        foreach (UsingDirectiveSyntax usingDir in newNode.Usings)
        {
            declaredUsings.Add(usingDir.NamespaceOrType.ToFullString().Trim());
        }

        addUsings.AddRange(from defaultUsing in defaultUsings where !declaredUsings.Contains(defaultUsing) select SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName(defaultUsing)));

        if (!declaredUsings.Contains($"{Source.ProjectNamespace}.Dao.Base"))
        {
            addUsings.Add(SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName($"{Source.ProjectNamespace}.Dao.Base")));
        }
        
        if (!declaredUsings.Contains(modelNamespace))
        {
            addUsings.Add(SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName(modelNamespace)));
        }

        if (customUsings is not null)
        {
            addUsings.AddRange(from x in customUsings where !declaredUsings.Contains(x.Value) select SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName(x.Value)));
        }

        if (addUsings.Count > 0)
        {
            newNode = newNode.WithUsings(newNode.Usings.AddRange(addUsings));
        }
        
        synchronizedDaoFiles.TryAdd(daoName.ToLowerInvariant(), true);

        return new ClassRewriteResult(daoClassVisitor.ClassRewritten, newNode.NormalizeWhitespace().ToFullString());
    }
    
    public async Task<ModelClassRewriteResult> UpdateModel(CsModelSource tree, SqlTable table, Dictionary<string, CsModelSource> tablesMap)
    {
        string modelName = table.Name;
        
        if (tablesMap.TryGetValue(table.Name.ToLowerInvariant(), out CsModelSource? model))
        {
            modelName = model.Name;
        }
        
        FileScopedNamespaceDeclarationSyntax? namespaceNode = (FileScopedNamespaceDeclarationSyntax?)tree.ModelRootNode.ChildNodes().FirstOrDefault(x => x is FileScopedNamespaceDeclarationSyntax fileNamespaceDecl);
        
        ModelClassRewriter modelClassVisitor = new ModelClassRewriter(modelName, table, tablesMap, tree, namespaceNode is null, (CompilationUnitSyntax)tree.ModelRootNode);
        CompilationUnitSyntax newNode = (CompilationUnitSyntax)modelClassVisitor.Visit(tree.ModelRootNode);

        string? modelNamespace = namespaceNode?.Name.ToFullString() ?? modelClassVisitor.Namespace;
        tree.ModelInfo.Namespace = modelNamespace;
        
        bool usingAnnotations = newNode.Usings.Any(x => x.NamespaceOrType is QualifiedNameSyntax { Left: IdentifierNameSyntax { Identifier.ValueText: "Minfold" }, Right.Identifier.ValueText: "Annotations" });

        if (!usingAnnotations)
        {
            QualifiedNameSyntax name = SyntaxFactory.QualifiedName(SyntaxFactory.IdentifierName("Minfold"), SyntaxFactory.IdentifierName("Annotations"));
            newNode = newNode.AddUsings(SyntaxFactory.UsingDirective(name).NormalizeWhitespace());   
        }

        if (!Source.DbSetMap.TryGetValue(modelName.ToLowerInvariant(), out CsDbSetDecl? decl))
        {
            Source.DbSetMap.TryAdd(modelName.ToLowerInvariant(), new CsDbSetDecl(modelName, table.Name, null));
        }
        
        return new ModelClassRewriteResult(modelClassVisitor.ClassRewritten, newNode.NormalizeWhitespace().ToFullString(), modelClassVisitor.PropertiesMap);
    }

    private void MapTables()
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

    private async Task MapDbSet()
    {
        string path = $"{Source.ProjectPath}\\Dao\\Db.cs";
        
        if (!File.Exists(path))
        {
            return;
        }

        string content = await File.ReadAllTextAsync(path);
        
        SyntaxTree dbsetAst = CSharpSyntaxTree.ParseText(content);
        SyntaxNode root = await dbsetAst.GetRootAsync();
        
        DbSetMapper dbSetMapper = new DbSetMapper("Db", Source.DbSetMap);
        dbSetMapper.Visit(root);
    }

    private async Task UpdateDbSet()
    {
        string path = $"{Source.ProjectPath}\\Dao\\Db.cs";

        if (!File.Exists(path))
        {
            return;
        }

        string dbSetText = await File.ReadAllTextAsync(path);
        
        SyntaxTree dbsetAst = CSharpSyntaxTree.ParseText(dbSetText);
        SyntaxNode root = await dbsetAst.GetRootAsync();

        List<CsDbSetDecl> decls = [];
        
        foreach (KeyValuePair<string, CsDbSetDecl> pair in Source.DbSetMap)
        {
            if (!synchronizedModelFiles.TryGetValue(pair.Key, out bool synchronized) || !synchronized)
            {
                continue;
            }

            decls.Add(pair.Value);
        }
        
        DbSetClassRewritter dbSetRewritter = new DbSetClassRewritter("Db", decls, modelsToTablesMap, modelProperties);
        CompilationUnitSyntax newNode = (CompilationUnitSyntax)dbSetRewritter.Visit(root);

        await File.WriteAllTextAsync(path, newNode.NormalizeWhitespace().ToFullString());
    }

    private void InferCapabilites()
    {
        bool uniformPks = File.Exists($"{Source.ProjectPath}\\Dao\\Base\\IEntity.cs");

        cfg = new MinfoldCfg(uniformPks);
    }

    private ConcurrentDictionary<string, bool> synchronizedTables = [];
    private ConcurrentDictionary<string, bool> synchronizedModelFiles = [];
    private ConcurrentDictionary<string, bool> synchronizedDaoFiles = [];
    private ConcurrentDictionary<string, CsPropertiesInfo> modelProperties = [];
    
    public async Task Synchronize(string sqlConn, string dbName, string codePath)
    {
        synchronizedTables = [];
        synchronizedModelFiles = [];
        synchronizedDaoFiles = [];
        modelProperties = [];
        
        await AnalyzeSqlSchema(sqlConn, dbName);
        await LoadCsCode(codePath);
        await MapDbSet();
        MapTables();
        InferCapabilites();

        await Parallel.ForEachAsync(Source.Models, async (source, token) =>
        {
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
            
            ModelClassRewriteResult modelUpdateResult = await UpdateModel(source.Value, mappedTable, tablesToModelsMap);

            if (modelUpdateResult.Properties is not null)
            {
                modelProperties.TryAdd(source.Key, modelUpdateResult.Properties);   
            }
            
            if (modelUpdateResult.Rewritten)
            {
                await File.WriteAllTextAsync(source.Value.ModelPath, modelUpdateResult.Text, token);
                synchronizedTables.TryAdd(mappedTable.Name.ToLowerInvariant(), true);
                synchronizedModelFiles.TryAdd(source.Key, true);
            }
            
            ClassRewriteResult daoUpdateResult = await UpdateOrCreateDao(source.Value, mappedTable, tablesToModelsMap, modelUpdateResult.Properties);
            
            if (daoUpdateResult.Rewritten && source.Value.DaoPath is not null)
            {
                await File.WriteAllTextAsync(source.Value.DaoPath, daoUpdateResult.Text, token);
            }
        });
        
        await Parallel.ForEachAsync(SqlSchema, async (source, token) =>
        {
            if (synchronizedTables.TryGetValue(source.Key, out bool synchronized) && synchronized)
            {
                return;
            }

            string className = source.Value.Name.FirstCharToUpper() ?? string.Empty;
            
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

            string path = $"{Source.ProjectPath}\\Dao\\Models\\{className}.cs";
            
            CsModelGenerateResult modelGen = await GenerateModel(className, source.Value, tablesToModelsMap);
            modelProperties.TryAdd(source.Key, modelGen.PropertiesInfo);   
            
            await File.WriteAllTextAsync(path, modelGen.Code, token);
            synchronizedTables.TryAdd(source.Key, true);
            synchronizedModelFiles.TryAdd(source.Key, true);

            SyntaxTree modelAst = CSharpSyntaxTree.ParseText(modelGen.Code, cancellationToken: token);
            SyntaxNode root = await modelAst.GetRootAsync(token);

            ClassDeclarationSyntax? classNode = (ClassDeclarationSyntax?)root.ChildNodes().FirstOrDefault(x => x is FileScopedNamespaceDeclarationSyntax)?.ChildNodes().FirstOrDefault(x => x is ClassDeclarationSyntax);

            if (classNode is not null)
            {
                SyntaxTree? daoAst = null;
                SyntaxNode? daoRootNode = null;
                string? daoCode = null;
            
                if (daoPaths.TryGetValue(className, out string? daoPath))
                {
                    daoCode = await File.ReadAllTextAsync(daoPath, token);
                    daoAst = CSharpSyntaxTree.ParseText(daoCode, cancellationToken: token);
                    daoRootNode = await daoAst.GetRootAsync(token);
                }
                
                CsModelSource model = new CsModelSource(className, path, daoPath, modelGen.Code, daoCode, modelAst, daoAst, "", source.Value, root, daoRootNode, modelGen.Columns, new ModelInfo { Namespace = modelGen.Namespace });
                ClassRewriteResult daoUpdateResult = await UpdateOrCreateDao(model, source.Value, tablesToModelsMap, ModelClassRewriter.Properties(classNode, source.Value, (CompilationUnitSyntax)root));
            
                if (daoUpdateResult.Rewritten && daoPath is not null)
                {
                    await File.WriteAllTextAsync(daoPath, daoUpdateResult.Text, token);
                }   
            }
        });

        Parallel.ForEach(Source.Models, pair =>
        {
            if (!synchronizedModelFiles.TryGetValue(pair.Key, out bool synchronizedModel) || !synchronizedModel)
            {
                if (File.Exists(pair.Value.ModelPath))
                {
                    File.Delete(pair.Value.ModelPath);   
                }
            }
        });
        
        Parallel.ForEach(Source.Daos, pair =>
        {
            if (!synchronizedDaoFiles.TryGetValue(pair.Key, out bool synchronizedDao) || !synchronizedDao)
            {
                if (File.Exists(pair.Value) && !ProtectedDaos.Contains(pair.Key))
                {
                    File.Delete(pair.Value);   
                }
            }
        });

        await UpdateDbSet();
    }

    private static readonly HashSet<string> ProtectedDaos = ["batchdao", "sqldao", "daobase"];
}