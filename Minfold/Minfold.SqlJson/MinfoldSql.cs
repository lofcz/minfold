using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
namespace Minfold.SqlJson;

public static class MinfoldSql
{
    public static async Task<MinfoldSqlResult> Map(string connString, string database, string sqlQuery)
    {
        using StringReader rdr = new StringReader(sqlQuery);
        TSql160Parser parser = new TSql160Parser(true, SqlEngineType.All);
        TSqlFragment tree = parser.Parse(rdr, out IList<ParseError>? errors);

        if (errors?.Count > 0)
        {
            return new MinfoldSqlResult { ResultType = MinfoldSqlResultTypes.SqlSyntaxInvalid, ParseErrors = errors };
        }
        
        MyVisitor checker = new MyVisitor();
        tree.Accept(checker);

        HashSet<string> deps = checker.Root.GatherDependencies();
        SqlService service = new SqlService(connString);
        
        ResultOrException<Dictionary<string, SqlTable>> test = deps.Count > 0 ? await service.GetSchema(database, deps.ToList()) : new ResultOrException<Dictionary<string, SqlTable>>(new Dictionary<string, SqlTable>(), null);

        if (test.Exception is not null || test.Result is null)
        {
            return new MinfoldSqlResult { ResultType = MinfoldSqlResultTypes.DatabaseConnectionFailed, Exception = test.Exception };
        }

        MappedModel model = checker.Root.Compile(test.Result, 0);
        DumpedModel dump = model.Dump();
        string genCode = dump.Flatten();

        return new MinfoldSqlResult { ResultType = MinfoldSqlResultTypes.Ok, GeneratedCode = genCode };
    }
}

class ScopeIdentifier
{
    public string Ident { get; set; }
    public string? Table { get; set; }
    public Scope? Scope { get; set; }
    public bool Nullable { get; set; }

    public ScopeIdentifier(string ident, string table, bool nullable)
    {
        Ident = ident;
        Table = table;
        Nullable = nullable;
    }
    
    public ScopeIdentifier(string ident, Scope scope, bool nullable)
    {
        Ident = ident;
        Scope = scope;
        Nullable = nullable;
    }
}

class SelectColumn
{
    public SelectColumnTypes OutputType => TransformedType ?? Type;
    public SelectColumnTypes? TransformedType { get; set; }
    public SelectColumnTypes Type { get; set; }
    public string? ColumnOutputName => Alias ?? ColumnName;
    public string? Alias { get; set; }
    public string? ColumnName { get; set; }
    public string? ColumnSource { get; set; }
    public Scope? Scope { get; set; }
    
    public SelectColumn(SelectColumnTypes type, string? alias, string? columnName, string? columnSource)
    {
        Type = type;
        Alias = alias;
        ColumnName = columnName;
        ColumnSource = columnSource;
    }

    public SelectColumn(Scope scope, string? alias)
    {
        Type = SelectColumnTypes.Query;
        Scope = scope;
        Alias = alias;
    }
}

public enum SelectColumnTypes
{
    Unknown,
    TableColumn,
    Integer,
    Number,
    String,
    Query,
    Bool
}

public static class Extensions
{
    public static SelectColumnTypes ToCsType(this SqlDataTypeReference dataType)
    {
        return dataType.SqlDataTypeOption.ToCsType();
    }

    public static SelectColumnTypes ToCsType(this SqlDataTypeOption type)
    {
        return type switch
        {
            SqlDataTypeOption.Bit => SelectColumnTypes.Bool,
            SqlDataTypeOption.NVarChar or SqlDataTypeOption.VarChar => SelectColumnTypes.String,
            SqlDataTypeOption.Float => SelectColumnTypes.Number,
            SqlDataTypeOption.Int => SelectColumnTypes.Integer,
            _ => SelectColumnTypes.Unknown
        };
    }
}

class Scope
{
    public List<ScopeIdentifier> Identifiers { get; set; } = [];
    public Scope? Parent { get; set; }
    public QuerySpecification? Select { get; set; }
    public List<SelectColumn> SelectColumns { get; set; } = [];
    public JsonForClauseOptions JsonOptions { get; set; }

    public ScopeIdentifier? GetIdentifier(string ident)
    {
        return Identifiers.FirstOrDefault(x => string.Equals(x.Ident, ident, StringComparison.InvariantCultureIgnoreCase)) ?? Parent?.GetIdentifier(ident);
    }

    public Scope(Scope? parent, QuerySpecification? qs)
    {
        Parent = parent;
        Select = qs;
    }

    public HashSet<string> GatherDependencies()
    {
        if (Identifiers.Count is 0 && SelectColumns.Count is 0)
        {
            return [];
        }

        HashSet<string> deps = [];

        foreach (ScopeIdentifier ident in Identifiers)
        {
            deps.Add(ident.Ident);
        }

        foreach (SelectColumn column in SelectColumns)
        {
            if (column.Scope is not null)
            {
                HashSet<string> childDeps = column.Scope.GatherDependencies();

                foreach (string dep in childDeps)
                {
                    deps.Add(dep);
                }
            }
        }

        return deps;
    }

    private ScopeIdentifier? GetSource(string ident)
    {
        return Identifiers.FirstOrDefault(x => string.Equals(x.Ident, ident, StringComparison.InvariantCultureIgnoreCase)) ?? Parent?.GetSource(ident);
    }

    public MappedModel Compile(Dictionary<string, SqlTable> schema, int modelIndex)
    {
        int unkIndex = 0;
        MappedModel model = new MappedModel($"Model{modelIndex}", JsonOptions);

        string GetUnkIdent()
        {
            string str = $"NoColumnName{unkIndex}";
            unkIndex++;
            return str;
        }

        void AddProperty(SqlDbTypeExt type, bool nullable, string? name, MappedModelPropertyTypeFlags flags)
        {
            MappedModelProperty property = new MappedModelProperty(type, nullable, name?.FirstCharToUpper() ?? GetUnkIdent(), flags);
            model.Properties.Add(property);
        }
        
        void AddPropertyWithModel(SqlDbTypeExt type, bool nullable, string? name, MappedModel mdl, MappedModelPropertyTypeFlags flags)
        {
            MappedModelProperty property = new MappedModelProperty(type, nullable, name?.FirstCharToUpper() ?? GetUnkIdent(), mdl, flags);
            model.Properties.Add(property);
        }

        int GetModelIndex()
        {
            modelIndex++;
            return modelIndex;
        }

        foreach (SelectColumn column in SelectColumns)
        {
            if (column.OutputType is SelectColumnTypes.Query)
            {
                if (column.Scope is not null)
                {
                    MappedModel subModel = column.Scope.Compile(schema, GetModelIndex());
                    bool mapAsJson = subModel.JsonOptions.HasFlag(JsonForClauseOptions.Path) || subModel.JsonOptions.HasFlag(JsonForClauseOptions.Auto);
                    bool mapAsList = !subModel.JsonOptions.HasFlag(JsonForClauseOptions.WithoutArrayWrapper);

                    MappedModelPropertyTypeFlags flags = MappedModelPropertyTypeFlags.None;
                    
                    if (mapAsList)
                    {
                        flags |= MappedModelPropertyTypeFlags.List;
                    }

                    if (mapAsJson)
                    {
                        if (JsonOptions.HasFlag(JsonForClauseOptions.Path) || JsonOptions.HasFlag(JsonForClauseOptions.Auto))
                        {
                            flags |= MappedModelPropertyTypeFlags.NestedJson;
                        }
                        else
                        {
                            flags |= MappedModelPropertyTypeFlags.Json;
                        }   
                    }
                    
                    AddPropertyWithModel(SqlDbTypeExt.CsIdentifier, true, column.ColumnOutputName, subModel, flags);
                }
            }
            else if (column.OutputType is SelectColumnTypes.TableColumn)
            {
                ScopeIdentifier? identifier = Identifiers.Count > 0 ? Identifiers[0] : null;

                if (column.ColumnSource is not null)
                {
                    identifier = identifier is not null ? GetSource(identifier.Ident) : null;
                }
                
                if (identifier?.Table is not null && schema.TryGetValue(identifier.Table, out SqlTable? sqlTable))
                {
                    if (column.ColumnName is not null && sqlTable.Columns.TryGetValue(column.ColumnName, out SqlTableColumn? colDef))
                    {
                        AddProperty(colDef.SqlType, colDef.IsNullable || identifier.Nullable, column.ColumnOutputName, MappedModelPropertyTypeFlags.None);
                    }
                }
                else if (identifier?.Scope is not null)
                {
                    MappedModel fromModel = identifier.Scope.Compile(schema, modelIndex);
                    MappedModelProperty? fromProp = fromModel.Properties.FirstOrDefault(x => string.Equals(x.Name, column.ColumnName, StringComparison.InvariantCultureIgnoreCase));

                    if (fromProp is not null)
                    {
                        AddProperty(fromProp.Type, fromProp.Nullable, column.ColumnOutputName, fromProp.Flags);
                    }
                    
                    int z = 0;
                }
            }
            else if (column.OutputType is SelectColumnTypes.Integer)
            {
                AddProperty(SqlDbTypeExt.Int, false, column.ColumnOutputName, MappedModelPropertyTypeFlags.None);
            }
            else if (column.OutputType is SelectColumnTypes.String)
            {
                AddProperty(SqlDbTypeExt.NVarChar, false, column.ColumnOutputName, MappedModelPropertyTypeFlags.None);
            }
            else if (column.OutputType is SelectColumnTypes.Bool)
            {
                AddProperty(SqlDbTypeExt.Bit, false, column.ColumnOutputName, MappedModelPropertyTypeFlags.None);
            }
            else if (column.OutputType is SelectColumnTypes.Number)
            {
                AddProperty(SqlDbTypeExt.Float, false, column.ColumnOutputName, MappedModelPropertyTypeFlags.None);
            }
        }
        
        return model;
    }
}

[Flags]
enum MappedModelPropertyTypeFlags
{
    None = 1 << 0,
    List = 1 << 1,
    Json = 1 << 2,
    NestedJson = 1 << 3
}

class MappedModelProperty
{
    public SqlDbTypeExt Type { get; set; }
    public bool Nullable { get; set; }
    public string Name { get; set; }
    public MappedModelPropertyTypeFlags Flags { get; set; }
    public MappedModel? Model { get; set; }

    public MappedModelProperty(SqlDbTypeExt type, bool nullable, string name, MappedModelPropertyTypeFlags flags)
    {
        Type = type;
        Nullable = nullable;
        Name = name;
        Flags = flags;
    }
    
    public MappedModelProperty(SqlDbTypeExt type, bool nullable, string name, MappedModel model, MappedModelPropertyTypeFlags flags)
    {
        Type = type;
        Model = model;
        Nullable = nullable;
        Name = name;
        Flags = flags;
    }
}

class DumpedModel
{
    public string Name { get; set; }
    public string Code { get; set; }
    public List<DumpedModel> ChildModels { get; set; } = [];

    public DumpedModel(string name)
    {
        Name = name;
    }
    
    public string Flatten()
    {
        if (ChildModels.Count is 0)
        {
            return Code;
        }
        
        StringBuilder sb = new StringBuilder();

        sb.AppendLine(Code);

        foreach (string childCode in ChildModels.Select(childModel => childModel.Flatten()))
        {
            sb.AppendLine(childCode);
        }

        return sb.ToString();
    }
}

class MappedModel
{
    public string Name { get; set; }
    public List<MappedModelProperty> Properties { get; set; } = [];
    public JsonForClauseOptions JsonOptions { get; set; }

    public MappedModel(string name, JsonForClauseOptions jsonOptions)
    {
        Name = name;
        JsonOptions = jsonOptions;
    }

    public string StringHash()
    {
        StringBuilder sb = new StringBuilder();

        foreach (MappedModelProperty prop in Properties)
        {
            sb.Append(prop.Name);
            sb.Append(',');
            sb.Append(prop.Nullable ? 1 : 0);
            sb.Append(',');
            sb.Append((int)prop.Type);
        }

        return sb.ToString();
    }

    public DumpedModel Dump(bool decorateForJson = false)
    {
        DumpedModel dumpedModel = new DumpedModel(Name);
        StringBuilder sb = new StringBuilder();

        void DumpProperty(MappedModelProperty prop)
        {
            sb.AppendLine($"public {prop.Type.ToTypeSyntax(prop.Nullable).ToFullString()} {prop.Name} {{ get; set; }}".Indent());
        }
        
        void DumpChildProperty(MappedModelProperty prop, DumpedModel childModel)
        {
            sb.Append("public ".Indent());

            if (prop.Flags.HasFlag(MappedModelPropertyTypeFlags.Json))
            {
                sb.Append("Json<");
            }
            
            if (prop.Flags.HasFlag(MappedModelPropertyTypeFlags.List))
            {
                sb.Append("List<");
            }
            
            sb.Append(childModel.Name);
            
            if (prop.Flags.HasFlag(MappedModelPropertyTypeFlags.List))
            {
                sb.Append(">?");
            }
            
            if (prop.Flags.HasFlag(MappedModelPropertyTypeFlags.Json))
            {
                sb.Append(">?");
            }

            if (prop.Flags.HasFlag(MappedModelPropertyTypeFlags.Json) || prop.Flags.HasFlag(MappedModelPropertyTypeFlags.List))
            {
                sb.Append(' ');
            }
            
            sb.Append(prop.Name);
            sb.Append(" { get; set; }");
            sb.AppendLine();
        }

        if (decorateForJson)
        {
            sb.AppendLine("[SqlJson]");
        }
        
        sb.AppendLine($"public class {Name}");
        sb.AppendLine("{");
        
        foreach (MappedModelProperty prop in Properties)
        {
            if (prop.Model is not null)
            {
                DumpedModel dumpedProp = prop.Model.Dump(prop.Flags.HasFlag(MappedModelPropertyTypeFlags.Json));
                dumpedModel.ChildModels.Add(dumpedProp);
                DumpChildProperty(prop, dumpedProp);
            }
            else
            {
                DumpProperty(prop);
            }
        }

        sb.AppendLine("}");
        dumpedModel.Code = sb.ToString();
        return dumpedModel;
    }
}

class MyVisitor : TSqlFragmentVisitor
{
    public Scope Root { get; set; }

    Scope SolveSelect(QuerySpecification qs, Scope parent)
    {
        Scope localScope = new Scope(parent, qs);
        
        if (qs.ForClause is JsonForClause jsonForClause)
        {
            foreach (JsonForClauseOption option in jsonForClause.Options)
            {
                localScope.JsonOptions |= option.OptionKind;
            }
        }

        if (qs.FromClause?.TableReferences is not null)
        {
            foreach (TableReference fromTbl in qs.FromClause.TableReferences)
            {
                string? tbl, alias;

                switch (fromTbl)
                {
                    case NamedTableReference namedFrom:
                        tbl = namedFrom.SchemaObject.BaseIdentifier.Value;
                        alias = namedFrom.Alias?.Value;

                        localScope.Identifiers.Add(new ScopeIdentifier(alias ?? tbl, tbl, false));
                        break;
                    case QueryDerivedTable queryDerivedTable:
                    {
                        alias = queryDerivedTable.Alias?.Value ?? string.Empty;

                        if (queryDerivedTable.QueryExpression is QuerySpecification fromQs)
                        {
                            localScope.Identifiers.Add(new ScopeIdentifier(alias, SolveSelect(fromQs, localScope), false));
                        }

                        break;
                    }
                    case QualifiedJoin qualifiedJoin:
                    {
                        bool nullable = qualifiedJoin.QualifiedJoinType is QualifiedJoinType.LeftOuter or QualifiedJoinType.FullOuter;
                
                        if (qualifiedJoin.SecondTableReference is NamedTableReference namedJoin)
                        {
                            alias = namedJoin.Alias.Value ?? string.Empty;   
                            localScope.Identifiers.Add(new ScopeIdentifier(alias, namedJoin.SchemaObject.BaseIdentifier.Value, nullable));
                        }

                        break;
                    }
                }
            }
        }
        
        SelectColumn? SolveSelectScalarCol(ScalarExpression scalarExpression, SelectScalarExpression? selScalar)
        {
            switch (scalarExpression)
            {
                case ColumnReferenceExpression colRef when colRef.MultiPartIdentifier.Identifiers.Count is 1:
                    return new SelectColumn(SelectColumnTypes.TableColumn, selScalar?.ColumnName?.Value, colRef.MultiPartIdentifier.Identifiers[0].Value, null);
                case ColumnReferenceExpression colRef when colRef.MultiPartIdentifier.Identifiers.Count is 2:
                    return new SelectColumn(SelectColumnTypes.TableColumn, selScalar?.ColumnName?.Value, colRef.MultiPartIdentifier.Identifiers[1].Value, colRef.MultiPartIdentifier.Identifiers[0].Value);
                case ScalarSubquery { QueryExpression: QuerySpecification subquerySpec }:
                    return new SelectColumn(SolveSelect(subquerySpec, localScope), selScalar?.ColumnName?.Value);
                case IntegerLiteral integerLiteral:
                    return new SelectColumn(SelectColumnTypes.Integer, selScalar?.ColumnName?.Value, null, null);
                case StringLiteral stringLiteral:
                    return new SelectColumn(SelectColumnTypes.String, selScalar?.ColumnName?.Value, null, null);
                case NumericLiteral numericLiteral:
                    return new SelectColumn(SelectColumnTypes.Number, selScalar?.ColumnName?.Value, null, null);
                case CastCall castCall:
                {
                    SelectColumn? castQuery = SolveSelectCol(castCall.Parameter);

                    if (castQuery is not null)
                    {
                        if (castCall.DataType is SqlDataTypeReference dataTypeReference)
                        {
                            castQuery.TransformedType = dataTypeReference.SqlDataTypeOption.ToCsType();
                        }
                    }
                
                    return castQuery;
                }
                default:
                    return null;
            }
        }
        
        SelectColumn? SolveSelectCol(TSqlFragment selCol)
        {
            switch (selCol)
            {
                case SelectStarExpression selStar:
                    break;
                case ScalarExpression scalarExpression:
                    return SolveSelectScalarCol(scalarExpression, null);
                case SelectScalarExpression selScalar:
                    return SolveSelectScalarCol(selScalar.Expression, selScalar);
            }

            return null;
        }
        
        foreach (SelectElement selCol in qs.SelectElements)
        {
            SelectColumn? column = SolveSelectCol(selCol);

            if (column is not null)
            {
                localScope.SelectColumns.Add(column);
            }
        }

        return localScope;
    }
    
    public override void Visit(TSqlStatement node)
    {
        if (node is SelectStatement select)
        {
            Scope root = new Scope(null, null);
            
            if (select.QueryExpression is QuerySpecification qs)
            {
                root = SolveSelect(qs, root);
            }
            
            Root = root;
            
            int z = 0;
        }

        base.Visit(node);
    }
}