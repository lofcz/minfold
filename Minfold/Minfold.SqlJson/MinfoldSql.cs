using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
namespace Minfold.SqlJson;

public class MinfoldSqlOptions
{
    public static readonly MinfoldSqlOptions Default = new MinfoldSqlOptions();
    
    /// <summary>
    /// If true, models with members with the same already declared will be produced without an error, e.g:
    /// select 1 as c1, 2 as c1 would produce a model with two integer columns named C1
    /// </summary>
    public bool IgnoreAmbiguities { get; set; }
}

public static class MinfoldSql
{
    public static async Task<MinfoldSqlResult> Map(string connString, string database, string sqlQuery, MinfoldSqlOptions? options = null)
    {
        options ??= MinfoldSqlOptions.Default;
        
        using StringReader rdr = new StringReader(sqlQuery);
        TSql160Parser parser = new TSql160Parser(true, SqlEngineType.All);
        TSqlFragment tree = parser.Parse(rdr, out IList<ParseError>? errors);

        if (errors?.Count > 0)
        {
            return new MinfoldSqlResult { ResultType = MinfoldSqlResultTypes.SqlSyntaxInvalid, ParseErrors = errors };
        }
        
        MyVisitor checker = new MyVisitor();
        tree.Accept(checker);
        
        SqlService service = new SqlService(connString);

        async Task MapQuery(Query qry)
        {
            ResultOrException<List<SqlResultSetColumn>> selectResult = await service.DescribeSelect(database, qry.TransformedSql ?? qry.RawSql);

            if (selectResult.Exception is null)
            {
                qry.SelectColumns = selectResult.Result;
            }

            foreach (Query child in qry.Childs)
            {
                await MapQuery(child);
            }
        }
        
        foreach (Query qry in checker.Query.Childs)
        {
            await MapQuery(qry);
        }
        
        DumpedQuery dumpedQuery = checker.Query.Dump();
        
        if (!options.IgnoreAmbiguities)
        {
            List<string> ambiguities = dumpedQuery.FlattenAmbiguities();
       
            if (ambiguities.Count > 0)
            {
                return await RunStaticAnalysis();
            }
        }
        
        string describeCode = dumpedQuery.Flatten();
        return new MinfoldSqlResult { ResultType = MinfoldSqlResultTypes.Ok, GeneratedCode = describeCode };

        async Task<MinfoldSqlResult> RunStaticAnalysis()
        {
            HashSet<string> deps = checker.Root.GatherDependencies();
            ResultOrException<ConcurrentDictionary<string, SqlTable>> test = deps.Count > 0 ? await service.GetSchema(database, deps.ToList()) : new ResultOrException<ConcurrentDictionary<string, SqlTable>>(new ConcurrentDictionary<string, SqlTable>(), null);

            if (test.Exception is not null || test.Result is null)
            {
                return new MinfoldSqlResult { ResultType = MinfoldSqlResultTypes.DatabaseConnectionFailed, Exception = test.Exception };
            }

            MappedModel model = checker.Root.Compile(test.Result, 0);

            if (!options.IgnoreAmbiguities)
            {
                List<MappedModelAmbiguities> ambiguities = model.GetMappingAmbiguities();
       
                if (ambiguities.Count > 0)
                {
                    MappedModelAmbiguitiesAccumulator accu = new MappedModelAmbiguitiesAccumulator
                    {
                        Ambiguities = ambiguities,
                        Input = sqlQuery
                    };
                
                    return new MinfoldSqlResult { ResultType = MinfoldSqlResultTypes.MappingAmbiguities, MappingAmbiguities = accu };
                }
            }
        
            DumpedModel dump = model.Dump();
            string genCode = dump.Flatten();

            return new MinfoldSqlResult { ResultType = MinfoldSqlResultTypes.Ok, GeneratedCode = genCode };   
        }
    }
}

class ScopeIdentifier
{
    public string? Ident { get; set; }
    public string? Table { get; set; }
    public Scope? Scope { get; set; }
    public bool Nullable { get; set; }

    public ScopeIdentifier(string? ident, string table, bool nullable)
    {
        Ident = ident;
        Table = table;
        Nullable = nullable;
    }
    
    public ScopeIdentifier(string? ident, Scope scope, bool nullable)
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
    public TSqlFragment Fragment { get; set; }
    public string? Value { get; set; }
    public SqlDbTypeExt? TransformedLiteralType { get; set; }
    public SqlDbTypeExt? LiteralType { get; set; }
    public SqlDbTypeExt? OutputLiteralType => TransformedLiteralType ?? LiteralType;
    public List<SelectColumn>? OneOfColumns { get; set; }
    public bool Nullable { get; set; }
    
    public SelectColumn(SelectColumnTypes type, string? alias, string? columnName, string? columnSource, TSqlFragment fragment, Scope scope)
    {
        Type = type;
        Alias = alias;
        ColumnName = columnName;
        ColumnSource = columnSource;
        Fragment = fragment;
        Scope = scope;
    }
    
    public SelectColumn(SelectColumnTypes type, string? alias, string? columnName, string? columnSource, string? value, TSqlFragment fragment)
    {
        Type = type;
        Alias = alias;
        ColumnName = columnName;
        ColumnSource = columnSource;
        Value = value;
        Fragment = fragment;
    }

    public SelectColumn(SelectColumnTypes type, Scope scope, string? alias, TSqlFragment fragment)
    {
        Type = type;
        Scope = scope;
        Alias = alias;
        Fragment = fragment;
    }
    
    public SelectColumn(SelectColumnTypes type, string? alias, TSqlFragment fragment, List<SelectColumn> oneOfColumns)
    {
        Type = type;
        Alias = alias;
        Fragment = fragment;
        OneOfColumns = oneOfColumns;
    }
}

public enum SelectColumnTypes
{
    Unknown,
    TableColumn,
    LiteralValue,
    Query,
    StarDelayed,
    OneOf
}

public static class Extensions
{
    public static SqlDbTypeExt ToCsType(this Literal literal)
    {
        return literal switch
        {
            BinaryLiteral binaryLiteral => SqlDbTypeExt.Binary,
            DefaultLiteral defaultLiteral => SqlDbTypeExt.Null,
            IdentifierLiteral identifierLiteral => SqlDbTypeExt.CsIdentifier,
            IntegerLiteral integerLiteral => SqlDbTypeExt.Int,
            MaxLiteral maxLiteral => SqlDbTypeExt.Max,
            MoneyLiteral moneyLiteral => SqlDbTypeExt.Money,
            NullLiteral nullLiteral => SqlDbTypeExt.Null,
            NumericLiteral numericLiteral => SqlDbTypeExt.Real,
            OdbcLiteral odbcLiteral => SqlDbTypeExt.Unknown,
            RealLiteral realLiteral => SqlDbTypeExt.Real,
            StringLiteral => SqlDbTypeExt.NVarChar,
            _ => SqlDbTypeExt.Unknown
        };
    }
    
    public static SqlDbTypeExt ToCsType(this SqlDataTypeOption type)
    {
        return type switch
        {
            SqlDataTypeOption.None => SqlDbTypeExt.Unknown,
            SqlDataTypeOption.BigInt => SqlDbTypeExt.BigInt,
            SqlDataTypeOption.Int => SqlDbTypeExt.Int,
            SqlDataTypeOption.SmallInt => SqlDbTypeExt.SmallInt,
            SqlDataTypeOption.TinyInt => SqlDbTypeExt.TinyInt,
            SqlDataTypeOption.Bit => SqlDbTypeExt.Bit,
            SqlDataTypeOption.Decimal => SqlDbTypeExt.Decimal,
            SqlDataTypeOption.Numeric => SqlDbTypeExt.Decimal,
            SqlDataTypeOption.Money => SqlDbTypeExt.Money,
            SqlDataTypeOption.SmallMoney => SqlDbTypeExt.SmallMoney,
            SqlDataTypeOption.Float => SqlDbTypeExt.Float,
            SqlDataTypeOption.Real => SqlDbTypeExt.Real,
            SqlDataTypeOption.DateTime => SqlDbTypeExt.DateTime,
            SqlDataTypeOption.SmallDateTime => SqlDbTypeExt.SmallDateTime,
            SqlDataTypeOption.Char => SqlDbTypeExt.Char,
            SqlDataTypeOption.VarChar => SqlDbTypeExt.VarChar,
            SqlDataTypeOption.Text => SqlDbTypeExt.Text,
            SqlDataTypeOption.NChar => SqlDbTypeExt.NChar,
            SqlDataTypeOption.NVarChar => SqlDbTypeExt.NVarChar,
            SqlDataTypeOption.NText => SqlDbTypeExt.NText,
            SqlDataTypeOption.Binary => SqlDbTypeExt.Binary,
            SqlDataTypeOption.VarBinary => SqlDbTypeExt.VarBinary,
            SqlDataTypeOption.Image => SqlDbTypeExt.Image,
            SqlDataTypeOption.Cursor => SqlDbTypeExt.Unknown,
            SqlDataTypeOption.Sql_Variant => SqlDbTypeExt.Variant,
            SqlDataTypeOption.Table => SqlDbTypeExt.Unknown,
            SqlDataTypeOption.Timestamp => SqlDbTypeExt.Timestamp,
            SqlDataTypeOption.UniqueIdentifier => SqlDbTypeExt.UniqueIdentifier,
            SqlDataTypeOption.Date => SqlDbTypeExt.Date,
            SqlDataTypeOption.Time => SqlDbTypeExt.Time,
            SqlDataTypeOption.DateTime2 => SqlDbTypeExt.DateTime2,
            SqlDataTypeOption.DateTimeOffset => SqlDbTypeExt.DateTimeOffset,
            SqlDataTypeOption.Rowversion => SqlDbTypeExt.Unknown,
            _ => SqlDbTypeExt.Unknown
        };
    }
}

class DumpedQuery
{
    public List<DumpedQuery> DumpedQueries { get; set; } = [];
    public string? Model { get; set; }
    public HashSet<string> PropertyNames { get; set; } = [];
    public List<string> Ambiguities { get; set; } = [];

    public string Flatten()
    {
        StringBuilder sb = new StringBuilder();

        if (Model is not null)
        {
            sb.AppendLine(Model);   
        }

        foreach (DumpedQuery child in DumpedQueries)
        {
            sb.AppendLine(child.Flatten());   
        }
        
        return sb.ToString().Trim();
    }

    public List<string> FlattenAmbiguities(List<string>? accu = null)
    {
        accu ??= [];
        accu.AddRange(Ambiguities);

        foreach (DumpedQuery child in DumpedQueries)
        {
            accu.AddRange(child.FlattenAmbiguities());
        }

        return accu;
    }
}

class Query
{
    public List<Query> Childs { get; set; } = [];
    public JsonForClauseOptions JsonOptions { get; set; }
    public string RawSql { get; set; }
    public string? TransformedSql { get; set; }
    public QuerySpecification QuerySpecification { get; set; }
    public int ColumnIndex { get; set; }
    public List<SqlResultSetColumn>? SelectColumns { get; set; }
    public Dictionary<string, int> StarsOffset { get; set; } = [];

    public DumpedQuery Dump(int modelIndex = 0, int jsonDepth = 0)
    {
        StringBuilder sb = new StringBuilder();

        DumpedQuery dumpedQuery = new DumpedQuery();
        
        int unkIndex = 0;

        string GetColName(SqlResultSetColumn col)
        {
            string res;
            
            if (col.Name is not null)
            {
                res = col.Name.FirstCharToUpper();
            }
            else
            {
                res = $"NoColumnName{unkIndex}";
                unkIndex++;
            }

            if (!dumpedQuery.PropertyNames.Add(res))
            {
                dumpedQuery.Ambiguities.Add(res);
            }
            
            return res;
        } 

        if (SelectColumns is not null)
        {
            if (jsonDepth is 1)
            {
                sb.AppendLine("[SqlJson]");
            }
            
            sb.AppendLine($"public class Model{modelIndex}");
            sb.AppendLine("{");
            
            foreach (SqlResultSetColumn col in SelectColumns.OrderBy(x => x.Position))
            {
                Query? child = Childs.FirstOrDefault(x => x.ColumnIndex == col.Position);

                if (child is not null)
                {
                    sb.AppendLine($"public {(jsonDepth is 0 ? "Json<" : "")}List<Model{modelIndex + 1}>?{(jsonDepth is 0 ? ">?" : "")} {GetColName(col)} {{ get; set; }}".Indent());

                    DumpedQuery childQuery = child.Dump(modelIndex + 1, jsonDepth + 1);
                    dumpedQuery.DumpedQueries.Add(childQuery);
                }
                else
                {
                    sb.AppendLine($"public {col.Type.ToTypeSyntax(col.Nullable).ToFullString()} {GetColName(col)} {{ get; set; }}".Indent());   
                }
            }
            
            sb.AppendLine("}");
        }
        else
        {
            foreach (Query child in Childs)
            {
                dumpedQuery.DumpedQueries.Add(child.Dump());
            }
        }

        dumpedQuery.Model = sb.ToString();
        return dumpedQuery;
    }
}

class Scope
{
    public List<ScopeIdentifier> Identifiers { get; set; } = [];
    public Scope? Parent { get; set; }
    public QuerySpecification? Select { get; set; }
    public List<SelectColumn> SelectColumns { get; set; } = [];
    public JsonForClauseOptions JsonOptions { get; set; }
    public bool DependenciesGathered { get; set; }

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
        DependenciesGathered = true;
        
        if (Identifiers.Count is 0 && SelectColumns.Count is 0)
        {
            return [];
        }

        HashSet<string> deps = [];

        foreach (ScopeIdentifier ident in Identifiers)
        {
            if (ident.Table is not null)
            {
                deps.Add(ident.Table);   
            }

            if (ident.Scope is not null)
            {
                HashSet<string> childDeps = ident.Scope.GatherDependencies();

                foreach (string dep in childDeps)
                {
                    deps.Add(dep);
                }
            }
        }

        void GatherColDeps(SelectColumn column)
        {
            if (!column.Scope?.DependenciesGathered ?? false)
            {
                HashSet<string> childDeps = column.Scope.GatherDependencies();

                foreach (string dep in childDeps)
                {
                    deps.Add(dep);
                }
            }
            
            if (column.OneOfColumns is not null)
            {
                foreach (SelectColumn oneOfCol in column.OneOfColumns)
                {
                    GatherColDeps(oneOfCol);
                }   
            }
        }

        foreach (SelectColumn column in SelectColumns)
        {
            GatherColDeps(column);
        }

        return deps;
    }

    private ScopeIdentifier? GetSource(string ident)
    {
        return Identifiers.FirstOrDefault(x => string.Equals(x.Ident, ident, StringComparison.InvariantCultureIgnoreCase)) ?? Parent?.GetSource(ident);
    }

    public MappedModel Compile(ConcurrentDictionary<string, SqlTable> schema, int modelIndex, List<SelectColumn>? columns = null)
    {
        int unkIndex = 0;
        MappedModel model = new MappedModel($"Model{modelIndex}", JsonOptions);

        string GetUnkIdent()
        {
            string str = $"NoColumnName{unkIndex}";
            unkIndex++;
            return str;
        }

        void CheckDupeName(SelectColumn column, MappedModelProperty prop)
        {
            if (!model.PropertyNames.Add(prop.Name))
            {
                model.Ambiguities.Add(new MappedModelAmbiguity(column, prop));
            }
        }
        
        MappedModelProperty AddPropertyRaw(SelectColumn column, MappedModelProperty property)
        {
            property.Name = property.Name.FirstCharToUpper() ?? string.Empty;
            CheckDupeName(column, property);
            model.Properties.Add(property);
            return property;
        }

        MappedModelProperty AddProperty(SelectColumn column, SqlDbTypeExt type, bool nullable, string? name, MappedModelPropertyTypeFlags flags, SqlTable? sourceTable)
        {
            MappedModelProperty property = new MappedModelProperty(type, nullable, name?.FirstCharToUpper() ?? GetUnkIdent(), flags, sourceTable);
            CheckDupeName(column, property);
            model.Properties.Add(property);
            return property;
        }
        
        MappedModelProperty AddPropertyWithModel(SelectColumn column, SqlDbTypeExt type, bool nullable, string? name, MappedModel mdl, MappedModelPropertyTypeFlags flags)
        {
            MappedModelProperty property = new MappedModelProperty(type, nullable, name?.FirstCharToUpper() ?? GetUnkIdent(), mdl, flags, null);
            CheckDupeName(column, property);
            model.Properties.Add(property);
            return property;
        }

        int GetModelIndex()
        {
            modelIndex++;
            return modelIndex;
        }

        foreach (SelectColumn column in columns ?? SelectColumns)
        {
            switch (column.OutputType)
            {
                case SelectColumnTypes.Query:
                {
                    if (column.Scope is not null)
                    {
                        MappedModel subModel = column.Scope.Compile(schema, GetModelIndex());
                        bool mapAsJson = subModel.JsonOptions.HasFlag(JsonForClauseOptions.Path) || subModel.JsonOptions.HasFlag(JsonForClauseOptions.Auto);
                        bool mapAsList = !subModel.JsonOptions.HasFlag(JsonForClauseOptions.WithoutArrayWrapper);

                        MappedModelPropertyTypeFlags flags = MappedModelPropertyTypeFlags.None;

                        if (mapAsJson)
                        {
                            if (mapAsList)
                            {
                                flags |= MappedModelPropertyTypeFlags.List;
                            }
                        
                            if (JsonOptions.HasFlag(JsonForClauseOptions.Path) || JsonOptions.HasFlag(JsonForClauseOptions.Auto))
                            {
                                flags |= MappedModelPropertyTypeFlags.NestedJson;
                            }
                            else
                            {
                                flags |= MappedModelPropertyTypeFlags.Json;
                            }
                        
                            AddPropertyWithModel(column, SqlDbTypeExt.CsIdentifier, true, column.ColumnOutputName, subModel, flags);
                        }
                        else
                        {
                            MappedModelProperty? firstProp = subModel.Properties.FirstOrDefault();

                            if (firstProp is not null)
                            {
                                firstProp.Name = column.ColumnOutputName ?? firstProp.Name;

                                if (!firstProp.Flags.HasFlag(MappedModelPropertyTypeFlags.LiteralValue))
                                {
                                    firstProp.Nullable = true;
                                }
                            
                                AddPropertyRaw(column, firstProp);
                            }
                        }
                    }

                    break;
                }
                case SelectColumnTypes.TableColumn:
                {
                    ScopeIdentifier? identifier = Identifiers.Count > 0 ? Identifiers[0] : null;

                    if (column.ColumnSource is not null)
                    {
                        identifier = identifier?.Ident is not null ? GetSource(identifier.Ident) : null;
                    }
                
                    if (identifier?.Table is not null && schema.TryGetValue(identifier.Table, out SqlTable? sqlTable))
                    {
                        if (column.ColumnName is not null && sqlTable.Columns.TryGetValue(column.ColumnName, out SqlTableColumn? colDef))
                        {
                            AddProperty(column, colDef.SqlType, colDef.IsNullable || identifier.Nullable, column.ColumnOutputName, MappedModelPropertyTypeFlags.None, sqlTable);
                        }
                    }
                    else if (identifier?.Scope is not null)
                    {
                        MappedModel fromModel = identifier.Scope.Compile(schema, modelIndex);
                        MappedModelProperty? fromProp = fromModel.Properties.FirstOrDefault(x => string.Equals(x.Name, column.ColumnName, StringComparison.InvariantCultureIgnoreCase));

                        if (fromProp is not null)
                        {
                            AddProperty(column, fromProp.Type, fromProp.Nullable, column.ColumnOutputName, fromProp.Flags, fromProp.SourceTable);
                        }
                    
                        int z = 0;
                    }

                    break;
                }
                case SelectColumnTypes.LiteralValue:
                {
                    AddProperty(column, column.OutputLiteralType ?? SqlDbTypeExt.Unknown, column.Nullable, column.ColumnOutputName, MappedModelPropertyTypeFlags.LiteralValue, null);
                    break;
                }
                case SelectColumnTypes.StarDelayed:
                {
                    if (column.Scope is not null)
                    {
                        foreach (ScopeIdentifier ident in column.Scope.Identifiers)
                        {
                            if (ident.Scope is not null)
                            {
                                MappedModel fromModel = ident.Scope.Compile(schema, modelIndex);

                                foreach (MappedModelProperty prop in fromModel.Properties)
                                {
                                    AddPropertyRaw(column, prop);
                                }
                            }
                            else if (ident.Table is not null)
                            {
                                if (schema.TryGetValue(ident.Table.ToLowerInvariant(), out SqlTable? sqlTable))
                                {
                                    foreach (KeyValuePair<string, SqlTableColumn> col in sqlTable.Columns.OrderBy(x => x.Value.OrdinalPosition))
                                    {
                                        AddProperty(column, col.Value.SqlType, col.Value.IsNullable, col.Value.Name, MappedModelPropertyTypeFlags.None, sqlTable);
                                    }
                                }
                            }
                        }
                    }

                    break;
                }
                case SelectColumnTypes.OneOf:
                {
                    if (column.OneOfColumns is not null)
                    {
                        MappedModel oneOfModel = Compile(schema, modelIndex, column.OneOfColumns);
                        ResultOrException<MappedModel> mergedModel = oneOfModel.Merge();

                        if (mergedModel.Exception is null && mergedModel.Result?.Properties.Count is 1)
                        {
                            mergedModel.Result.Properties[0].Name = column.ColumnOutputName ?? mergedModel.Result.Properties[0].Name;
                            AddPropertyRaw(column, mergedModel.Result.Properties[0]);
                        }
                        
                        int z = 0;
                    }
                    
                    break;
                }
            }
        }
        
        return model;
    }
}

[Flags]
internal enum MappedModelPropertyTypeFlags
{
    None = 1 << 0,
    List = 1 << 1,
    Json = 1 << 2,
    NestedJson = 1 << 3,
    LiteralValue = 1 << 4,
    BinaryExpr = 1 << 5
}

internal class MappedModelProperty
{
    public SqlDbTypeExt Type { get; set; }
    public bool Nullable { get; set; }
    public string Name { get; set; }
    public MappedModelPropertyTypeFlags Flags { get; set; }
    public MappedModel? Model { get; set; }
    public SqlTable? SourceTable { get; set; }

    public MappedModelProperty(SqlDbTypeExt type, bool nullable, string name, MappedModelPropertyTypeFlags flags, SqlTable? sourceTable)
    {
        Type = type;
        Nullable = nullable;
        Name = name;
        Flags = flags;
        SourceTable = sourceTable;
    }
    
    public MappedModelProperty(SqlDbTypeExt type, bool nullable, string name, MappedModel model, MappedModelPropertyTypeFlags flags, SqlTable? sourceTable)
    {
        Type = type;
        Model = model;
        Nullable = nullable;
        Name = name;
        Flags = flags;
        SourceTable = sourceTable;
    }

    public ResultOrException<bool> Merge(MappedModelProperty prop)
    {
        bool TypesMatch(SqlDbTypeExt a, SqlDbTypeExt b)
        {
            return (prop.Type == a && Type == b) || (prop.Type == b && Type == a);
        }

        bool GetOne(SqlDbTypeExt type, [NotNullWhen(true)] out MappedModelProperty? p)
        {
            if (Type == type)
            {
                p = this;
                return true;
            }

            if (prop.Type == type)
            {
                p = prop;
                return true;
            }

            p = null;
            return false;
        }

        if (TypesMatch(SqlDbTypeExt.CsIdentifier, SqlDbTypeExt.Null))
        {
            if (GetOne(SqlDbTypeExt.CsIdentifier, out MappedModelProperty? target))
            {
                target.Nullable = true;
            }
        }
        else if (TypesMatch(SqlDbTypeExt.CsIdentifier, SqlDbTypeExt.CsIdentifier))
        {
            
        }
        
        return new ResultOrException<bool>(true, null);
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

public enum ErrorDumpModes
{
    HumanReadable,
    Serializable
}

public class MappedModelAmbiguitiesAccumulator
{
    public List<MappedModelAmbiguities> Ambiguities { get; set; } = [];
    public string Input { get; set; }

    public string DumpConcat(ErrorDumpModes mode = ErrorDumpModes.HumanReadable, int snippetMaxLen = 100)
    {
        return string.Join(mode is ErrorDumpModes.HumanReadable ? "\n\n" : "\n", Dump(mode, snippetMaxLen));
    }
    
    public List<string> Dump(ErrorDumpModes mode = ErrorDumpModes.HumanReadable, int snippetMaxLen = 100)
    {
        if (Ambiguities.Count is 0)
        {
            return [];
        }

        List<string> errors = [];

        foreach (MappedModelAmbiguities modelAmbiguities in Ambiguities)
        {
            foreach (MappedModelAmbiguity ambiguity in modelAmbiguities.Ambiguities)
            {
                if (ambiguity.Column.Fragment is not null)
                {
                    int errorStartOffset = ambiguity.Column.Fragment.ScriptTokenStream[ambiguity.Column.Fragment.FirstTokenIndex].Offset;
                    int errorEndOffset = ambiguity.Column.Fragment.ScriptTokenStream[ambiguity.Column.Fragment.LastTokenIndex].Offset;
                    string errorSnippet = Input.Substring(errorStartOffset, Math.Max(1, errorEndOffset - errorStartOffset));

                    if (mode is ErrorDumpModes.Serializable)
                    {
                        errors.Add($"c:\"{ambiguity.Prop.Name}\", m:\"{modelAmbiguities.Model.Name}\", t:\"{ambiguity.Prop.SourceTable?.Name}\", o: {errorStartOffset}");
                        continue;
                    }
                    
                    int remLenPerSide = Math.Max(0, snippetMaxLen - errorSnippet.Length) / 2;
                    errorSnippet = $"--> {errorSnippet} <--";

                    int offsetBeforeLen = Math.Max(0, errorStartOffset - remLenPerSide);
                    int offsetAfterLen = Math.Min(Input.Length, errorEndOffset + remLenPerSide);
                    string errorWithContextSnippet = $"{Input.Substring(offsetBeforeLen, errorStartOffset)}{errorSnippet}{Input.Substring(errorEndOffset + 1, offsetAfterLen - errorEndOffset - 1)}";

                    StringBuilder sb = new StringBuilder();
                    sb.Append($"Error {errors.Count + 1}: column \"{ambiguity.Prop.Name}\", model {modelAmbiguities.Model.Name}");

                    if (ambiguity.Prop.SourceTable is not null)
                    {
                        sb.Append($", table \"{ambiguity.Prop.SourceTable.Name}\"");
                    }

                    sb.Append(", near:\n");
                    sb.Append(errorWithContextSnippet);
                    
                    errors.Add(sb.ToString());   
                    continue;
                }
                
                if (mode is ErrorDumpModes.Serializable)
                {
                    errors.Add($"c:\"{ambiguity.Prop.Name}\", m:\"{modelAmbiguities.Model.Name}\", t:\"{ambiguity.Prop.SourceTable?.Name}\", o: -1");
                    continue;
                }
                
                errors.Add($"Error {errors.Count + 1}: column \"{ambiguity.Prop.Name}\", model {modelAmbiguities.Model.Name}: unknown source location");
            }
        }

        return errors;
    }
}

public class MappedModelAmbiguities
{
    internal MappedModel Model { get; set; }
    internal List<MappedModelAmbiguity> Ambiguities { get; set; } = [];
}

class MappedModelAmbiguity
{
    public SelectColumn Column { get; set; }
    public MappedModelProperty Prop { get; set; }

    public MappedModelAmbiguity(SelectColumn column, MappedModelProperty prop)
    {
        Column = column;
        Prop = prop;
    }
}

class MappedModel
{
    public string Name { get; set; }
    public List<MappedModelProperty> Properties { get; set; } = [];
    public JsonForClauseOptions JsonOptions { get; set; }
    public HashSet<string> PropertyNames { get; set; } = [];
    public List<MappedModelAmbiguity> Ambiguities { get; set; } = [];

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

    public ResultOrException<MappedModel> Merge()
    {
        MappedModel model = new MappedModel(Name, JsonOptions)
        {
            Ambiguities = [..Ambiguities]
        };

        foreach (MappedModelProperty property in Properties)
        {
            if (model.Properties.Count is 0)
            {
                model.Properties.Add(property);
                continue;
            }

            ResultOrException<bool> propMergeResult = model.Properties[0].Merge(property);

            if (propMergeResult.Exception is not null)
            {
                return new ResultOrException<MappedModel>(null, propMergeResult.Exception);
            }
        }
        
        return new ResultOrException<MappedModel>(model, null);
    }

    public List<MappedModelAmbiguities> GetMappingAmbiguities()
    {
        List<MappedModelAmbiguities> accu = [];

        if (Ambiguities.Count > 0)
        {
            accu.Add(new MappedModelAmbiguities { Model = this, Ambiguities = Ambiguities });
        }

        foreach (MappedModelProperty prop in Properties)
        {
            if (prop.Model is not null)
            {
                accu.AddRange(prop.Model.GetMappingAmbiguities());
            }
        }

        return accu;
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
    public Query Query { get; set; }

    void SolveQuery(Scope scope, QuerySpecification qs, Query parent, int columnIndex, Dictionary<string, int> starsOffset)
    {
        Query query = new Query()
        {
            QuerySpecification = qs,
            ColumnIndex = columnIndex,
            StarsOffset = starsOffset
        };

        StringBuilder sb = new StringBuilder();
        
        if (qs.ForClause is JsonForClause jsonForClause)
        {
            int start = qs.ForClause.LastTokenIndex - qs.ForClause.FragmentLength;
            int len = qs.ForClause.FragmentLength;

            for (int i = qs.FirstTokenIndex; i < qs.LastTokenIndex; i++)
            {
                if (i >= start && i <= start + len)
                {
                    continue;
                }

                sb.Append(qs.ScriptTokenStream[i].Text);
            }

            query.TransformedSql = sb.ToString();
            
            foreach (JsonForClauseOption option in jsonForClause.Options)
            {
                query.JsonOptions |= option.OptionKind;
            }
        }

        sb.Clear();
        
        for (int i = qs.FirstTokenIndex; i <= qs.LastTokenIndex; i++)
        {
            sb.Append(qs.ScriptTokenStream[i].Text);
        }
        
        query.RawSql = sb.ToString();

        void SolveSelectScalarCol(ScalarExpression scalarExpression, SelectScalarExpression? selScalar, int cIndex, Dictionary<string, int> starsOffset)
        {
            switch (scalarExpression)
            {
                case ScalarSubquery { QueryExpression: QuerySpecification subquerySpec }:
                {
                    SolveQuery(scope, subquerySpec, query, cIndex, starsOffset);
                    break;
                }      
                case ParenthesisExpression parentExpr:
                {
                    SolveSelectScalarCol(parentExpr.Expression, null, cIndex, starsOffset);
                    break;
                }
                case SearchedCaseExpression searchedCaseExpression:
                {
                    foreach (SearchedWhenClause whenBranch in searchedCaseExpression.WhenClauses)
                    {
                        SolveSelectScalarCol(whenBranch.ThenExpression, null, cIndex, starsOffset);
                    }

                    SolveSelectScalarCol(searchedCaseExpression.ElseExpression, null, cIndex, starsOffset);
                    break;
                }
            }
        }

        Dictionary<string, int> so = [];
        Scope localScope = new Scope(null, qs);

        if (qs.FromClause?.TableReferences is not null)
        {
            foreach (TableReference source in qs.FromClause.TableReferences)
            {
                string? tbl, alias;

                switch (source)
                {
                    case NamedTableReference namedFrom:
                        tbl = namedFrom.SchemaObject.BaseIdentifier.Value;
                        alias = namedFrom.Alias?.Value;

                        localScope.Identifiers.Add(new ScopeIdentifier(alias ?? tbl, tbl, false));
                        break;
                    case QueryDerivedTable queryDerivedTable:
                    {
                        alias = queryDerivedTable.Alias?.Value;

                        if (queryDerivedTable.QueryExpression is QuerySpecification fromQs)
                        {
                            localScope.Identifiers.Add(new ScopeIdentifier(alias, SolveSelect(fromQs, scope), false));
                        }

                        break;
                    }
                    case QualifiedJoin qualifiedJoin:
                    {
                        bool nullable = qualifiedJoin.QualifiedJoinType is QualifiedJoinType.LeftOuter or QualifiedJoinType.FullOuter;

                        if (qualifiedJoin.FirstTableReference is NamedTableReference namedJoin1)
                        {
                            alias = namedJoin1.Alias?.Value;
                            localScope.Identifiers.Add(new ScopeIdentifier(alias, namedJoin1.SchemaObject.BaseIdentifier.Value, nullable));
                        }
                        
                        if (qualifiedJoin.SecondTableReference is NamedTableReference namedJoin2)
                        {
                            alias = namedJoin2.Alias?.Value;
                            localScope.Identifiers.Add(new ScopeIdentifier(alias, namedJoin2.SchemaObject.BaseIdentifier.Value, nullable));
                        }

                        break;
                    }
                }
            }   
        }
        
        for (int i = 0; i < qs.SelectElements.Count; i++)
        {
            switch (qs.SelectElements[i])
            {
                case SelectStarExpression selStar:
                {
                    if (selStar.Qualifier is null || selStar.Qualifier.Identifiers.Count is 1 && selStar.Qualifier.Identifiers[0].Value is "*")
                    {
                        
                    }
                    else
                    {
                        
                    }
                    
                    break;
                }
                case SelectScalarExpression selScalar:
                {
                    SolveSelectScalarCol(selScalar.Expression, selScalar, i, []);
                    so.Clear();
                    break;
                }
            }
        }

        parent.Childs.Add(query);
    }
    
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
                        alias = queryDerivedTable.Alias?.Value;

                        if (queryDerivedTable.QueryExpression is QuerySpecification fromQs)
                        {
                            localScope.Identifiers.Add(new ScopeIdentifier(alias, SolveSelect(fromQs, localScope), false));
                        }

                        break;
                    }
                    case QualifiedJoin qualifiedJoin:
                    {
                        bool nullable = qualifiedJoin.QualifiedJoinType is QualifiedJoinType.LeftOuter or QualifiedJoinType.FullOuter;

                        if (qualifiedJoin.FirstTableReference is NamedTableReference namedJoin1)
                        {
                            alias = namedJoin1.Alias?.Value;
                            localScope.Identifiers.Add(new ScopeIdentifier(alias, namedJoin1.SchemaObject.BaseIdentifier.Value, nullable));
                        }
                        
                        if (qualifiedJoin.SecondTableReference is NamedTableReference namedJoin2)
                        {
                            alias = namedJoin2.Alias?.Value;
                            localScope.Identifiers.Add(new ScopeIdentifier(alias, namedJoin2.SchemaObject.BaseIdentifier.Value, nullable));
                        }

                        break;
                    }
                }
            }
        }
        
        SelectColumn? SolveSelectScalarCol(ScalarExpression scalarExpression, SelectScalarExpression? selScalar)
        {
            string? alias = selScalar?.ColumnName?.Value;
            
            switch (scalarExpression)
            {
                case ColumnReferenceExpression colRef when colRef.MultiPartIdentifier.Identifiers.Count is 1:
                {
                    return new SelectColumn(SelectColumnTypes.TableColumn, alias, colRef.MultiPartIdentifier.Identifiers[0].Value, null, null, colRef);
                }
                case ColumnReferenceExpression colRef when colRef.MultiPartIdentifier.Identifiers.Count is 2:
                {
                    return new SelectColumn(SelectColumnTypes.TableColumn, alias, colRef.MultiPartIdentifier.Identifiers[1].Value, colRef.MultiPartIdentifier.Identifiers[0].Value, null, colRef);
                }
                case ScalarSubquery { QueryExpression: QuerySpecification subquerySpec }:
                {
                    return new SelectColumn(SelectColumnTypes.Query, SolveSelect(subquerySpec, localScope), alias, scalarExpression);
                }
                case Literal literal:
                {
                    return new SelectColumn(SelectColumnTypes.LiteralValue, alias, null, null, literal.Value, literal)
                    {
                        LiteralType = literal.ToCsType()
                    };
                }
                case CastCall castCall:
                {
                    SelectColumn? castQuery = SolveSelectCol(castCall.Parameter);

                    if (castQuery is not null)
                    {
                        if (castCall.DataType is SqlDataTypeReference dataTypeReference)
                        {
                            castQuery.TransformedType = SelectColumnTypes.LiteralValue;
                            castQuery.TransformedLiteralType = dataTypeReference.SqlDataTypeOption.ToCsType();
                            castQuery.Nullable = true;
                        }

                        castQuery.Alias = alias;
                    }
                    
                    return castQuery;
                }
                case SearchedCaseExpression searchedCaseExpression:
                {
                    List<SelectColumn> accu = [];
                    
                    foreach (SearchedWhenClause whenClause in searchedCaseExpression.WhenClauses)
                    {
                        SelectColumn? branchSelect = SolveSelectScalarCol(whenClause.ThenExpression, null);

                        if (branchSelect is not null)
                        {
                            accu.Add(branchSelect);
                        }
                    }

                    SelectColumn? elseSelect = SolveSelectScalarCol(searchedCaseExpression.ElseExpression, null);
                    
                    if (elseSelect is not null)
                    {
                        accu.Add(elseSelect);
                    }

                    foreach (SelectColumn x in accu)
                    {
                        if (x.Type is not SelectColumnTypes.LiteralValue)
                        {
                            return new SelectColumn(SelectColumnTypes.OneOf, alias, searchedCaseExpression, accu);
                        }
                    }
                    
                    return null;
                }
                case BinaryExpression binExpr:
                {
                    SelectColumn? lhs = SolveSelectCol(binExpr.FirstExpression);
                    SelectColumn? rhs = SolveSelectCol(binExpr.SecondExpression);

                    if (lhs?.Type is SelectColumnTypes.LiteralValue && rhs?.Type is SelectColumnTypes.LiteralValue && lhs.LiteralType is not null && rhs.LiteralType is not null)
                    {
                        int lhsP = lhs.LiteralType.Value.ImplicitConversionPriority();
                        int rhsP = rhs.LiteralType.Value.ImplicitConversionPriority();

                        return new SelectColumn(SelectColumnTypes.LiteralValue, alias, null, null, null, binExpr)
                        {
                            LiteralType = lhsP > rhsP ? lhs.LiteralType.Value : rhs.LiteralType.Value,
                            Nullable = lhs.LiteralType is SqlDbTypeExt.Null || rhs.LiteralType is SqlDbTypeExt.Null || lhs.Nullable || rhs.Nullable
                        };
                    }
                    
                    int z = 0;
                    
                    return null;
                }
                case ParenthesisExpression parentExpr:
                {
                    return SolveSelectCol(parentExpr.Expression);
                }
                case FunctionCall functionCall:
                {
                    if (BuiltInFunctions.Common.TryGetValue(functionCall.FunctionName.Value, out Nullable<SqlDbTypeExt>? builtInType))
                    {
                        if (builtInType.Value is not (SqlDbTypeExt.ArgMixed or SqlDbTypeExt.Unknown or SqlDbTypeExt.CsIdentifier))
                        {
                            return new SelectColumn(SelectColumnTypes.LiteralValue, alias, null, null, null, functionCall)
                            {
                                LiteralType = builtInType.Value,
                                Nullable = builtInType.CanBeNull
                            };   
                        }
                    }
                    
                    return null;
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
                {
                    return new SelectColumn(SelectColumnTypes.StarDelayed, null, null, null, selStar, localScope);
                }
                case ScalarExpression scalarExpression:
                {
                    return SolveSelectScalarCol(scalarExpression, null);
                }
                case SelectScalarExpression selScalar:
                {
                    return SolveSelectScalarCol(selScalar.Expression, selScalar);
                }
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
            Query rootQuery = new Query();
            
            if (select.QueryExpression is QuerySpecification qs)
            {
                SolveQuery(root, qs, rootQuery, 0, []);
                root = SolveSelect(qs, root);
            }
            
            Root = root;
            Query = rootQuery;
            
            int z = 0;
        }

        base.Visit(node);
    }
}