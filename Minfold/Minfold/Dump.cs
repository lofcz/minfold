using System.Collections.Concurrent;
using System.Data;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.MSBuild;
using Microsoft.Data.SqlClient;

namespace Minfold;

public record SqlTable(string Name, Dictionary<string, SqlTableColumn> Columns);
public record SqlTableColumn(string Name, int OrdinalPosition, bool IsNullable, bool IsIdentity, SqlDbTypeExt SqlType, List<SqlForeignKey> ForeignKeys, bool IsComputed, bool IsPrimaryKey, string? ComputedSql);
public record SqlForeignKey(string Name, string Table, string Column, string RefTable, string RefColumn, bool NotEnforced);
public record CsModelSource(string Name, string ModelPath, string? DaoPath, string ModelSourceCode, string? DaoSourceCode, SyntaxTree ModelAst, SyntaxTree? DaoAst, string NameLastPart, SqlTable? Table, SyntaxNode ModelRootNode, SyntaxNode? DaoRootNode, Dictionary<string, string> Columns, ModelInfo ModelInfo);
public record CsSource(string DbPath, string DbSource, ConcurrentDictionary<string, CsModelSource> Models, ConcurrentDictionary<string, string> Daos, string ProjectNamespace, string ProjectPath, Dictionary<string, CsDbSetDecl> DbSetMap);
public record ColumnDefaultVal(SqlTableColumn Column, string? DefaultValue, ColumnDefaultValTypes Type);
public record ClassRewriteResult(bool Rewritten, string? Text);
public record ModelClassRewriteResult(bool Rewritten, string? Text, CsPropertiesInfo? Properties) : ClassRewriteResult(Rewritten, Text);
public record ModelActionsPatch(List<ModelActionType> Actions);
public record CsPropertyDecl(string Name, SqlDbTypeExt Type, bool Nullable, List<SqlForeignKey> FkForeignKeys, string? Token);
public record CsForeignKey(string? Target, bool? Enforced);
public record CsPropertyInfo(string Name, bool Mapped, List<CsForeignKey> ForeignKeys, ColumnDefaultVal? DefaultVal, CsPropertyDecl Decl, CsTypeAlias? TypeAlias, SqlTableColumn? Column, bool CanSet);
public record ModelPropertiesPatch(List<CsPropertyDecl> PropertiesAdd, List<string> PropertiesRemove, List<CsPropertyDecl> PropertiesUpdate, CsPropertiesInfo PropertiesInfo);
public record ModelForeignKeysPatch(Dictionary<string, CsPropertyFkDecl> PropertiesUpdate);
public record CsPropertyFkDecl(string Name, List<SqlForeignKey> ForeignKeys);
public record SemanticSolution(MSBuildWorkspace Workspace, Solution Solution);
public record CsModelGenerateResult(string Name, string Code, string Namespace, Dictionary<string, string> Columns, CsPropertiesInfo PropertiesInfo);
public record CsTypeAlias(string Symbol, Dictionary<string, string> Usings);
public record CsDbSetDecl(string ModelName, string SetName, PropertyDeclarationSyntax? Decl);
public record MinfoldCfg(bool UniformPk);
public record ResultOrException<T>(T? Result, Exception? Exception);
public record MinfoldError(MinfoldSteps Step, string Messsage, Exception Exception);
public record MinfoldResult(MinfoldError? Error);

public record SqlConnectionResult(SqlConnection? Connection, Exception? Exception) : IDisposable, IAsyncDisposable
{
    public void Dispose()
    {
        Connection?.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        if (Connection is not null)
        {
            return Connection.DisposeAsync();
        }
        
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }
}

public enum ColumnDefaultValTypes
{
    UserAssigned,
    Optional,
    Value
}

public enum ModelActionType
{
    EmptyCtor,
    ModelCtor
}

public enum MinfoldSteps
{
    ConnectDb,
    AnalyzeDb,
    LoadCode,
    MapSets,
    InferConfig,
    LoadConfig,
    UpdateCreateModels,
    UpdateCreateDaos,
    DeleteModels,
    DeleteDaos,
    UpdateSets
}

public class CsPropertiesInfo
{
    public Dictionary<string, CsPropertyInfo> Properties { get; set; } = new Dictionary<string, CsPropertyInfo>();
}

public class CsPropertyDeclPatch(CsPropertyDecl property, bool solved, bool mapped)
{
    public CsPropertyDecl Property { get; set; } = property;
    public bool Solved { get; set; } = solved;
    public bool Mapped { get; set; } = mapped;
}

public class ModelInfo
{
    public string? Namespace { get; set; }
}