using System.Collections.Concurrent;
using System.Data;
using Microsoft.CodeAnalysis;

namespace Minfold;

public record SqlTable(string Name, Dictionary<string, SqlTableColumn> Columns);
public record SqlTableColumn(string Name, int OrdinalPosition, bool IsNullable, bool IsIdentity, SqlDbTypeExt SqlType, List<SqlForeignKey> ForeignKeys, bool IsComputed);
public record SqlForeignKey(string Name, string Table, string Column, string RefTable, string RefColumn, bool NotEnforced);
public record CsModelSource(string Name, string ModelPath, string? DaoPath, string ModelSourceCode, string? DaoSourceCode, SyntaxTree ModelAst, SyntaxTree? DaoAst, string NameLastPart, SqlTable? Table, SyntaxNode ModelRootNode, Dictionary<string, string> Columns);
public record CsSource(string DbPath, string DbSource, ConcurrentDictionary<string, CsModelSource> Models, string ProjectNamespace, string ProjectPath);
public record ColumnDefaultVal(SqlTableColumn Column, string? DefaultValue, ColumnDefaultValTypes Type);
public record ModelClassRewriteResult(bool Rewritten, string Text);
public record ModelActionsPatch(List<ModelActionType> Actions);
public record CsPropertyDecl(string Name, SqlDbTypeExt Type, bool Nullable, List<SqlForeignKey> FkForeignKeys, string? Token);
public record CsForeignKey(string? Target, bool? Enforced);
public record CsPropertyInfo(string Name, bool Mapped, List<CsForeignKey> ForeignKeys, ColumnDefaultVal? DefaultVal, CsPropertyDecl Decl);
public record ModelPropertiesPatch(List<CsPropertyDecl> PropertiesAdd, List<string> PropertiesRemove, List<CsPropertyDecl> PropertiesUpdate, CsPropertiesInfo PropertiesInfo);
public record ModelForeignKeysPatch(Dictionary<string, CsPropertyFkDecl> PropertiesUpdate);
public record CsPropertyFkDecl(string Name, List<SqlForeignKey> ForeignKeys);

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