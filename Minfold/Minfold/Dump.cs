using System.Collections.Concurrent;
using System.Data;
using Microsoft.CodeAnalysis;

namespace Minfold;

public record SqlTable(string Name, Dictionary<string, SqlTableColumn> Columns);
public record SqlTableColumn(string Name, int OrdinalPosition, bool IsNullable, bool IsIdentity, SqlDbTypeExt SqlType, List<SqlForeignKey> ForeignKeys);
public record SqlForeignKey(string Name, string Table, string Column, string RefTable, string RefColumn, bool NotEnforced);
public record CsModelSource(string Name, string ModelPath, string DaoPath, string ModelSourceCode, string DaoSourceCode, SyntaxTree ModelAst, SyntaxTree DaoAst, string NameLastPart, SqlTable? Table, SyntaxNode ModelRootNode, Dictionary<string, string> Columns);
public record CsSource(string DbPath, string DbSource, ConcurrentDictionary<string, CsModelSource> Models);