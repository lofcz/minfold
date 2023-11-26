using System.Data;
using Microsoft.CodeAnalysis;

namespace Minfold;

public record SqlTable(string Name, List<SqlTableColumn> Columns);
public record SqlTableColumn(string Name, int OrdinalPosition, bool IsNullable, bool IsIdentity, SqlDbType SqlType, List<SqlForeignKey> ForeignKeys);
public record SqlForeignKey(string Name, string Table, string Column, string RefTable, string RefColumn, bool NotEnforced);
public record CsModelSource(string Name, string ModelPath, string DaoPath, string ModelSourceCode, string DaoSourceCode, SyntaxTree ModelAst, SyntaxTree DaoAst);
public record CsSource(string DbPath, string DbSource, List<CsModelSource> Models);