
using System.Data;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

public static class Extensions
{
    public static TypeSyntax ToTypeSyntax(this string type) => SyntaxFactory.IdentifierName(SyntaxFactory.Identifier(type));
    public static TypeSyntax ToTypeSyntax(this string type, params string[] generic) => type.ToTypeSyntax(generic.Select(x => x.ToTypeSyntax()).ToArray());
    public static TypeSyntax ToTypeSyntax(this string type, params TypeSyntax[] generic) => SyntaxFactory.GenericName(SyntaxFactory.Identifier(type), SyntaxFactory.TypeArgumentList(
            SyntaxFactory.SeparatedList(generic.Select(x => 
                    {
                        if (x is GenericNameSyntax genX)
                        {
                            return genX.Identifier.ToString().ToTypeSyntax(genX.TypeArgumentList.Arguments.ToArray());
                        }
                        
                        return x;
                    }
                )
            )
        )
    );
    public static TypeSyntax ToTypeSyntax(this SqlDbType type, bool nullable)
    {
        char? n = nullable ? '?' : null;
        return type switch
        {
            SqlDbType.Binary => $"byte[]{n}".ToTypeSyntax(),
            SqlDbType.Bit => $"bool{n}".ToTypeSyntax(),
            SqlDbType.Char => $"char{n}".ToTypeSyntax(),
            SqlDbType.Date => $"DateTime{n}".ToTypeSyntax(),
            SqlDbType.Decimal => $"decimal{n}".ToTypeSyntax(),
            SqlDbType.Float => $"double{n}".ToTypeSyntax(),
            SqlDbType.Image => $"byte[]{n}".ToTypeSyntax(),
            SqlDbType.Int => $"int{n}".ToTypeSyntax(),
            SqlDbType.Money => $"decimal{n}".ToTypeSyntax(),
            SqlDbType.Real => $"single{n}".ToTypeSyntax(),
            SqlDbType.Text => $"string{n}".ToTypeSyntax(),
            SqlDbType.Time => $"TimeSpan{n}".ToTypeSyntax(),
            SqlDbType.Timestamp => $"byte[]{n}".ToTypeSyntax(),
            SqlDbType.DateTime => $"DateTime{n}".ToTypeSyntax(),
            SqlDbType.DateTime2 => $"DateTime{n}".ToTypeSyntax(),
            SqlDbType.NChar => $"char{n}".ToTypeSyntax(),
            SqlDbType.NText => $"char{n}".ToTypeSyntax(),
            SqlDbType.SmallInt => $"short{n}".ToTypeSyntax(),
            SqlDbType.SmallMoney => $"decimal{n}".ToTypeSyntax(),
            SqlDbType.TinyInt => $"byte{n}".ToTypeSyntax(),
            SqlDbType.UniqueIdentifier => $"Guid{n}".ToTypeSyntax(),
            SqlDbType.VarBinary => $"byte[]{n}".ToTypeSyntax(),
            SqlDbType.VarChar => $"string{n}".ToTypeSyntax(),
            SqlDbType.DateTimeOffset => $"DateTimeOffset{n}".ToTypeSyntax(),
            SqlDbType.NVarChar => $"string{n}".ToTypeSyntax(),
            SqlDbType.SmallDateTime => $"DateTime{n}".ToTypeSyntax(),
            SqlDbType.Xml => $"string{n}".ToTypeSyntax(),
            _ => "string".ToTypeSyntax()
        };
    }
    
    public static string? FirstCharToLower(this string? str)
    {
        if (!string.IsNullOrEmpty(str) && char.IsUpper(str[0]))
        {
            return str.Length is 1 ? char.ToLower(str[0]).ToString() : char.ToLower(str[0]) + str[1..];   
        }

        return str;
    }
}