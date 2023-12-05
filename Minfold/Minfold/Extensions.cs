
using System.Data;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Minfold;

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

    private static CsPropertyDecl ToPropertyDecl(TypeSyntax type, string ident, bool nullable)
    {
        return type switch
        {
            PredefinedTypeSyntax baseType => new CsPropertyDecl(ident, baseType.Keyword.ValueText.ToSqlType(), nullable, [], type.ToFullString()),
            NullableTypeSyntax nullableType => ToPropertyDecl(nullableType.ElementType, ident, true),
            IdentifierNameSyntax identSyntax => new CsPropertyDecl(ident, identSyntax.Identifier.ValueText.ToSqlType(), nullable, [], type.ToFullString()),
            GenericNameSyntax genIdent => new CsPropertyDecl(ident, SqlDbTypeExt.CsIdentifier, nullable, [], type.ToFullString()),
            _ => new CsPropertyDecl(ident, SqlDbTypeExt.Unknown, nullable, [], type.ToFullString())
        };
    }
    
    public static CsPropertyDecl ToPropertyDecl(this PropertyDeclarationSyntax prop)
    {
        string ident = prop.Identifier.ValueText;
        return ToPropertyDecl(prop.Type, ident, false);
    }

    public static bool CsEqual(this SqlDbTypeExt a, SqlDbTypeExt b)
    {
        if (SqlTypes.TryGetValue(a, out string? aCs) && SqlTypes.TryGetValue(b, out string? bCs))
        {
            return aCs == bCs;
        }

        return false;
    }

    private static Dictionary<SqlDbTypeExt, string> SqlTypes = new Dictionary<SqlDbTypeExt, string>
    {
        { SqlDbTypeExt.Binary, "byte[]" },
        { SqlDbTypeExt.Bit, "bool" },
        { SqlDbTypeExt.Char, "char" },
        { SqlDbTypeExt.Date, "DateTime" },
        { SqlDbTypeExt.Decimal, "decimal" },
        { SqlDbTypeExt.Float, "double" },
        { SqlDbTypeExt.Image, "byte[]" },
        { SqlDbTypeExt.Int, "int" },
        { SqlDbTypeExt.Money, "decimal" },
        { SqlDbTypeExt.Real, "single" },
        { SqlDbTypeExt.Text, "string" },
        { SqlDbTypeExt.Time, "TimeSpan" },
        { SqlDbTypeExt.Timestamp, "byte[]" },
        { SqlDbTypeExt.DateTime, "DateTime" },
        { SqlDbTypeExt.DateTime2, "DateTime" },
        { SqlDbTypeExt.NChar, "char" },
        { SqlDbTypeExt.NText, "char" },
        { SqlDbTypeExt.SmallInt, "short" },
        { SqlDbTypeExt.SmallMoney, "decimal" },
        { SqlDbTypeExt.TinyInt, "byte" },
        { SqlDbTypeExt.UniqueIdentifier, "Guid" },
        { SqlDbTypeExt.VarBinary, "byte[]" },
        { SqlDbTypeExt.VarChar, "string" },
        { SqlDbTypeExt.DateTimeOffset, "DateTimeOffset" },
        { SqlDbTypeExt.NVarChar, "string" },
        { SqlDbTypeExt.SmallDateTime, "DateTime" },
        { SqlDbTypeExt.Xml, "string" }
    };
    
    public static TypeSyntax ToTypeSyntax(this SqlDbTypeExt type, bool nullable)
    {
        char? n = nullable ? '?' : null;
        return SqlTypes.TryGetValue(type, out string? strType) ? $"{strType}{n}".ToTypeSyntax() : "string".ToTypeSyntax();
    }
    
    public static SqlDbTypeExt ToSqlType(this string type)
    {
        return type switch
        {
            "string" => SqlDbTypeExt.NVarChar,
            "int" => SqlDbTypeExt.Int,
            "double" => SqlDbTypeExt.Float,
            "DateTime" => SqlDbTypeExt.DateTime2,
            "bool" => SqlDbTypeExt.Bit,
            "Guid" => SqlDbTypeExt.UniqueIdentifier,
            "float" => SqlDbTypeExt.Real,
            "byte[]" => SqlDbTypeExt.Binary,
            "char" => SqlDbTypeExt.Char,
            "decimal" => SqlDbTypeExt.Decimal,
            "TimeSpan" => SqlDbTypeExt.Time,
            "short" => SqlDbTypeExt.SmallInt,
            "byte" => SqlDbTypeExt.TinyInt,
            "DateTimeOffset" => SqlDbTypeExt.DateTimeOffset,
            _ => SqlDbTypeExt.CsIdentifier
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
    
    public static string? FirstCharToUpper(this string? str)
    {
        if (!string.IsNullOrEmpty(str) && char.IsLower(str[0]))
        {
            return str.Length is 1 ? char.ToUpper(str[0]).ToString() : char.ToUpper(str[0]) + str[1..];   
        }

        return str;
    }
    
    public static string ReplaceLast(this string source, string find, string replace)
    {
        int place = source.LastIndexOf(find, StringComparison.InvariantCulture);
        return place == -1 ? source : source.Remove(place, find.Length).Insert(place, replace);
    }
}