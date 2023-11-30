using System.Data;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
namespace Minfold;

public static class SyntaxFactory2
{
    public static PropertyDeclarationSyntax MakeProperty(string name, SqlDbTypeExt type, bool nullable)
    {
        PropertyDeclarationSyntax property = SyntaxFactory.PropertyDeclaration(type.ToTypeSyntax(nullable), name)
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .AddAccessorListAccessors(
                SyntaxFactory.AccessorDeclaration(SyntaxKind.GetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)),
                SyntaxFactory.AccessorDeclaration(SyntaxKind.SetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken))
            );

        return property;
    }
    
    public static ParameterSyntax MakeParameter(string name, SqlDbTypeExt type, bool nullable)
    {
        ParameterSyntax property = SyntaxFactory.Parameter(SyntaxFactory.Identifier(name))
            .WithType(type.ToTypeSyntax(nullable));
        return property;
    }

    public static ExpressionStatementSyntax MakeAssignment(string identifier, string valueIdentifier)
    {
        AssignmentExpressionSyntax newAssignment = SyntaxFactory.AssignmentExpression(SyntaxKind.SimpleAssignmentExpression, SyntaxFactory.IdentifierName(identifier), SyntaxFactory.IdentifierName(valueIdentifier));
        return SyntaxFactory.ExpressionStatement(newAssignment);
    }
}