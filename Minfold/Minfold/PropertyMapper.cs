using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Minfold;

public class PropertyMapper : CSharpSyntaxRewriter
{
    private string expectedClassName;
    private readonly CsModelSource modelSource;
    
    /// <summary>
    /// Maps model
    /// </summary>
    public PropertyMapper(string expectedClassName, CsModelSource modelSource)
    {
        this.expectedClassName = expectedClassName;
        this.modelSource = modelSource;
    }
    
    private void MapColumns(ClassDeclarationSyntax node)
    {
        modelSource.Columns.Clear();

        foreach (MemberDeclarationSyntax member in node.Members)
        {
            if (member is not PropertyDeclarationSyntax propDecl)
            {
                continue;
            }

            modelSource.Columns.TryAdd(propDecl.Identifier.ValueText.ToLowerInvariant(), propDecl.Identifier.ValueText);
        }
    }
    
    public override SyntaxNode VisitClassDeclaration(ClassDeclarationSyntax node)
    {
        if (node.Identifier.ValueText != expectedClassName)
        {
            return node;
        }
        
        MapColumns(node);
        return node;
    }
}