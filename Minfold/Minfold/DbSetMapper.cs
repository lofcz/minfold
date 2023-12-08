using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Minfold;

public class DbSetMapper : CSharpSyntaxRewriter
{
    public string NewCode { get; set; }
    public bool ClassRewritten { get; set; }
    
    private string expectedClassName;
    private readonly Dictionary<string, string> dbSetMap;
    
    /// <summary>
    /// Updates a dao
    /// </summary>
    public DbSetMapper(string expectedClassName, Dictionary<string, string> dbSetMap)
    {
        this.expectedClassName = expectedClassName;
        this.dbSetMap = dbSetMap;
    }
    
    public override SyntaxNode VisitClassDeclaration(ClassDeclarationSyntax node)
    {
        if (node.Identifier.ValueText != expectedClassName)
        {
            return node;
        }

        foreach (MemberDeclarationSyntax memberDecl in node.Members)
        {
            if (memberDecl is not PropertyDeclarationSyntax propDecl)
            {
                continue;
            }

            if (propDecl.Type is not GenericNameSyntax genIdent)
            {
                continue;
            }

            if (genIdent.TypeArgumentList.Arguments.Count is not 1)
            {
                continue;
            }

            if (genIdent.Identifier.ValueText is not "DbSet")
            {
                continue;
            }
            
            if (!propDecl.Modifiers.Any(SyntaxKind.VirtualKeyword))
            {
                // [todo] warn
            }

            string modelName = genIdent.TypeArgumentList.Arguments[0].ToFullString();
            string setName = propDecl.Identifier.ValueText;

            if (!dbSetMap.TryAdd(modelName.ToLowerInvariant(), setName))
            {
                // [todo] warn
            }
        }
        
        NewCode = node.NormalizeWhitespace().ToFullString();
        
        return node;
    }
}