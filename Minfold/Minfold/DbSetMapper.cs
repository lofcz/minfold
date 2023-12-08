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
    private readonly Dictionary<string, CsDbSetDecl> dbSetMap;
    
    /// <summary>
    /// Updates a dao
    /// </summary>
    public DbSetMapper(string expectedClassName, Dictionary<string, CsDbSetDecl> dbSetMap)
    {
        this.expectedClassName = expectedClassName;
        this.dbSetMap = dbSetMap;
    }

    public static CsDbSetDecl? MemberIsDbSetDecl(MemberDeclarationSyntax memberDecl)
    {
        if (memberDecl is not PropertyDeclarationSyntax propDecl)
        {
            return null;
        }

        if (propDecl.Type is not GenericNameSyntax genIdent)
        {
            return null;
        }

        if (genIdent.TypeArgumentList.Arguments.Count is not 1)
        {
            return null;
        }

        if (genIdent.Identifier.ValueText is not "DbSet")
        {
            return null;
        }
        
        return new CsDbSetDecl(genIdent.TypeArgumentList.Arguments[0].ToFullString().Trim(), propDecl.Identifier.ValueText.Trim(), propDecl);
    }
    
    public override SyntaxNode VisitClassDeclaration(ClassDeclarationSyntax node)
    {
        if (node.Identifier.ValueText != expectedClassName)
        {
            return node;
        }

        foreach (MemberDeclarationSyntax memberDecl in node.Members)
        {
            CsDbSetDecl? setDecl = MemberIsDbSetDecl(memberDecl);
            
            if (setDecl is null)
            {
                continue;
            }

            if (!dbSetMap.TryAdd(setDecl.ModelName.ToLowerInvariant(), setDecl))
            {
                // [todo] warn
            }
        }
        
        NewCode = node.NormalizeWhitespace().ToFullString();
        
        return node;
    }
}