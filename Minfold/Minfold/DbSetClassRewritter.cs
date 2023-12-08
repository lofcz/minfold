using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Minfold;

public class DbSetClassRewritter : CSharpSyntaxRewriter
{
    public string NewCode { get; set; }
    public bool ClassRewritten { get; set; }
    
    private string expectedClassName;
    private readonly List<CsDbSetDecl> sets;
    
    /// <summary>
    /// Updates a dao
    /// </summary>
    public DbSetClassRewritter(string expectedClassName, List<CsDbSetDecl> sets)
    {
        this.expectedClassName = expectedClassName;
        this.sets = sets;
    }
    
    public override SyntaxNode VisitClassDeclaration(ClassDeclarationSyntax node)
    {
        if (node.Identifier.ValueText != expectedClassName)
        {
            return node;
        }
        
        List<MemberDeclarationSyntax> beforePropMembers = [];
        List<MemberDeclarationSyntax> propMembers = [];
        List<MemberDeclarationSyntax> afterPropMembers = [];
        
        foreach (MemberDeclarationSyntax memberDecl in node.Members)
        {
            if (memberDecl is ConstructorDeclarationSyntax)
            {
                beforePropMembers.Add(memberDecl);
                continue;
            }

            CsDbSetDecl? setDecl = DbSetMapper.MemberIsDbSetDecl(memberDecl);

            if (setDecl is null)
            {
                afterPropMembers.Add(memberDecl);   
            }
        }

        propMembers.AddRange(sets.Select(decl => SyntaxFactory.PropertyDeclaration(SyntaxFactory.GenericName(SyntaxFactory.Identifier("DbSet"))
                .WithTypeArgumentList(SyntaxFactory.TypeArgumentList(SyntaxFactory.SingletonSeparatedList<TypeSyntax>(SyntaxFactory.IdentifierName(decl.ModelName)))), SyntaxFactory.Identifier(decl.SetName))
                .WithModifiers(SyntaxFactory.TokenList([SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.VirtualKeyword)]))
                .WithAccessorList(SyntaxFactory.AccessorList(SyntaxFactory.List([SyntaxFactory.AccessorDeclaration(SyntaxKind.GetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)), SyntaxFactory.AccessorDeclaration(SyntaxKind.SetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken))])))));

        node = node.WithMembers(SyntaxFactory.List([..beforePropMembers, ..propMembers, ..afterPropMembers]));
        
        NewCode = node.NormalizeWhitespace().ToFullString();
        
        return node;
    }
}