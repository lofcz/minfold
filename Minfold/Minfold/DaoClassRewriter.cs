using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Minfold;

public class DaoClassRewriter : CSharpSyntaxRewriter
{
    public string NewCode { get; set; }
    public bool ClassRewritten { get; set; }
 
    private SqlTable table;
    private string expectedClassName, modelName;
    private readonly Dictionary<string, CsModelSource> tablesMap;
    private readonly CsModelSource modelSource;
    
    /// <summary>
    /// Updates a dao
    /// </summary>
    public DaoClassRewriter(string expectedClassName, string modelName, SqlTable table, Dictionary<string, CsModelSource> tablesMap, CsModelSource modelSource)
    {
        this.table = table;
        this.expectedClassName = expectedClassName;
        this.tablesMap = tablesMap;
        this.modelSource = modelSource;
        this.modelName = modelName;
    }
    
    public override SyntaxNode VisitClassDeclaration(ClassDeclarationSyntax node)
    {
        if (node.Identifier.ValueText != expectedClassName)
        {
            return node;
        }

        BaseListSyntax? baseList = node.BaseList;
        bool rewriteBase = false;

        if (baseList is null)
        {
            rewriteBase = true;
        }
        else 
        {
            if (baseList.Types.Count is not 1)
            {
                rewriteBase = true;
            }
            else
            {
                if (baseList.Types[0].Type is not GenericNameSyntax gen)
                {
                    rewriteBase = true;
                }
                else if (gen.Identifier.ValueText is not "DaoBase")
                {
                    rewriteBase = true;
                }
                else if (gen.TypeArgumentList.Arguments.Count is not 1)
                {
                    rewriteBase = true;
                }
                else if (gen.TypeArgumentList.Arguments[0] is not IdentifierNameSyntax modelIdent)
                {
                    rewriteBase = true;
                }
                else if (modelIdent.Identifier.ValueText != modelName)
                {
                    rewriteBase = true;
                }
            }
        }

        if (rewriteBase)
        {
            node = node.WithBaseList(SyntaxFactory.BaseList(
                SyntaxFactory.SeparatedList<BaseTypeSyntax>(new[]
                {
                    SyntaxFactory.SimpleBaseType(SyntaxFactory.GenericName(SyntaxFactory.Identifier("DaoBase"), SyntaxFactory.TypeArgumentList(SyntaxFactory.SeparatedList<TypeSyntax>(new[]
                    {
                        SyntaxFactory.IdentifierName("User")
                    }))))
                })));

            ClassRewritten = true;
        }
        
        NewCode = node.NormalizeWhitespace().ToFullString();
        
        return node;
    }
}