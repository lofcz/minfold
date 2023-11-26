using System.Data;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Minfold;

record CsPropertyDecl(string Name, SqlDbType Type, bool Nullable);

public class ClassRewriter : CSharpSyntaxRewriter
{
    private static ClassDeclarationSyntax RemoveProperties(ClassDeclarationSyntax node, string[] properties)
    {
        List<MemberDeclarationSyntax> keptMembers = new List<MemberDeclarationSyntax>();
        
        foreach (MemberDeclarationSyntax member in node.Members)
        {
            if (member is PropertyDeclarationSyntax prop)
            {
                if (properties.Contains(prop.Identifier.ValueText))
                {
                    continue;
                }
            }
            
            keptMembers.Add(member);
        }

        node = node.WithMembers(SyntaxFactory.List(keptMembers));
        return node;
    }
    
    private static ClassDeclarationSyntax AddProperties(ClassDeclarationSyntax node, IEnumerable<CsPropertyDecl> properties)
    {
        int lastIndex = node.Members.LastIndexOf(x => x.IsKind(SyntaxKind.PropertyDeclaration));
        int lastWithTrivia = FindFirstDynamicComment();

        if (lastWithTrivia > -1)
        {
            lastIndex = lastWithTrivia;
        }
        else if (lastIndex < 0)
        {
            lastIndex = 0;
        }
        else
        {
            lastIndex += 1;
        }

        SyntaxList<MemberDeclarationSyntax> nodes = node.Members.InsertRange(lastIndex, properties.Select(x => SyntaxFactory2.MakeProperty(x.Name, x.Type, x.Nullable)));
        return node.WithMembers(nodes);
        
        int FindFirstDynamicComment()
        {
            foreach (MemberDeclarationSyntax tNode in node.Members.Where(tNode => tNode.DescendantTrivia().Where(y => y.IsKind(SyntaxKind.SingleLineCommentTrivia) || y.IsKind(SyntaxKind.MultiLineCommentTrivia)).Any(trivia => (trivia.IsKind(SyntaxKind.MultiLineCommentTrivia) && Regexes.MultilineCommentDynamic.IsMatch(trivia.ToFullString())) || (trivia.IsKind(SyntaxKind.SingleLineCommentTrivia) && Regexes.SinglelineCommentDynamic.IsMatch(trivia.ToFullString())))))
            {
                return node.Members.IndexOf(tNode);
            }

            return -1;
        }
    }

    private static ClassDeclarationSyntax ExtendConstructor(ClassDeclarationSyntax node, CsPropertyDecl[] properties)
    {
        bool ctorUpdated = false;
        
        foreach (MemberDeclarationSyntax mNode in node.Members.Where(x => x.IsKind(SyntaxKind.ConstructorDeclaration)))
        {
            if (mNode is not ConstructorDeclarationSyntax ctorNode)
            {
                continue;
            }

            if (ctorNode.ParameterList.Parameters.Count is 0)
            {
                continue;
            }

            int insertIndex = ctorNode.ParameterList.Parameters.IndexOf(x => x.Default is not null);

            if (insertIndex is -1)
            {
                insertIndex = ctorNode.ParameterList.Parameters.Count;
            }

            SeparatedSyntaxList<ParameterSyntax> newNode = ctorNode.ParameterList.Parameters.InsertRange(insertIndex, properties.Select(x => SyntaxFactory2.MakeParameter(x.Name.FirstCharToLower() ?? string.Empty, x.Type, x.Nullable)));
            ParameterListSyntax newParamList = ctorNode.ParameterList.WithParameters(newNode);
            ctorNode = ctorNode.WithParameterList(newParamList);

            if (ctorNode.Body is null)
            {
                ctorNode = ctorNode.WithBody(SyntaxFactory.Block());
            }

            if (ctorNode.Body is not null)
            {
                SyntaxList<StatementSyntax> newStatements = ctorNode.Body.Statements.AddRange(properties.Select(x => SyntaxFactory2.MakeAssignment(x.Name, x.Name.FirstCharToLower() ?? string.Empty)));
                ctorNode = ctorNode.WithBody(ctorNode.Body.WithStatements(newStatements));
            }
            
            node = node.ReplaceNode(mNode, ctorNode);
            ctorUpdated = true;
            break;
        }

        return node;
    }
    
    private static ClassDeclarationSyntax ShrinkConstructor(ClassDeclarationSyntax node, string[] properties) 
    {
        bool ctorUpdated = false;
        
        foreach (MemberDeclarationSyntax mNode in node.Members.Where(x => x.IsKind(SyntaxKind.ConstructorDeclaration)))
        {
            if (mNode is not ConstructorDeclarationSyntax ctorNode)
            {
                continue;
            }

            if (ctorNode.ParameterList.Parameters.Count is 0)
            {
                continue;
            }

            List<ParameterSyntax> keptParameters = new List<ParameterSyntax>();

            foreach (ParameterSyntax parameter in ctorNode.ParameterList.Parameters)
            {
                if (properties.Contains(parameter.Identifier.ValueText))
                {
                    continue;
                }

                keptParameters.Add(parameter);
            }
            
            ctorNode = ctorNode.WithParameterList(SyntaxFactory.ParameterList(SyntaxFactory.SeparatedList(keptParameters)));

            if (ctorNode.Body is null)
            {
                ctorNode = ctorNode.WithBody(SyntaxFactory.Block());
            }

            if (ctorNode.Body is not null)
            {
                List<StatementSyntax> keptStmts = new List<StatementSyntax>();
                
                foreach (StatementSyntax stmt in ctorNode.Body.Statements)
                {
                    if (stmt is ExpressionStatementSyntax { Expression: AssignmentExpressionSyntax { Left: IdentifierNameSyntax lhsIdent } })
                    {
                        if (properties.Contains(lhsIdent.Identifier.ValueText))
                        {
                            continue;
                        }
                    }

                    keptStmts.Add(stmt);
                }
                
            }
            
            node = node.ReplaceNode(mNode, ctorNode);
            ctorUpdated = true;
            break;
        }

        return node;
    }

    private ClassDeclarationSyntax AddPropertiesExtendCtor(ClassDeclarationSyntax node, IEnumerable<CsPropertyDecl> properties)
    {
        CsPropertyDecl[] propertiesArr = properties.ToArray();
        node = AddProperties(node, propertiesArr);
        node = ExtendConstructor(node, propertiesArr);
        return node;
    }

    private ClassDeclarationSyntax RemovePropertiesShrinkCtor(ClassDeclarationSyntax node, IEnumerable<string> properties)
    {
        string[] propertiesArr = properties.ToArray();
        node = RemoveProperties(node, propertiesArr);
        node = ShrinkConstructor(node, propertiesArr);
        return node;
    }
    
    public override SyntaxNode VisitClassDeclaration(ClassDeclarationSyntax node)
    {
        node = AddPropertiesExtendCtor(node, new CsPropertyDecl[] { new("MojeVlastnost", SqlDbType.Image, true) });
        node = RemovePropertiesShrinkCtor(node, new[] { "Iduser" });
        
        string str2 = node.NormalizeWhitespace().ToFullString();
        return node;
    }
}