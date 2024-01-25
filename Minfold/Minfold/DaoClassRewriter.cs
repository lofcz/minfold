using System.Collections.Concurrent;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Minfold;

public class DaoClassRewriter : CSharpSyntaxRewriter
{
    public string NewCode { get; set; }
    public bool ClassRewritten { get; set; }
 
    private readonly SqlTable table;
    private readonly string expectedClassName, modelName;
    private readonly ConcurrentDictionary<string, CsModelSource> tablesMap;
    private readonly CsModelSource modelSource;
    private readonly string dbSetMappedTableName;
    private readonly string? identityColumnId, identityColumnType;
    private readonly bool generateGetWhereId;
    private readonly ConcurrentDictionary<string, string>? customUsings;
    
    /// <summary>
    /// Updates a dao
    /// </summary>
    public DaoClassRewriter(string expectedClassName, string modelName, SqlTable table, ConcurrentDictionary<string, CsModelSource> tablesMap, CsModelSource modelSource, string dbSetMappedTableName, string? identityColumnId, string? identityColumnType, bool generateGetWhereId, ConcurrentDictionary<string, string>? customUsings)
    {
        this.table = table;
        this.expectedClassName = expectedClassName;
        this.tablesMap = tablesMap;
        this.modelSource = modelSource;
        this.modelName = modelName;
        this.dbSetMappedTableName = dbSetMappedTableName;
        this.identityColumnId = identityColumnId;
        this.identityColumnType = identityColumnType;
        this.generateGetWhereId = generateGetWhereId;
        this.customUsings = customUsings;
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
                        SyntaxFactory.IdentifierName(modelName)
                    }))))
                })));
        }

        bool solved = false;

        MethodDeclarationSyntax? GetWhereIdMethodDecl()
        {
            if (identityColumnId is null || identityColumnType is null)
            {
                return null;
            }
            
            string code = Minfold.GenerateDaoGetWhereId(modelName, dbSetMappedTableName, identityColumnId, identityColumnType);
            CompilationUnitSyntax root = (CompilationUnitSyntax)CSharpSyntaxTree.ParseText(code, new CSharpParseOptions(kind: SourceCodeKind.Script)).GetRoot();
            return (MethodDeclarationSyntax)root.ChildNodes().FirstOrDefault(x => x is MethodDeclarationSyntax)!;
        }
        
        foreach (MemberDeclarationSyntax member in node.Members)
        {
            if (member is MethodDeclarationSyntax { Identifier.ValueText: "GetWhereId" })
            {
                if (!solved && generateGetWhereId && identityColumnId is not null && identityColumnType is not null)
                {
                    MethodDeclarationSyntax getWhereIdDecl = GetWhereIdMethodDecl()!;
                    node = node.WithMembers(node.Members.Replace(member, getWhereIdDecl));
                    solved = true;
                }
                else
                {
                    node = node.WithMembers(node.Members.Remove(member));   
                }
            }
        }

        if (!solved)
        {
            MethodDeclarationSyntax? getWhereIdDecl = GetWhereIdMethodDecl();

            if (getWhereIdDecl is not null)
            {
                node = node.WithMembers(node.Members.Insert(0, getWhereIdDecl));
            }
        }
        
        NewCode = node.NormalizeWhitespace().ToFullString();
        ClassRewritten = true;
        
        return node;
    }
}