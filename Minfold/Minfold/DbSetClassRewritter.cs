using System.Collections.Concurrent;
using System.Text;
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
    private readonly Dictionary<string, SqlTable> modelsToTablesMap;
    private readonly ConcurrentDictionary<string, CsPropertiesInfo> modelProperties;
    
    /// <summary>
    /// Updates database set class
    /// </summary>
    public DbSetClassRewritter(string expectedClassName, List<CsDbSetDecl> sets, Dictionary<string, SqlTable> modelsToTablesMap, ConcurrentDictionary<string, CsPropertiesInfo> modelProperties)
    {
        this.expectedClassName = expectedClassName;
        this.sets = sets;
        this.modelsToTablesMap = modelsToTablesMap;
        this.modelProperties = modelProperties;
    }

    private string GenerateModelMap()
    {
        StringBuilder sb = new StringBuilder();
        sb.AppendLine("protected override void OnModelCreating(ModelBuilder modelBuilder)");
        sb.AppendLine("{");

        foreach (CsDbSetDecl set in sets.OrderBy(x => x.ModelName))
        {
            if (!modelsToTablesMap.TryGetValue(set.ModelName.ToLowerInvariant(), out SqlTable? tbl))
            {
                continue;
            }

            if (!modelProperties.TryGetValue(set.ModelName.ToLowerInvariant(), out CsPropertiesInfo? props))
            {
                continue;
            }
            
            CsPropertyInfo? pkCol = props.Properties.FirstOrDefault(x => x.Value.Column?.IsPrimaryKey ?? false).Value;
          
            sb.AppendLine($"modelBuilder.Entity<{set.ModelName}>(entity => {{".Indent());
            sb.AppendLine($"entity.ToTable(\"{tbl.Name}\");".Indent(2));

            if (pkCol is not null)
            {
                sb.AppendLine($"entity.HasKey(e => e.{pkCol.Name});".Indent(2));
            }

            foreach (KeyValuePair<string, CsPropertyInfo> x in props.Properties)
            {
                if (x.Value.Column is not null && x.Value.Name.FirstCharToLower() != x.Value.Column.Name)
                {
                    if (!x.Value.Mapped || !x.Value.CanSet)
                    {
                        continue;
                    }
                    
                    sb.Append($"entity.Property(e => e.{x.Value.Name})".Indent(2));

                    if (x.Value.Column.IsPrimaryKey && !x.Value.Column.IsIdentity)
                    {
                        sb.Append(".ValueGeneratedNever()");
                    }

                    if (x.Value.Column.IsComputed)
                    {
                        sb.Append($".HasComputedColumnSql(\"{x.Value.Column.ComputedSql}\", false)");
                    }

                    sb.Append($".HasColumnName(\"{x.Value.Column.Name}\")");
                    sb.Append(';');
                    sb.AppendLine();
                }
            }
            
            sb.AppendLine("});".Indent());
        }
        
        sb.AppendLine("OnModelCreatingPartial(modelBuilder);");
        sb.AppendLine("}");
        return sb.ToString();
    }

    private MethodDeclarationSyntax GenerateModelMapDecl()
    {
        string code = GenerateModelMap();
        SyntaxTree modelAst = CSharpSyntaxTree.ParseText(code);

        int z = 0;
        
        return (MethodDeclarationSyntax)modelAst.GetRoot().ChildNodes().FirstOrDefault(x => x is MethodDeclarationSyntax)!;
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
        MethodDeclarationSyntax? modelMethod = null;
        
        foreach (MemberDeclarationSyntax memberDecl in node.Members)
        {
            SyntaxToken? ident = memberDecl switch
            {
                PropertyDeclarationSyntax propDecl => propDecl.Identifier,
                FieldDeclarationSyntax fieldDecl => fieldDecl.Declaration.Variables[0].Identifier,
                MethodDeclarationSyntax methodDecl => methodDecl.Identifier,
                _ => null
            };

            switch (memberDecl)
            {
                case ConstructorDeclarationSyntax:
                    beforePropMembers.Add(memberDecl);
                    continue;
                case MethodDeclarationSyntax methodDecl2 when methodDecl2.Identifier.ValueText.Trim() is "OnModelCreating":
                    modelMethod ??= GenerateModelMapDecl();
                    continue;
            }

            CsDbSetDecl? setDecl = DbSetMapper.MemberIsDbSetDecl(memberDecl);

            if (setDecl is null)
            {
                if (ident is not null)
                {
                    if (sets.Any(x => x.SetName.Equals(ident.Value.ValueText.Trim(), StringComparison.InvariantCultureIgnoreCase)))
                    {
                        continue;
                    }
                }
                
                afterPropMembers.Add(memberDecl);   
            }
        }
        
        modelMethod ??= GenerateModelMapDecl();

        propMembers.AddRange(sets.Select(decl => SyntaxFactory.PropertyDeclaration(SyntaxFactory.GenericName(SyntaxFactory.Identifier("DbSet"))
                .WithTypeArgumentList(SyntaxFactory.TypeArgumentList(SyntaxFactory.SingletonSeparatedList<TypeSyntax>(SyntaxFactory.IdentifierName(decl.ModelName)))), SyntaxFactory.Identifier(decl.SetName))
                .WithModifiers(SyntaxFactory.TokenList([SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.VirtualKeyword)]))
                .WithAccessorList(SyntaxFactory.AccessorList(SyntaxFactory.List([SyntaxFactory.AccessorDeclaration(SyntaxKind.GetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)), SyntaxFactory.AccessorDeclaration(SyntaxKind.SetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken))])))));
        
        node = node.WithMembers(SyntaxFactory.List([..beforePropMembers, ..propMembers, modelMethod, ..afterPropMembers]));
        
        NewCode = node.NormalizeWhitespace().ToFullString();
       
        return node;
    }
}