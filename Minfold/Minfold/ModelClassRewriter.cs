using System.Data;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.FindSymbols;
using Microsoft.CodeAnalysis.Text;
namespace Minfold;

public class ModelClassRewriter : CSharpSyntaxRewriter
{
    public string NewCode { get; set; }
    public bool ClassRewritten { get; set; }
    public string? Namespace { get; set; }
    public CsPropertiesInfo? PropertiesMap { get; set; }
 
    private readonly SqlTable table;
    private readonly string expectedClassName;
    private readonly Dictionary<string, CsModelSource> tablesMap;
    private readonly CsModelSource modelSource;
    private readonly bool scanNamespace;
    private readonly CompilationUnitSyntax rootNode;
    
    /// <summary>
    /// Updates a model
    /// </summary>
    public ModelClassRewriter(string expectedClassName, SqlTable table, Dictionary<string, CsModelSource> tablesMap, CsModelSource modelSource, bool scanNamespace, CompilationUnitSyntax rootNode)
    {
        this.table = table;
        this.expectedClassName = expectedClassName;
        this.tablesMap = tablesMap;
        this.modelSource = modelSource;
        this.scanNamespace = scanNamespace;
        this.rootNode = rootNode;
    }
    
    private static ClassDeclarationSyntax RemoveProperties(ClassDeclarationSyntax node, string[] properties)
    {
        List<MemberDeclarationSyntax> keptMembers = [];
        
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

    private static HashSet<string> ForeignKeyAttributes = [ "ReferenceKey", "ReferenceKeyAttribute" ];

    private static ClassDeclarationSyntax UpdateProperty(ClassDeclarationSyntax node, CsPropertyDecl property)
    {
        MemberDeclarationSyntax? existingProp = node.Members.FirstOrDefault(x => x is PropertyDeclarationSyntax propSyntax2 && propSyntax2.Identifier.ValueText == property.Name);

        if (existingProp is not PropertyDeclarationSyntax propSyntax)
        {
            return node;
        }

        propSyntax = propSyntax.WithIdentifier(SyntaxFactory.Identifier(property.Name)).WithType(property.Type.ToTypeSyntax(property.Nullable));

        List<AttributeListSyntax> keptAttrLists = [];
        
        foreach (AttributeListSyntax aList in propSyntax.AttributeLists)
        {
            List<AttributeSyntax> keptAttrs = [];

            foreach (AttributeSyntax attr in aList.Attributes)
            {
                if (attr.Name is IdentifierNameSyntax ident && ForeignKeyAttributes.Contains(ident.Identifier.ValueText))
                {
                    continue;
                }

                keptAttrs.Add(attr);
            }

            if (keptAttrs.Count > 0)
            {
                keptAttrLists.Add(aList.WithAttributes(SyntaxFactory.SeparatedList(keptAttrs)));   
            }
        }
        
        propSyntax = propSyntax.WithAttributeLists(SyntaxFactory.List(keptAttrLists));
        
        node = node.ReplaceNode(existingProp, propSyntax);
        return node;
    }

    public static CsPropertiesInfo Properties(ClassDeclarationSyntax node, SqlTable table, CompilationUnitSyntax rootNode)
    {
        CsPropertiesInfo info = new CsPropertiesInfo();
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

        for (int i = 0; i < node.Members.Count; i++)
        {
            if (node.Members[i] is PropertyDeclarationSyntax propDecl)
            {
                bool mapped = i < lastIndex;

                if (mapped)
                {
                    mapped = !propDecl.AttributeLists.Any(x => x.Attributes.Any(y => y.Name is IdentifierNameSyntax ident && NotMappedAttributes.Contains(ident.Identifier.ValueText)));
                }

                List<CsForeignKey> keys = [];
                
                foreach (AttributeSyntax attr in propDecl.AttributeLists.SelectMany(aList => aList.Attributes))
                {
                    if (attr.Name is IdentifierNameSyntax ident && ForeignKeyAttributes.Contains(ident.Identifier.ValueText))
                    {
                        if (attr.ArgumentList is not null)
                        {
                            string? refIdentVal = null;
                            bool? enforced = null;
                            
                            for (int j = 0; j < attr.ArgumentList.Arguments.Count; j++)
                            {
                                switch (j)
                                {
                                    case 0 when attr.ArgumentList.Arguments[j].Expression is TypeOfExpressionSyntax { Type: IdentifierNameSyntax refIdent }:
                                        refIdentVal = refIdent.Identifier.ValueText;
                                        break;
                                    case 1 when attr.ArgumentList.Arguments[j].Expression is LiteralExpressionSyntax literalExpr:
                                    {
                                        if (literalExpr.Token.IsKind(SyntaxKind.TrueKeyword))
                                        {
                                            enforced = true;
                                        }
                                        else if (literalExpr.Token.IsKind(SyntaxKind.FalseKeyword))
                                        {
                                            enforced = false;
                                        }

                                        break;
                                    }
                                }
                            }
                            
                            keys.Add(new CsForeignKey(refIdentVal, enforced));
                        }
                    }
                }

                SqlTableColumn? tableColumn = null;
                CsPropertyDecl propDeclInfo = propDecl.ToPropertyDecl();
                CsTypeAlias? alias = null;
                
                if (table.Columns.TryGetValue(propDecl.Identifier.ValueText.ToLowerInvariant(), out SqlTableColumn? col))
                {
                    tableColumn = col;

                    if (propDeclInfo.Type is SqlDbTypeExt.CsIdentifier && tableColumn.SqlType is not SqlDbTypeExt.CsIdentifier)
                    {
                        Dictionary<string, string>? usings = GetUsings(rootNode);
                        alias = new CsTypeAlias(propDeclInfo.Token ?? string.Empty, usings);
                    }
                }

                bool canSet = true;
                bool anySet = false;
                
                if (propDecl.ExpressionBody is not null)
                {
                    canSet = false;
                }
                else if (propDecl.AccessorList is null)
                {
                    canSet = false;
                }
                else
                {
                    foreach (AccessorDeclarationSyntax accessor in propDecl.AccessorList.Accessors)
                    {
                        if (!accessor.Keyword.IsKind(SyntaxKind.SetKeyword))
                        {
                            continue;
                        }

                        anySet = true;

                        if (accessor.Modifiers.Any(x => x.IsKind(SyntaxKind.PrivateKeyword)))
                        {
                            canSet = false;
                        }
                    }
                }

                if (canSet)
                {
                    canSet = anySet;
                }
                
                info.Properties.TryAdd(propDecl.Identifier.ValueText.ToLowerInvariant(), new CsPropertyInfo(propDecl.Identifier.ValueText, mapped, keys, Minfold.ColumnDefaultValue(propDecl.Identifier.ValueText.ToLowerInvariant(), tableColumn), propDecl.ToPropertyDecl(), alias, tableColumn, canSet));   
            }
        }

        return info;
        
        int FindFirstDynamicComment()
        {
            foreach (MemberDeclarationSyntax tNode in node.Members.Where(tNode => tNode.DescendantTrivia().Where(y => y.IsKind(SyntaxKind.SingleLineCommentTrivia) || y.IsKind(SyntaxKind.MultiLineCommentTrivia)).Any(trivia => (trivia.IsKind(SyntaxKind.MultiLineCommentTrivia) && Regexes.MultilineCommentDynamic.IsMatch(trivia.ToFullString())) || (trivia.IsKind(SyntaxKind.SingleLineCommentTrivia) && Regexes.SinglelineCommentDynamic.IsMatch(trivia.ToFullString())))))
            {
                return node.Members.IndexOf(tNode);
            }

            return -1;
        }
    }
    
    private static List<string> GetUsingsInner(CompilationUnitSyntax rootNode)
    {
        List<string> usings = [];
        usings.AddRange(rootNode.Usings.Select(n => n.NamespaceOrType.ToFullString().Trim()));

        return usings;
    }

    private static Dictionary<string, string>? GetUsings(CompilationUnitSyntax node)
    {
        List<string> usings = GetUsingsInner(node);

        if (usings.Count is 0)
        {
            return null;
        }
        
        Dictionary<string, string> parsed = [];

        foreach (string str in usings)
        {
            parsed.TryAdd(str.Trim().Replace(" ", "").Replace(";", ""), str);
        }
        
        return parsed;
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

    private static ConstructorDeclarationSyntax? GetCtorNode(ClassDeclarationSyntax node)
    {
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

            return ctorNode;
        }

        return null;
    }
    
    private ClassDeclarationSyntax UpdateConstructorProperty(ClassDeclarationSyntax node, CsPropertyDecl property)
    {
        bool ctorUpdated = false;
        ConstructorDeclarationSyntax? ctor = GetCtorNode(node);

        if (ctor is null)
        {
            return node;
        }

        ParameterSyntax? param = ctor.ParameterList.Parameters.FirstOrDefault(x => x.Identifier.ValueText == property.Name.FirstCharToLower());
        ParameterSyntax? paramReplace = param?.WithType(property.Type.ToTypeSyntax(property.Nullable));

        if (param is not null && paramReplace is not null)
        {
            ConstructorDeclarationSyntax ctorReplace = ctor.ReplaceNode(param, paramReplace);
            node = node.ReplaceNode(ctor, ctorReplace);
        }
        
        return node;
    }

    private static ClassDeclarationSyntax ExtendConstructor(ClassDeclarationSyntax node, CsPropertyDecl[] properties)
    {
        bool ctorUpdated = false;

        void DoExtendConstructor(MemberDeclarationSyntax mNode, ConstructorDeclarationSyntax ctorNode)
        {
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
        }

        void TryExtendCtor(ConstructorDeclarationSyntax? ctorNode)
        {
            if (ctorNode is null)
            {
                foreach (MemberDeclarationSyntax mNode in node.Members.Where(x => x.IsKind(SyntaxKind.ConstructorDeclaration)))
                {
                    if (mNode is not ConstructorDeclarationSyntax ctorNode2)
                    {
                        continue;
                    }

                    if (ctorNode2.ParameterList.Parameters.Count is 0)
                    {
                        continue;
                    }

                    ctorNode = ctorNode2;
                    break;
                }   
            }

            if (ctorNode is not null)
            {
                DoExtendConstructor(ctorNode, ctorNode);
                ctorUpdated = true;
            }
        }

        TryExtendCtor(null);
        
        if (!ctorUpdated)
        {
            MemberDeclarationSyntax? emptyCtorNode = node.Members.FirstOrDefault(x => x.IsKind(SyntaxKind.ConstructorDeclaration));
            int insertIndex = 0;

            if (emptyCtorNode is not null)
            {
                insertIndex = node.Members.IndexOf(emptyCtorNode) + 1;
            }

            ConstructorDeclarationSyntax newCtor = SyntaxFactory.ConstructorDeclaration(node.Identifier.ValueText)
                .WithBody(SyntaxFactory.Block())
                .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword));
            
            SyntaxList<MemberDeclarationSyntax> newMembers = node.Members.Insert(insertIndex, newCtor);
            node = node.WithMembers(newMembers);
            TryExtendCtor(newCtor);
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

            List<ParameterSyntax> keptParameters = [];

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
                List<StatementSyntax> keptStmts = [];
                
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

    private ClassDeclarationSyntax UpdateForeignKeys(ClassDeclarationSyntax node, ModelForeignKeysPatch patch)
    {
        List<MemberDeclarationSyntax> members = [];
        
        foreach (MemberDeclarationSyntax member in node.Members)
        {
            if (member is not PropertyDeclarationSyntax propDecl)
            {
                members.Add(member);
                continue;
            }

            if (!patch.PropertiesUpdate.TryGetValue(propDecl.Identifier.ValueText, out CsPropertyFkDecl? patchProp))
            {
                members.Add(member);
                continue;
            }

            List<AttributeListSyntax> keptAttrLists = [];
        
            foreach (AttributeListSyntax aList in propDecl.AttributeLists)
            {
                List<AttributeSyntax> keptAttrs = [];

                foreach (AttributeSyntax attr in aList.Attributes)
                {
                    switch (attr.Name)
                    {
                        case IdentifierNameSyntax ident when ForeignKeyAttributes.Contains(ident.Identifier.ValueText):
                        case GenericNameSyntax genIdent when ForeignKeyAttributes.Contains(genIdent.Identifier.ValueText):
                            continue;
                        default:
                            keptAttrs.Add(attr);
                            break;
                    }
                }

                if (keptAttrs.Count > 0)
                {
                    keptAttrLists.Add(aList.WithAttributes(SyntaxFactory.SeparatedList(keptAttrs)));   
                }
            }

            foreach (SqlForeignKey fk in patchProp.ForeignKeys)
            {
                string modelName = fk.RefTable;
                string columnName = fk.RefColumn.FirstCharToUpper() ?? string.Empty;
                bool selfRef = false;
                
                if (tablesMap.TryGetValue(fk.RefTable.ToLowerInvariant(), out CsModelSource? str))
                {
                    if (string.Equals(str.Name, expectedClassName, StringComparison.InvariantCultureIgnoreCase))
                    {
                        selfRef = true;
                    }
                    
                    modelName = str.Name;

                    // currently we are single-passing both props and FKs, if we generated the column before this will fail but the newly generated column is named correctly
                    if (str.Columns.TryGetValue(columnName.ToLowerInvariant(), out string? existingColumn))
                    {
                        columnName = existingColumn;
                    }
                }

                ExpressionSyntax expr = selfRef ? SyntaxFactory.IdentifierName(SyntaxFactory.Identifier(columnName)) : SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression, SyntaxFactory.IdentifierName(modelName), SyntaxFactory.IdentifierName(columnName));
                
                keptAttrLists.Add(SyntaxFactory.AttributeList(SyntaxFactory.SeparatedList(new []
                {
                    SyntaxFactory.Attribute(SyntaxFactory.IdentifierName("ReferenceKey"), SyntaxFactory.AttributeArgumentList(SyntaxFactory.SeparatedList(new []
                    {
                        SyntaxFactory.AttributeArgument(SyntaxFactory.TypeOfExpression(SyntaxFactory.IdentifierName(modelName))),
                        SyntaxFactory.AttributeArgument(SyntaxFactory.InvocationExpression(SyntaxFactory.IdentifierName(SyntaxFactory.Identifier("nameof")), SyntaxFactory.ArgumentList(
                            SyntaxFactory.SeparatedList(new []
                            {
                                SyntaxFactory.Argument(expr)
                            })
                        ))),
                        SyntaxFactory.AttributeArgument(SyntaxFactory.LiteralExpression(fk.NotEnforced ? SyntaxKind.FalseLiteralExpression : SyntaxKind.TrueLiteralExpression))
                    })))
                })));
            }

            PropertyDeclarationSyntax propDeclNew = propDecl.WithAttributeLists(SyntaxFactory.List(keptAttrLists));
            members.Add(propDeclNew);
        }

        node = node.WithMembers(SyntaxFactory.List(members));
        return node;
    }

    private ClassDeclarationSyntax AddPropertiesExtendCtor(ClassDeclarationSyntax node, IEnumerable<CsPropertyDecl> properties)
    {
        CsPropertyDecl[] propertiesArr = properties.ToArray();
        
        if (propertiesArr.Length is 0)
        {
            return node;
        }
        
        node = AddProperties(node, propertiesArr);
        //node = ExtendConstructor(node, propertiesArr);
        return node;
    }

    private ClassDeclarationSyntax RemovePropertiesShrinkCtor(ClassDeclarationSyntax node, IEnumerable<string> properties)
    {
        string[] propertiesArr = properties.ToArray();

        if (propertiesArr.Length is 0)
        {
            return node;
        }
        
        node = RemoveProperties(node, propertiesArr);
        //node = ShrinkConstructor(node, propertiesArr);
        return node;
    }

    private ClassDeclarationSyntax ChangePropertiesTypeUpdateCtor(ClassDeclarationSyntax node, IEnumerable<CsPropertyDecl> properties)
    {
        CsPropertyDecl[] propertiesArr = properties.ToArray();
        
        if (propertiesArr.Length is 0)
        {
            return node;
        }
        
        node = propertiesArr.Aggregate(node, UpdateProperty);
        //node = propertiesArr.Aggregate(node, UpdateConstructorProperty);
        return node;
    }

    private static HashSet<string> NotMappedAttributes = ["NotMapped", "NotMappedAttribute"];

    private ModelForeignKeysPatch ComputeFksPatch(ClassDeclarationSyntax node)
    {
        CsPropertiesInfo properties = Properties(node, table, rootNode);
        ModelForeignKeysPatch patch = new ModelForeignKeysPatch([]);
        
        foreach (MemberDeclarationSyntax member in node.Members)
        {
            if (member is not PropertyDeclarationSyntax propDecl)
            {
                continue;
            }

            if (!properties.Properties.TryGetValue(propDecl.Identifier.ValueText.ToLowerInvariant(), out CsPropertyInfo? propInfo))
            {
                continue;
            }

            if (!propInfo.Mapped)
            {
                continue;
            }
            
            if (!table.Columns.TryGetValue(propDecl.Identifier.ValueText.ToLowerInvariant(), out SqlTableColumn? column))
            {
                continue;
            }

            if (column.ForeignKeys.Count > 0)
            {
                int zz = 0;
            }

            patch.PropertiesUpdate.TryAdd(propDecl.Identifier.ValueText, new CsPropertyFkDecl(propDecl.Identifier.ValueText, column.ForeignKeys));
        }

        return patch;
    }

    private ModelActionsPatch ComputeActionsPatch(ClassDeclarationSyntax node, CsPropertiesInfo properties)
    {
        ModelActionsPatch patch = new ModelActionsPatch([]);
        List<ConstructorDeclarationSyntax> ctors = GetCtors(node);

        if (!ctors.Any(x => x.ParameterList.Parameters.Count is 0 && x.Modifiers.Any(y => y.IsKind(SyntaxKind.PublicKeyword)) && !x.Modifiers.Any(y => y.IsKind(SyntaxKind.StaticKeyword))))
        {
            patch.Actions.Add(ModelActionType.EmptyCtor);
        }

        if (properties.Properties.Count > 0 && !ctors.Any(x => x.ParameterList.Parameters.Count > 0 && x.Modifiers.Any(y => y.IsKind(SyntaxKind.PublicKeyword)) && !x.Modifiers.Any(y => y.IsKind(SyntaxKind.StaticKeyword))))
        {
            patch.Actions.Add(ModelActionType.ModelCtor);
        }

        return patch;
    }
    
    private ModelPropertiesPatch ComputePatch(ClassDeclarationSyntax node, CsPropertiesInfo properties)
    {
        List<CsPropertyDeclPatch> decls = [];
        ModelPropertiesPatch patch = new ModelPropertiesPatch([], [], [], properties);
        
        foreach (MemberDeclarationSyntax member in node.Members)
        {
            if (member is not PropertyDeclarationSyntax propDecl)
            {
                continue;
            }
            
            if (properties.Properties.TryGetValue(propDecl.Identifier.ValueText.ToLowerInvariant(), out CsPropertyInfo? prop))
            {
                decls.Add(new CsPropertyDeclPatch(propDecl.ToPropertyDecl(), false, prop.Mapped));   
            }
        }

        foreach (SqlTableColumn column in table.Columns.Values)
        {
            CsPropertyDeclPatch? prop = decls.FirstOrDefault(x => string.Equals(x.Property.Name, column.Name, StringComparison.InvariantCultureIgnoreCase));

            if (prop is null)
            {
                patch.PropertiesAdd.Add(new CsPropertyDecl(column.Name.FirstCharToUpper(), column.SqlType, column.IsNullable, column.ForeignKeys, null));
                continue;
            }

            if (prop.Mapped && (!prop.Property.Type.CsEqual(column.SqlType) || prop.Property.Nullable != column.IsNullable))
            {
                // skip enums (will trip on classes too as we are not using semantic model)
                if (prop.Property.Type is SqlDbTypeExt.CsIdentifier && column.SqlType is SqlDbTypeExt.Int or SqlDbTypeExt.SmallInt or SqlDbTypeExt.TinyInt)
                {
                    prop.Solved = true;
                    continue;
                }
                
                patch.PropertiesUpdate.Add(new CsPropertyDecl(prop.Property.Name, column.SqlType, column.IsNullable, column.ForeignKeys, null));
                prop.Solved = true;
                continue;
            }

            prop.Solved = true;
        }

        foreach (CsPropertyDeclPatch currentDecl in decls.Where(x => x is { Solved: false, Mapped: true }))
        {
            patch.PropertiesRemove.Add(currentDecl.Property.Name);
        }
        
        return patch;
    }
    
    private static List<ConstructorDeclarationSyntax> GetCtors(ClassDeclarationSyntax node)
    {
        List<ConstructorDeclarationSyntax> ctors = [];

        foreach (MemberDeclarationSyntax mNode in node.Members.Where(x => x.IsKind(SyntaxKind.ConstructorDeclaration)))
        {
            if (mNode is not ConstructorDeclarationSyntax ctorNode)
            {
                continue;
            }

            ctors.Add(ctorNode);
        }

        return ctors;
    }

    private ClassDeclarationSyntax EnsureNonEmptyCtor(ClassDeclarationSyntax node, CsPropertiesInfo properties)
    {
        List<ConstructorDeclarationSyntax> ctors = GetCtors(node);

        int insertIndex = -1;
        
        ConstructorDeclarationSyntax? nonEmptyCtor = ctors.FirstOrDefault(x => x.ParameterList.Parameters.Count > 0 && !x.Modifiers.Any(y => y.IsKind(SyntaxKind.StaticKeyword)));

        if (nonEmptyCtor is not null)
        {
            node = node.WithMembers(node.Members.Remove(nonEmptyCtor));
            ctors = GetCtors(node);
        }
        
        if (ctors.Count > 0)
        {
            ConstructorDeclarationSyntax? emptyCtor = ctors.FirstOrDefault(x => x.ParameterList.Parameters.Count is 0 && !x.Modifiers.Any(y => y.IsKind(SyntaxKind.StaticKeyword)));

            if (emptyCtor is not null)
            {
                insertIndex = node.Members.IndexOf(emptyCtor) + 1;   
            }
        }

        if (insertIndex is -1)
        {
            insertIndex = node.Members.Count;
        }

        string? ctortext = Minfold.DumpCtor(node.Identifier.ValueText, table, properties);

        if (ctortext is null)
        {
            return node;
        }

        MemberDeclarationSyntax? mmbr = SyntaxFactory.ParseMemberDeclaration(ctortext);

        if (mmbr is not null)
        {
            node = node.WithMembers(node.Members.Insert(insertIndex, mmbr));
        }
        
        string text = node.NormalizeWhitespace().ToFullString();
        
        return node;
    }
    
    private ClassDeclarationSyntax EnsureEmptyCtor(ClassDeclarationSyntax node)
    {
        List<ConstructorDeclarationSyntax> ctors = GetCtors(node);

        if (!ctors.Any(x => x.ParameterList.Parameters.Count is 0 && x.Modifiers.Any(y => y.IsKind(SyntaxKind.PublicKeyword)) && !x.Modifiers.Any(y => y.IsKind(SyntaxKind.StaticKeyword))))
        {
            MemberDeclarationSyntax? appendAfter = null;
            
            foreach (MemberDeclarationSyntax x in node.Members)
            {
                if (x is ConstructorDeclarationSyntax)
                {
                    break;
                }

                appendAfter = x;
            }

            if (appendAfter is not null)
            {
                SyntaxList<MemberDeclarationSyntax> newMembers = node.Members.Insert(node.Members.IndexOf(appendAfter) + 1, SyntaxFactory.ConstructorDeclaration(node.Identifier.ValueText)
                    .WithBody(SyntaxFactory.Block())
                    .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
                );
                node = node.WithMembers(newMembers);
            }
        }

        return node;
    }

    private void ScanNamespace(SyntaxNode node)
    {
        while (node.Parent is not null)
        {
            NamespaceDeclarationSyntax? namespaceDecl = (NamespaceDeclarationSyntax?)node.Parent.ChildNodes().FirstOrDefault(x => x is NamespaceDeclarationSyntax);

            if (namespaceDecl is not null)
            {
                Namespace = namespaceDecl.Name.ToFullString();
                break;
            }

            node = node.Parent;
        }
    }
    
    public override SyntaxNode VisitClassDeclaration(ClassDeclarationSyntax node)
    {
        if (node.Identifier.ValueText != expectedClassName)
        {
            return node;
        }

        if (scanNamespace)
        {
            ScanNamespace(node);
        }

        ClassRewritten = true;

        CsPropertiesInfo properties = Properties(node, table, rootNode);
        ModelActionsPatch actionsPatch = ComputeActionsPatch(node, properties);

        node = EnsureEmptyCtor(node);
        
        ModelPropertiesPatch patch = ComputePatch(node, properties);
        node = RemovePropertiesShrinkCtor(node, patch.PropertiesRemove);
        node = ChangePropertiesTypeUpdateCtor(node, patch.PropertiesUpdate);   
        node = AddPropertiesExtendCtor(node, patch.PropertiesAdd);
        
        ModelForeignKeysPatch fksPatch = ComputeFksPatch(node);
        node = UpdateForeignKeys(node, fksPatch);
        
        node = EnsureNonEmptyCtor(node, properties);

        PropertiesMap = Properties(node, table, rootNode);
        NewCode = node.NormalizeWhitespace().ToFullString();
        
        return node;
    }
}