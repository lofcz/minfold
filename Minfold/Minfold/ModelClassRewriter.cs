using System.Data;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.FindSymbols;
using Microsoft.CodeAnalysis.Text;

namespace Minfold;

public enum SqlDbTypeExt
{
    /// <summary>
    /// <see cref="T:System.Int64" />. A 64-bit signed integer.</summary>
    BigInt = 0,
    /// <summary>
    /// <see cref="T:System.Array" /> of type <see cref="T:System.Byte" />. A fixed-length stream of binary data ranging between 1 and 8,000 bytes.</summary>
    Binary = 1,
    /// <summary>
    /// <see cref="T:System.Boolean" />. An unsigned numeric value that can be 0, 1, or <see langword="null" />.</summary>
    Bit = 2,
    /// <summary>
    /// <see cref="T:System.String" />. A fixed-length stream of non-Unicode characters ranging between 1 and 8,000 characters.</summary>
    Char = 3,
    /// <summary>
    /// <see cref="T:System.DateTime" />. Date and time data ranging in value from January 1, 1753 to December 31, 9999 to an accuracy of 3.33 milliseconds.</summary>
    DateTime = 4,
    /// <summary>
    /// <see cref="T:System.Decimal" />. A fixed precision and scale numeric value between -10 38 -1 and 10 38 -1.</summary>
    Decimal = 5,
    /// <summary>
    /// <see cref="T:System.Double" />. A floating point number within the range of -1.79E +308 through 1.79E +308.</summary>
    Float = 6,
    /// <summary>
    /// <see cref="T:System.Array" /> of type <see cref="T:System.Byte" />. A variable-length stream of binary data ranging from 0 to 2 31 -1 (or 2,147,483,647) bytes.</summary>
    Image = 7,
    /// <summary>
    /// <see cref="T:System.Int32" />. A 32-bit signed integer.</summary>
    Int = 8,
    /// <summary>
    /// <see cref="T:System.Decimal" />. A currency value ranging from -2 63 (or -9,223,372,036,854,775,808) to 2 63 -1 (or +9,223,372,036,854,775,807) with an accuracy to a ten-thousandth of a currency unit.</summary>
    Money = 9,
    /// <summary>
    /// <see cref="T:System.String" />. A fixed-length stream of Unicode characters ranging between 1 and 4,000 characters.</summary>
    NChar = 10, // 0x0000000A
    /// <summary>
    /// <see cref="T:System.String" />. A variable-length stream of Unicode data with a maximum length of 2 30 - 1 (or 1,073,741,823) characters.</summary>
    NText = 11, // 0x0000000B
    /// <summary>
    /// <see cref="T:System.String" />. A variable-length stream of Unicode characters ranging between 1 and 4,000 characters. Implicit conversion fails if the string is greater than 4,000 characters. Explicitly set the object when working with strings longer than 4,000 characters. Use <see cref="F:System.Data.SqlDbType.NVarChar" /> when the database column is <see langword="nvarchar(max)" />.</summary>
    NVarChar = 12, // 0x0000000C
    /// <summary>
    /// <see cref="T:System.Single" />. A floating point number within the range of -3.40E +38 through 3.40E +38.</summary>
    Real = 13, // 0x0000000D
    /// <summary>
    /// <see cref="T:System.Guid" />. A globally unique identifier (or GUID).</summary>
    UniqueIdentifier = 14, // 0x0000000E
    /// <summary>
    /// <see cref="T:System.DateTime" />. Date and time data ranging in value from January 1, 1900 to June 6, 2079 to an accuracy of one minute.</summary>
    SmallDateTime = 15, // 0x0000000F
    /// <summary>
    /// <see cref="T:System.Int16" />. A 16-bit signed integer.</summary>
    SmallInt = 16, // 0x00000010
    /// <summary>
    /// <see cref="T:System.Decimal" />. A currency value ranging from -214,748.3648 to +214,748.3647 with an accuracy to a ten-thousandth of a currency unit.</summary>
    SmallMoney = 17, // 0x00000011
    /// <summary>
    /// <see cref="T:System.String" />. A variable-length stream of non-Unicode data with a maximum length of 2 31 -1 (or 2,147,483,647) characters.</summary>
    Text = 18, // 0x00000012
    /// <summary>
    /// <see cref="T:System.Array" /> of type <see cref="T:System.Byte" />. Automatically generated binary numbers, which are guaranteed to be unique within a database. <see langword="timestamp" /> is used typically as a mechanism for version-stamping table rows. The storage size is 8 bytes.</summary>
    Timestamp = 19, // 0x00000013
    /// <summary>
    /// <see cref="T:System.Byte" />. An 8-bit unsigned integer.</summary>
    TinyInt = 20, // 0x00000014
    /// <summary>
    /// <see cref="T:System.Array" /> of type <see cref="T:System.Byte" />. A variable-length stream of binary data ranging between 1 and 8,000 bytes. Implicit conversion fails if the byte array is greater than 8,000 bytes. Explicitly set the object when working with byte arrays larger than 8,000 bytes.</summary>
    VarBinary = 21, // 0x00000015
    /// <summary>
    /// <see cref="T:System.String" />. A variable-length stream of non-Unicode characters ranging between 1 and 8,000 characters. Use <see cref="F:System.Data.SqlDbType.VarChar" /> when the database column is <see langword="varchar(max)" />.</summary>
    VarChar = 22, // 0x00000016
    /// <summary>
    /// <see cref="T:System.Object" />. A special data type that can contain numeric, string, binary, or date data as well as the SQL Server values Empty and Null, which is assumed if no other type is declared.</summary>
    Variant = 23, // 0x00000017
    /// <summary>An XML value. Obtain the XML as a string using the <see cref="M:System.Data.SqlClient.SqlDataReader.GetValue(System.Int32)" /> method or <see cref="P:System.Data.SqlTypes.SqlXml.Value" /> property, or as an <see cref="T:System.Xml.XmlReader" /> by calling the <see cref="M:System.Data.SqlTypes.SqlXml.CreateReader" /> method.</summary>
    Xml = 25, // 0x00000019
    /// <summary>A SQL Server user-defined type (UDT).</summary>
    Udt = 29, // 0x0000001D
    /// <summary>A special data type for specifying structured data contained in table-valued parameters.</summary>
    Structured = 30, // 0x0000001E
    /// <summary>Date data ranging in value from January 1,1 AD through December 31, 9999 AD.</summary>
    Date = 31, // 0x0000001F
    /// <summary>Time data based on a 24-hour clock. Time value range is 00:00:00 through 23:59:59.9999999 with an accuracy of 100 nanoseconds. Corresponds to a SQL Server <see langword="time" /> value.</summary>
    Time = 32, // 0x00000020
    /// <summary>Date and time data. Date value range is from January 1,1 AD through December 31, 9999 AD. Time value range is 00:00:00 through 23:59:59.9999999 with an accuracy of 100 nanoseconds.</summary>
    DateTime2 = 33, // 0x00000021
    /// <summary>Date and time data with time zone awareness. Date value range is from January 1,1 AD through December 31, 9999 AD. Time value range is 00:00:00 through 23:59:59.9999999 with an accuracy of 100 nanoseconds. Time zone value range is -14:00 through +14:00.</summary>
    DateTimeOffset = 34, // 0x00000022,
    CsIdentifier,
    Unknown
}

public record CsPropertyDecl(string Name, SqlDbTypeExt Type, bool Nullable, List<SqlForeignKey> FkForeignKeys, string? Token);
public record CsForeignKey(string? Target, bool? Enforced);
public record CsPropertyInfo(string Name, bool Mapped, List<CsForeignKey> ForeignKeys, ColumnDefaultVal? DefaultVal, CsPropertyDecl Decl);

public class CsPropertiesInfo
{
    public Dictionary<string, CsPropertyInfo> Properties { get; set; } = new Dictionary<string, CsPropertyInfo>();
}

public class CsPropertyDeclPatch(CsPropertyDecl property, bool solved, bool mapped)
{
    public CsPropertyDecl Property { get; set; } = property;
    public bool Solved { get; set; } = solved;
    public bool Mapped { get; set; } = mapped;
}

public enum ModelActionType
{
    EmptyCtor,
    ModelCtor
}

public record ModelActionsPatch(List<ModelActionType> Actions);
public record ModelPropertiesPatch(List<CsPropertyDecl> PropertiesAdd, List<string> PropertiesRemove, List<CsPropertyDecl> PropertiesUpdate, CsPropertiesInfo PropertiesInfo);
public record ModelForeignKeysPatch(Dictionary<string, CsPropertyFkDecl> PropertiesUpdate);
public record CsPropertyFkDecl(string Name, List<SqlForeignKey> ForeignKeys);

public class ModelClassRewriter : CSharpSyntaxRewriter
{
    public string NewCode { get; set; }
    public bool ClassRewritten { get; set; }
 
    private SqlTable table;
    private string expectedClassName;
    private readonly Dictionary<string, CsModelSource> tablesMap;
    private readonly CsModelSource modelSource;
    
    /// <summary>
    /// Updates a model
    /// </summary>
    public ModelClassRewriter(string expectedClassName, SqlTable table, Dictionary<string, CsModelSource> tablesMap, CsModelSource modelSource)
    {
        this.table = table;
        this.expectedClassName = expectedClassName;
        this.tablesMap = tablesMap;
        this.modelSource = modelSource;
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

    private CsPropertiesInfo Properties(ClassDeclarationSyntax node)
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

                if (table.Columns.TryGetValue(propDecl.Identifier.ValueText.ToLowerInvariant(), out SqlTableColumn? col))
                {
                    tableColumn = col;
                }

                info.Properties.TryAdd(propDecl.Identifier.ValueText.ToLowerInvariant(), new CsPropertyInfo(propDecl.Identifier.ValueText, mapped, keys, Minfold.ColumnDefaultValue(propDecl.Identifier.ValueText.ToLowerInvariant(), tableColumn), propDecl.ToPropertyDecl()));   
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
        if (node.Identifier.ValueText.ToLowerInvariant().Trim() is "form")
        {
            int z = 0;
        }
        
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
        CsPropertiesInfo properties = Properties(node);
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

        if (node.Identifier.ValueText.ToLowerInvariant() is "llmconfig")
        {
            int z = 0;
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
    
    public override SyntaxNode VisitClassDeclaration(ClassDeclarationSyntax node)
    {
        if (node.Identifier.ValueText != expectedClassName)
        {
            return node;
        }

        ClassRewritten = true;

        CsPropertiesInfo properties = Properties(node);
        ModelActionsPatch actionsPatch = ComputeActionsPatch(node, properties);

        node = EnsureEmptyCtor(node);
        
        ModelPropertiesPatch patch = ComputePatch(node, properties);
        node = RemovePropertiesShrinkCtor(node, patch.PropertiesRemove);
        node = ChangePropertiesTypeUpdateCtor(node, patch.PropertiesUpdate);   
        node = AddPropertiesExtendCtor(node, patch.PropertiesAdd);
        
        ModelForeignKeysPatch fksPatch = ComputeFksPatch(node);
        node = UpdateForeignKeys(node, fksPatch);
        
        node = EnsureNonEmptyCtor(node, properties);
        
        NewCode = node.NormalizeWhitespace().ToFullString();
        
        return node;
    }
}