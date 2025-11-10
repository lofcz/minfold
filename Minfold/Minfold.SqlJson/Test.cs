using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace Minfold.SqlJson;

public class ForJsonVisitor : TSqlFragmentVisitor
{
    public List<SelectStatement> InnerSelectStatements { get; } = new List<SelectStatement>();

    public override void Visit(SelectStatement node)
    {
        base.Visit(node);
        if (node.QueryExpression.ForClause != null && node.QueryExpression.ForClause is JsonForClause jfc)
        {
            var innerSelect = GetInnerSelectStatement(node);
            if(innerSelect != null){
                InnerSelectStatements.Add(innerSelect);
            }
        }
    }

    private SelectStatement? GetInnerSelectStatement(SelectStatement statement)
    {
        if (statement.QueryExpression is QuerySpecification querySpecification)
        {
            if(querySpecification.SelectElements != null)
                foreach(var element in querySpecification.SelectElements){
                    if (element is SelectScalarExpression selectScalarExpression)
                    {
                        if(selectScalarExpression.Expression is ScalarSubquery subquery)
                        {

                            return null;

                            //return subquery.QueryExpression as SelectStatement;
                        }
                    }
                }
        }
        return null;
    }
}

public class SqlToCteTransformer
{
    private class SubqueryVisitor : TSqlFragmentVisitor
    {
        private int cteCounter = 0;
        private readonly List<CommonTableExpression> ctes = new List<CommonTableExpression>();
        private readonly Stack<QuerySpecification> queryStack = new Stack<QuerySpecification>();
        private SelectStatement selectStatement;

        public IReadOnlyList<CommonTableExpression> Ctes => ctes;
        public SelectStatement SelectStatement => selectStatement;

        public override void Visit(QuerySpecification node)
        {
            queryStack.Push(node);
            base.Visit(node);
            queryStack.Pop();
        }

        public override void Visit(ScalarSubquery node)
        {
            if (queryStack.Count == 0) return;
            
            var currentQuery = queryStack.Peek();
            var cteName = $"CTE_{++cteCounter}";
            
            // Vytvoř nové CTE
            var cte = new CommonTableExpression
            {
                ExpressionName = new Identifier { Value = cteName },
                QueryExpression = node.QueryExpression
            };
            
            ctes.Add(cte);

            // Nahraď subquery referencí na CTE
            var columnReference = new ColumnReferenceExpression
            {
                // Správná inicializace MultiPartIdentifier
                MultiPartIdentifier = new MultiPartIdentifier
                {
                    Identifiers =
                    {
                        new Identifier { Value = cteName }
                    }
                }
            };


            // Najdi a nahraď subquery v parent query
            ReplaceSubquery(currentQuery, node, columnReference);
        }

        private void ReplaceSubquery(QuerySpecification parent, ScalarSubquery oldNode, ColumnReferenceExpression newNode)
        {
            // Nahrazení v SELECT části
            for (int i = 0; i < parent.SelectElements.Count; i++)
            {
                if (parent.SelectElements[i] is SelectScalarExpression selectExpr &&
                    ContainsSubquery(selectExpr.Expression, oldNode))
                {
                    selectExpr.Expression = newNode;
                }
            }
        }

        private bool ContainsSubquery(ScalarExpression expression, ScalarSubquery subquery)
        {
            return expression == subquery;
        }
    }

    public string Transform(string sql)
    {
        // Parser
        var parser = new TSql160Parser(false);
        using var reader = new StringReader(sql);
        var tree = parser.Parse(reader, out var errors);

        if (errors.Count > 0)
        {
            throw new Exception($"SQL parsing failed: {string.Join(", ", errors.Select(e => e.Message))}");
        }

        // Visitor pro nalezení a extrakci subqueries
        var visitor = new SubqueryVisitor();
        tree.Accept(visitor);

        // Pokud nejsou žádné CTE, vrať původní SQL
        if (!visitor.Ctes.Any())
            return sql;

        // Generátor pro vytvoření nového SQL
        var generator = new Sql150ScriptGenerator(new SqlScriptGeneratorOptions
        {
            KeywordCasing = KeywordCasing.Uppercase,
            IncludeSemicolons = true
        });

        // Vytvoř nový SELECT statement s WITH
        var selectStatement = visitor.SelectStatement;
        if (selectStatement == null)
            throw new Exception("Not a SELECT statement");
        
        var withClause = new WithCtesAndXmlNamespaces();

        foreach (CommonTableExpression cte in visitor.Ctes)
        {
            withClause.CommonTableExpressions.Add(cte);   
        }

        var newSelect = new SelectStatement
        {
            WithCtesAndXmlNamespaces = withClause,
            QueryExpression = selectStatement.QueryExpression
        };

        generator.GenerateScript(newSelect, out var result);
        return result;
    }
    
    class SubqueryVisitor2 : TSqlFragmentVisitor
    {
        private int cteCounter = 0;
        private readonly List<CommonTableExpression> ctes = new List<CommonTableExpression>();
        private readonly Stack<QuerySpecification> queryStack = new Stack<QuerySpecification>();
        private SelectStatement mainSelect;

        public IReadOnlyList<CommonTableExpression> Ctes => ctes;
        public SelectStatement MainSelect => mainSelect;

        public override void Visit(SelectStatement node)
        {
            mainSelect = node;
            base.Visit(node);
        }

        public override void Visit(QuerySpecification node)
        {
            queryStack.Push(node);
            base.Visit(node);
            queryStack.Pop();
        }

        public override void Visit(ScalarSubquery node)
        {
            if (queryStack.Count == 0) return;
            
            var currentQuery = queryStack.Peek();
            var cteName = $"CTE_{++cteCounter}";
            
            var cte = new CommonTableExpression
            {
                ExpressionName = new Identifier { Value = cteName },
                QueryExpression = node.QueryExpression
            };
            
            ctes.Add(cte);

            var columnReference = new ColumnReferenceExpression
            {
                MultiPartIdentifier = new MultiPartIdentifier
                {
                    Identifiers = 
                    {
                        new Identifier { Value = cteName }
                    }
                }
            };

            ReplaceSubquery(currentQuery, node, columnReference);
        }

        private void ReplaceSubquery(QuerySpecification parent, ScalarSubquery oldNode, ColumnReferenceExpression newNode)
        {
            for (int i = 0; i < parent.SelectElements.Count; i++)
            {
                if (parent.SelectElements[i] is SelectScalarExpression selectExpr &&
                    ContainsSubquery(selectExpr.Expression, oldNode))
                {
                    selectExpr.Expression = newNode;
                }
            }
        }

        private bool ContainsSubquery(ScalarExpression expression, ScalarSubquery subquery)
        {
            return expression == subquery;
        }
    }

}

public class SqlModelGenerator
{
    private readonly string _connectionString;
    private readonly string _database;
    private readonly List<string> _generatedClasses = new List<string>();

    private SqlService service;
    public static SqlService staticService;
    
    public SqlModelGenerator(string connectionString, string database)
    {
        _connectionString = connectionString;
        _database = database;
        service = new SqlService(connectionString);
        staticService = service;
    }

    public async Task<string> GenerateModelClasses(string sql)
    {
        var parser = new TSql160Parser(false);
        using var reader = new StringReader(sql);
        var tree = parser.Parse(reader, out var errors);

        if (errors.Count > 0)
        {
            throw new Exception($"SQL parsing failed: {string.Join(", ", errors.Select(e => e.Message))}");
        }

        var visitor = new SelectStatementVisitor();
        tree.Accept(visitor);

        if (visitor.MainSelect == null)
        {
            throw new Exception("No SELECT statement found");
        }

        return await GenerateClassFromSelect(visitor.MainSelect, sql);
    }

    private async Task<string> GenerateClassFromSelect(SelectStatement select, string sql)
    {
        string data = new SqlToCteTransformer().Transform(sql);
        
        if (select.QueryExpression is QuerySpecification qs)
        {
            
        }
        
        return string.Empty;
    }

    private async Task<string> GenerateClassFromSelectOld(SelectStatement select, string sql)
    {
        var columns = await service.DescribeSelect(_database, sql);
        
        if (columns.Exception is not null)
        {
            throw columns.Exception;
        }

        var sb = new StringBuilder();
        sb.AppendLine("public class GeneratedClass");
        sb.AppendLine("{");

        foreach (var column in columns.Result!)
        {
            var type = DetermineColumnType(column, select);
            var propertyName = ToPascalCase(column.Name ?? "Unknown");
            
            sb.AppendLine($"    public {type} {propertyName} {{ get; set; }}");
        }

        sb.AppendLine("}");

        // Přidáme všechny vygenerované vnořené třídy
        foreach (var nestedClass in _generatedClasses)
        {
            sb.AppendLine();
            sb.AppendLine(nestedClass);
        }

        return sb.ToString();
    }

    private SelectScalarExpression? FindSelectElementForColumn(SelectStatement select, string? columnName)
    {
        if (select.QueryExpression is QuerySpecification querySpec)
        {
            return querySpec.SelectElements
                .OfType<SelectScalarExpression>()
                .FirstOrDefault(e => GetColumnName(e).Equals(columnName, StringComparison.OrdinalIgnoreCase));
        }
        return null;
    }

    private string GetCSharpType(SqlDbTypeExt sqlType, bool isNullable)
    {
        var type = sqlType switch
        {
            SqlDbTypeExt.BigInt => "long",
            SqlDbTypeExt.Int => "int",
            SqlDbTypeExt.SmallInt => "short",
            SqlDbTypeExt.TinyInt => "byte",
            SqlDbTypeExt.Bit => "bool",
            SqlDbTypeExt.Decimal => "decimal",
            SqlDbTypeExt.Money => "decimal",
            SqlDbTypeExt.Float => "double",
            SqlDbTypeExt.Real => "float",
            SqlDbTypeExt.DateTime => "DateTime",
            SqlDbTypeExt.DateTime2 => "DateTime",
            SqlDbTypeExt.Date => "DateTime",
            SqlDbTypeExt.Time => "TimeSpan",
            SqlDbTypeExt.Char => "string",
            SqlDbTypeExt.VarChar => "string",
            SqlDbTypeExt.NChar => "string",
            SqlDbTypeExt.NVarChar => "string",
            SqlDbTypeExt.Text => "string",
            SqlDbTypeExt.NText => "string",
            SqlDbTypeExt.UniqueIdentifier => "Guid",
            _ => "object"
        };

        return isNullable && type != "string" ? type + "?" : type;
    }

    private string DetermineColumnType(SqlResultSetColumn column, SelectStatement select)
    {
        // Zjistíme, zda je sloupec JSON
        var selectElement = FindSelectElementForColumn(select, column.Name);
        if (selectElement != null && IsJsonColumn(selectElement))
        {
            var jsonSelect = ExtractJsonSelect(selectElement);
            
            if (jsonSelect != null)
            {
                var jsonType = DetermineJsonType(jsonSelect);
                return $"Json<{jsonType}>";
            }
        }

        // Pro běžné sloupce převedeme SQL typ na C#
        var csharpType = GetCSharpType(column.Type, column.Nullable);
        return csharpType;
    }

    private string DetermineJsonType(QuerySpecification jsonSelect)
    {
        var className = "JsonResult";
        var sb = new StringBuilder();
        sb.AppendLine($"public class {className}");
        sb.AppendLine("{");

        // Analyzujeme závislosti
        var analyzer = new DependencyAnalyzer();
        jsonSelect.Accept(analyzer);

        // Vytvoříme nový SELECT s potřebnými JOINy
        var modifiedSelect = ReconstructSelect(jsonSelect, analyzer.Dependencies);

        var sqlGenerator = new Sql160ScriptGenerator(new SqlScriptGeneratorOptions
        {
            KeywordCasing = KeywordCasing.Uppercase
        });
    
        var selectStatement = new SelectStatement { QueryExpression = modifiedSelect };
        sqlGenerator.GenerateScript(selectStatement, out var selectSql);

        var jsonColumns = service.DescribeSelect(_database, selectSql).Result;
    
        if (jsonColumns.Result != null)
        {
            foreach (var col in jsonColumns.Result)
            {
                var propertyName = ToPascalCase(col.Name ?? "Unknown");
                var propertyType = GetCSharpType(col.Type, col.Nullable);
                sb.AppendLine($"    public {propertyType} {propertyName} {{ get; set; }}");
            }
        }

        sb.AppendLine("}");
        _generatedClasses.Add(sb.ToString());
        return className;
    }
    
    private class DependencyAnalyzer : TSqlFragmentVisitor
{
    private readonly Stack<QueryScope> _scopes = new Stack<QueryScope>();
    private readonly List<TableReference> _dependencies = new List<TableReference>();
    
    public IReadOnlyList<TableReference> Dependencies => _dependencies;

    public DependencyAnalyzer()
    {
        _scopes.Push(new QueryScope());
    }

    public override void Visit(SelectStatement node)
    {
        _scopes.Push(new QueryScope(_scopes.Peek()));
        base.Visit(node);
        _scopes.Pop();
    }

    public override void Visit(NamedTableReference node)
    {
        var table = new TableReference
        {
            TableName = string.Join(".", node.SchemaObject.Identifiers.Select(i => i.Value)),
            Alias = node.Alias?.Value ?? node.SchemaObject.Identifiers.Last().Value
        };
        
        _scopes.Peek().Tables[table.Alias] = table;
        base.Visit(node);
    }

    public override void Visit(JoinTableReference node)
    {
        // Nejdřív navštívíme obě tabulky
        base.Visit(node);
    
        // Analyzujeme obě tabulky v JOINu
        if (node.FirstTableReference is NamedTableReference first &&
            node.SecondTableReference is NamedTableReference second)
        {
            var firstTable = new TableReference
            {
                TableName = string.Join(".", first.SchemaObject.Identifiers.Select(i => i.Value)),
                Alias = first.Alias?.Value ?? first.SchemaObject.Identifiers.Last().Value
            };
        
            var secondTable = new TableReference
            {
                TableName = string.Join(".", second.SchemaObject.Identifiers.Select(i => i.Value)),
                Alias = second.Alias?.Value ?? second.SchemaObject.Identifiers.Last().Value
            };

            // Přidáme tabulky do aktuálního scope
            var currentScope = _scopes.Peek();
            currentScope.Tables[firstTable.Alias] = firstTable;
            currentScope.Tables[secondTable.Alias] = secondTable;

            // Zaznamenáme vztah mezi tabulkami
            var joinInfo = new JoinCondition
            {
                LeftColumn = new ColumnReference 
                { 
                    SourceTable = firstTable 
                },
                RightColumn = new ColumnReference 
                { 
                    SourceTable = secondTable 
                }
            };

            firstTable.JoinConditions.Add(joinInfo);
            secondTable.JoinConditions.Add(joinInfo);
        }
    }


    public override void Visit(ColumnReferenceExpression node)
    {
        if (node.MultiPartIdentifier.Count > 1)
        {
            var alias = node.MultiPartIdentifier.Identifiers[0].Value;
            var columnName = node.MultiPartIdentifier.Identifiers[1].Value;
            
            var table = _scopes.Peek().FindTable(alias);
            if (table == null)
            {
                var outerTable = FindOuterTableReference(alias);
                if (outerTable != null && !_dependencies.Contains(outerTable))
                {
                    _dependencies.Add(outerTable);
                }
            }
        }
        base.Visit(node);
    }

    private void AnalyzeJoinCondition(BooleanComparisonExpression comparison, 
        TableReference firstTable, TableReference secondTable)
    {
        if (comparison.FirstExpression is ColumnReferenceExpression left &&
            comparison.SecondExpression is ColumnReferenceExpression right)
        {
            var leftRef = CreateColumnReference(left);
            var rightRef = CreateColumnReference(right);
        
            if (leftRef.SourceTable != null && rightRef.SourceTable != null)
            {
                var joinCondition = new JoinCondition
                {
                    LeftColumn = leftRef,
                    RightColumn = rightRef
                };
            
                leftRef.SourceTable.JoinConditions.Add(joinCondition);
                rightRef.SourceTable.JoinConditions.Add(joinCondition);
            }
        }
    }

    private ColumnReference CreateColumnReference(ColumnReferenceExpression expr)
    {
        var alias = expr.MultiPartIdentifier.Count > 1 
            ? expr.MultiPartIdentifier.Identifiers[0].Value 
            : "";
        var columnName = expr.MultiPartIdentifier.Identifiers.Last().Value;
        
        var table = _scopes.Peek().FindTable(alias);
        return new ColumnReference
        {
            ColumnName = columnName,
            SourceTable = table
        };
    }

    private TableReference? FindOuterTableReference(string alias)
    {
        foreach (var scope in _scopes)
        {
            var table = scope.FindTable(alias);
            if (table != null)
                return table;
        }
        return null;
    }
}
    
    private class QueryScope
    {
        public QueryScope? Parent { get; }
        public Dictionary<string, TableReference> Tables { get; } = new Dictionary<string, TableReference>();
    
        public QueryScope(QueryScope? parent = null)
        {
            Parent = parent;
        }

        public TableReference? FindTable(string alias)
        {
            return Tables.TryGetValue(alias, out var table) 
                ? table 
                : Parent?.FindTable(alias);
        }
    }
    
    private QuerySpecification ReconstructSelect(QuerySpecification original, IReadOnlyList<TableReference> dependencies)
    {
        var parser = new TSql160Parser(false);
    
        // Vytvoříme základní SELECT dotaz
        var baseSql = "SELECT * FROM sys.tables WHERE 1=1";
        using var reader = new StringReader(baseSql);
        var tree = parser.Parse(reader, out var errors);

        if (errors.Count > 0)
            throw new Exception($"Failed to parse base SELECT: {string.Join(", ", errors.Select(e => e.Message))}");

        var baseSelect = (QuerySpecification)((SelectStatement)((TSqlScript)tree).Batches[0].Statements[0]).QueryExpression;

        // Zkopírujeme SELECT část
        baseSelect.SelectElements.Clear();
        foreach (var element in original.SelectElements)
        {
            baseSelect.SelectElements.Add(element);
        }

        // Přidáme základní tabulky z původního FROM
        baseSelect.FromClause.TableReferences.Clear();
        if (original.FromClause?.TableReferences != null)
        {
            foreach (var tableRef in original.FromClause.TableReferences)
            {
                baseSelect.FromClause.TableReferences.Add(tableRef);
            }
        }

        // Přidáme potřebné JOINy pro závislosti
        foreach (var dep in dependencies)
        {
            AddRequiredJoins(baseSelect, dep);
        }

        return baseSelect;
    }


    private void AddRequiredJoins(QuerySpecification select, TableReference table)
    {
        foreach (var joinCondition in table.JoinConditions)
        {
            var parser = new TSql160Parser(false);
            var joinSql = $@"
            SELECT * 
            FROM {joinCondition.LeftColumn.SourceTable!.TableName} AS {joinCondition.LeftColumn.SourceTable!.Alias} 
            JOIN {joinCondition.RightColumn.SourceTable!.TableName} AS {joinCondition.RightColumn.SourceTable!.Alias} 
            ON {joinCondition.LeftColumn.SourceTable!.Alias}.{joinCondition.LeftColumn.ColumnName} = 
               {joinCondition.RightColumn.SourceTable!.Alias}.{joinCondition.RightColumn.ColumnName}";

            using var reader = new StringReader(joinSql);
            var tree = parser.Parse(reader, out var errors);

            if (errors.Count > 0)
                throw new Exception($"Failed to parse JOIN: {string.Join(", ", errors.Select(e => e.Message))}");

            var joinStatement = (SelectStatement)((TSqlScript)tree).Batches[0].Statements[0];
            var querySpec = (QuerySpecification)joinStatement.QueryExpression;
            var joinRef = (JoinTableReference)querySpec.FromClause.TableReferences[0];

            // Přidáme JOIN pouze pokud ještě není v seznamu
            if (!select.FromClause.TableReferences.OfType<JoinTableReference>()
                    .Any(j => IsEquivalentJoin(j, joinRef)))
            {
                select.FromClause?.TableReferences.Add(joinRef);
            }
        }
    }

private bool IsEquivalentJoin(JoinTableReference join1, JoinTableReference join2)
{
    // Porovnáme tabulky a jejich aliasy
    return GetTableFullName(join1.FirstTableReference) == GetTableFullName(join2.FirstTableReference) &&
           GetTableFullName(join1.SecondTableReference) == GetTableFullName(join2.SecondTableReference);
}

private string GetTableFullName(Microsoft.SqlServer.TransactSql.ScriptDom.TableReference table)
{
    if (table is NamedTableReference named)
    {
        return $"{string.Join(".", named.SchemaObject.Identifiers.Select(i => i.Value))}.{named.Alias?.Value}";
    }
    return string.Empty;
}

private BooleanExpression CreateJoinCondition(JoinCondition condition)
{
    return new BooleanComparisonExpression
    {
        FirstExpression = new ColumnReferenceExpression
        {
            MultiPartIdentifier = new MultiPartIdentifier
            {
                Identifiers = 
                {
                    new Identifier { Value = condition.LeftColumn.SourceTable!.Alias },
                    new Identifier { Value = condition.LeftColumn.ColumnName }
                }
            }
        },
        SecondExpression = new ColumnReferenceExpression
        {
            MultiPartIdentifier = new MultiPartIdentifier
            {
                Identifiers = 
                {
                    new Identifier { Value = condition.RightColumn.SourceTable!.Alias },
                    new Identifier { Value = condition.RightColumn.ColumnName }
                }
            }
        },
        ComparisonType = BooleanComparisonType.Equals
    };
}
    
    private QuerySpecification ReplaceCorrelatedColumns(QuerySpecification original)
    {
        // Vytvoříme nový statement s kopií původního dotazu
        var newStatement = new SelectStatement { QueryExpression = original };
    
        // Inicializujeme scope tracker a visitor
        var scopeTracker = new ScopeTracker();
        var visitor = new CorrelatedColumnReplacer(scopeTracker);
    
        // Projdeme celý strom a nahradíme korelované sloupce
        newStatement.Accept(visitor);
    
        // Odstraníme FOR JSON klauzuli
        var querySpec = (QuerySpecification)newStatement.QueryExpression;
        querySpec.ForClause = null;

        // Vygenerujeme a zparsujeme SQL
        var sqlGenerator = new Sql160ScriptGenerator(new SqlScriptGeneratorOptions
        {
            KeywordCasing = KeywordCasing.Uppercase
        });
    
        sqlGenerator.GenerateScript(newStatement, out var modifiedSql);

        var parser = new TSql160Parser(false);
        using var reader = new StringReader(modifiedSql);
        var tree = parser.Parse(reader, out var errors);

        if (errors.Count > 0)
        {
            throw new Exception($"SQL parsing failed: {string.Join(", ", errors.Select(e => e.Message))}");
        }

        var script = (TSqlScript)tree;
        var batch = (TSqlBatch)script.Batches[0];
        var statement = (SelectStatement)batch.Statements[0];
        return (QuerySpecification)statement.QueryExpression;
    }
    
private class CorrelatedColumnReplacer : TSqlFragmentVisitor
{
    private readonly Stack<TSqlFragment> _parentStack = new Stack<TSqlFragment>();
    private readonly ScopeTracker _scopeTracker;
    
    public CorrelatedColumnReplacer(ScopeTracker scopeTracker)
    {
        _scopeTracker = scopeTracker;
    }
    
    public override void Visit(TSqlFragment node)
    {
        if (node == null) return;
        
        _parentStack.Push(node);
        
        if (node is ScalarSubquery || node is QuerySpecification)
        {
            _scopeTracker.EnterScope();
        }
        
        base.Visit(node);
        
        if (node is ScalarSubquery || node is QuerySpecification)
        {
            _scopeTracker.ExitScope();
        }
        
        _parentStack.Pop();
    }
    
    public override void Visit(NamedTableReference node)
    {
        if (node.Alias != null)
        {
            _scopeTracker.AddTable(
                node.Alias.Value,
                string.Join(".", node.SchemaObject.Identifiers.Select(i => i.Value))
            );
        }
        base.Visit(node);
    }

    public override void Visit(WhereClause node)
    {
        // Nahradíme WHERE podmínku za 1=1
        var newPredicate = new BooleanComparisonExpression
        {
            FirstExpression = new IntegerLiteral { Value = "1" },
            SecondExpression = new IntegerLiteral { Value = "1" },
            ComparisonType = BooleanComparisonType.Equals
        };
        
        node.SearchCondition = newPredicate;
    }

    // Důležité: NENAHRAZUJEME korelované sloupce v SELECT části
    public override void Visit(ColumnReferenceExpression node)
    {
        // Ponecháme původní sloupce
        base.Visit(node);
    }

    private void ReplaceNode(TSqlFragment oldNode, TSqlFragment newNode)
    {
        foreach (var parent in _parentStack)
        {
            foreach (var prop in parent.GetType().GetProperties())
            {
                if (!prop.CanWrite) continue;
                
                var value = prop.GetValue(parent);
                if (value == oldNode)
                {
                    prop.SetValue(parent, newNode);
                    return;
                }
                
                if (value is IList list)
                {
                    for (int i = 0; i < list.Count; i++)
                    {
                        if (list[i] == oldNode)
                        {
                            list[i] = newNode;
                            return;
                        }
                    }
                }
            }
        }
    }
}


private class CorrelatedColumnFinder : TSqlFragmentVisitor
{
    public bool HasCorrelatedColumn { get; private set; }

    public override void Visit(ColumnReferenceExpression node)
    {
        if (node.MultiPartIdentifier.Count > 1)
        {
            HasCorrelatedColumn = true;
        }
        base.Visit(node);
    }
}

    
    private bool IsJsonColumn(SelectScalarExpression scalar)
    {
        if (scalar.Expression is ScalarSubquery subquery)
        {
            var visitor = new JsonForClauseVisitor();
            subquery.Accept(visitor);
            return visitor.HasJsonForClause;
        }
        return false;
    }

    private QuerySpecification? ExtractJsonSelect(SelectScalarExpression scalar)
    {
        if (scalar.Expression is ScalarSubquery subquery)
        {
            return subquery.QueryExpression as QuerySpecification;
        }
        return null;
    }

    private string ToPascalCase(string input)
    {
        if (string.IsNullOrEmpty(input))
            return input;

        // Odstraníme neplatné znaky
        var chars = input.Where(c => char.IsLetterOrDigit(c) || c == '_').ToArray();
        if (chars.Length == 0)
            return "Property";

        return char.ToUpper(chars[0]) + new string(chars.Skip(1).ToArray());
    }
    
    private string GetColumnName(SelectScalarExpression scalar)
    {
        if (scalar.ColumnName != null)
            return scalar.ColumnName.Value;

        // Pokud není explicitní alias, pokusíme se získat název z výrazu
        if (scalar.Expression is ColumnReferenceExpression colRef)
            return colRef.MultiPartIdentifier.Identifiers.Last().Value;

        return "Unknown";
    }
    
    public class ColumnInfo
    {
        public string ColumnName { get; set; } = "";
        public bool IsJson { get; set; }
        public SelectStatement? JsonQuerySelect { get; set; }
    }
    
    public class SelectStatementVisitor : TSqlFragmentVisitor
    {
        public SelectStatement? MainSelect { get; private set; }

        public override void Visit(SelectStatement node)
        {
            if (MainSelect == null)
                MainSelect = node;
        
            base.Visit(node);
        }
    }

    public class JsonForClauseVisitor : TSqlFragmentVisitor
    {
        public bool HasJsonForClause { get; private set; }

        public override void Visit(JsonForClause node)
        {
            HasJsonForClause = true;
            base.Visit(node);
        }
    }
    
    private class TableScope
    {
        public readonly TableScope? _parent;
        public readonly Dictionary<string, TableInfo> _tables = new Dictionary<string, TableInfo>();

        public TableScope(TableScope? parent = null)
        {
            _parent = parent;
        }

        public class TableInfo
        {
            public string Alias { get; set; } = "";
            public string TableName { get; set; } = "";
            public List<ColumnInfo> Columns { get; } = new List<ColumnInfo>();
        }

        public class ColumnInfo
        {
            public string Name { get; set; } = "";
            public string DataType { get; set; } = "";
        }

        public void AddTable(string alias, string tableName)
        {
            _tables[alias] = new TableInfo 
            { 
                Alias = alias, 
                TableName = tableName 
            };
        }

        public TableInfo? GetTableByAlias(string alias)
        {
            // Nejdřív hledáme v aktuálním scope
            if (_tables.TryGetValue(alias, out var table))
                return table;

            // Pokud nenajdeme, hledáme v rodičovském scope
            return _parent?.GetTableByAlias(alias);
        }

        public TableScope CreateChildScope()
        {
            return new TableScope(this);
        }
    }

    private class ScopeTracker
    {
        private TableScope _currentScope;

        public ScopeTracker()
        {
            _currentScope = new TableScope();
        }

        public void EnterScope()
        {
            _currentScope = _currentScope.CreateChildScope();
        }

        public void ExitScope()
        {
            _currentScope = _currentScope._parent ?? new TableScope();
        }

        public void AddTable(string alias, string tableName)
        {
            _currentScope.AddTable(alias, tableName);
        }

        public TableScope.TableInfo? GetTableByAlias(string alias)
        {
            return _currentScope.GetTableByAlias(alias);
        }
    }
    
    public class TableReference
    {
        public string TableName { get; set; } = "";
        public string Alias { get; set; } = "";
        public List<ColumnReference> Columns { get; } = new List<ColumnReference>();
        public List<JoinCondition> JoinConditions { get; } = new List<JoinCondition>();
    }

    public class ColumnReference
    {
        public string ColumnName { get; set; } = "";
        public TableReference? SourceTable { get; set; }
    }

    public class JoinCondition
    {
        public ColumnReference LeftColumn { get; set; } = null!;
        public ColumnReference RightColumn { get; set; } = null!;
    }
}
