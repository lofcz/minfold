using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.SqlServer.Dac;
using Microsoft.SqlServer.Dac.CodeAnalysis;
using Microsoft.SqlServer.Dac.Model;
using Microsoft.SqlServer.TransactSql.ScriptDom;
namespace Minfold.SqlJson;

public static class Program
{
    public static void Main()
    {
        string connectionString = "Server=.;Database=AdventureWorks2019;Integrated Security=True;TrustServerCertificate=True;";
        string sqlQuery = @"
            SELECT 
                c.CustomerID,
                p.FirstName,
                (
                    SELECT 
                        soh.OrderDate,
                        soh.TotalDue
                    FROM Sales.SalesOrderHeader soh
                    WHERE soh.CustomerID = c.CustomerID
                    FOR JSON PATH
                ) AS CustomerOrders
            FROM Sales.Customer c
            JOIN Person.Person p ON p.BusinessEntityID = c.PersonID
            WHERE c.CustomerID < 11010;
        ";

        try
        {
            var resolver = new FinalColumnResolver(connectionString);
            var resolvedColumns = resolver.Analyze(sqlQuery);
            
            foreach (var column in resolvedColumns)
            {
                string indent = new string(' ', column.NestingLevel * 2);
                //Console.WriteLine($"{indent}Name: {column.Name}, Alias: {column.Alias ?? "[N/A]"}, SQL Type: {column.SqlType}");
            }
        }
        catch (Exception ex)
        {
           
        }
    }
}

public record ResolvedColumn(string Name, string? Alias, string SqlType, int NestingLevel);

public class FinalColumnResolver
{
    private readonly string _connectionString;

    public FinalColumnResolver(string connectionString)
    {
        _connectionString = connectionString;
    }

    public List<ResolvedColumn> Analyze(string sqlQuery)
    {
        // 1. Načtení modelu databáze.
        using TSqlModel dbModel = TSqlModel.LoadFromDatabase(_connectionString);

        // 2. Naparsování dotazu.
        var parser = new TSql160Parser(true);
        var fragment = parser.Parse(new StringReader(sqlQuery), out var errors);

        if (errors != null && errors.Count > 0)
        {
            throw new System.InvalidOperationException("SQL Parse Error: " + errors[0].Message);
        }

        // 3. Vytvoření a spuštění visitoru.
        var visitor = new ColumnSchemaVisitor(dbModel);
        fragment.Accept(visitor);

        return visitor.ResolvedColumns;
    }
}

public class ColumnSchemaVisitor : TSqlFragmentVisitor
{
    private readonly TSqlModel _model;
    private readonly Stack<Dictionary<string, TSqlObject>> _fromClauseTables = new();
    private int _nestingLevel = 0;

    public List<ResolvedColumn> ResolvedColumns { get; } = new();

    public ColumnSchemaVisitor(TSqlModel model)
    {
        _model = model;
    }

    public override void Visit(QuerySpecification node)
    {
        _nestingLevel++;
        _fromClauseTables.Push(new Dictionary<string, TSqlObject>(System.StringComparer.OrdinalIgnoreCase));

        // Nejdříve zpracovat FROM klauzuli, abychom znali tabulky a aliasy.
        if (node.FromClause != null)
        {
            base.Visit(node.FromClause);
        }

        // Poté zpracovat zbytek, včetně SELECT listu.
        foreach (var element in node.SelectElements)
        {
            element.Accept(this);
        }
        
        _fromClauseTables.Pop();
        _nestingLevel--;
    }

    public override void Visit(NamedTableReference node)
    {
        string alias = node.Alias?.Value ?? node.SchemaObject.BaseIdentifier.Value;
        var table = _model.GetObjects(DacQueryScopes.UserDefined, Table.TypeClass)
                          .FirstOrDefault(t => t.Name.ToString() == node.SchemaObject.ToString());

        if (table != null && _fromClauseTables.Count > 0)
        {
            _fromClauseTables.Peek()[alias] = table;
        }
        base.Visit(node);
    }

    public override void Visit(SelectScalarExpression node)
    {
        if (node.Expression is ColumnReferenceExpression colRef)
        {
            string alias = node.ColumnName?.Value;
            var (columnObject, columnName) = ResolveColumn(colRef);

            if (columnObject != null)
            {
                string typeName = GetTypeDetails(columnObject);
                ResolvedColumns.Add(new ResolvedColumn(columnName, alias, typeName, _nestingLevel));
            }
        }
        else
        {
            // Pro subqueries atd. pokračujeme v procházení.
            base.Visit(node);
        }
    }

    private (TSqlObject, string) ResolveColumn(ColumnReferenceExpression node)
    {
        var identifiers = node.MultiPartIdentifier.Identifiers;
        string columnName = identifiers.Last().Value;
        string tableAlias = identifiers.Count > 1 ? identifiers[identifiers.Count - 2].Value : null;

        // Prohledávání scopů od nejvnitřnějšího po nejvnější (řeší korelaci).
        foreach (var scope in _fromClauseTables)
        {
            TSqlObject tableObject = null;
            if (tableAlias != null)
            {
                scope.TryGetValue(tableAlias, out tableObject);
            }
            else // Sloupec bez aliasu tabulky - musíme prohledat všechny tabulky ve scope.
            {
                // Najdeme první tabulku, která obsahuje sloupec s daným jménem.
                // Toto je zjednodušení, které neřeší nejednoznačnost.
                tableObject = scope.Values.FirstOrDefault(t => t.GetReferenced(Table.Columns).Any(c => c.Name.Parts.Last() == columnName));
            }

            if (tableObject != null)
            {
                var column = tableObject.GetReferenced(Table.Columns)
                                        .FirstOrDefault(c => c.Name.Parts.Last().Equals(columnName, System.StringComparison.OrdinalIgnoreCase));
                if (column != null)
                {
                    return (column, column.Name.ToString());
                }
            }
        }
        return (null, null);
    }

    private static string GetTypeDetails(TSqlObject column)
    {
        var dataType = column.GetReferenced(Column.DataType).FirstOrDefault();
        if (dataType == null) return "UNKNOWN";

        string typeName = dataType.Name.Parts.Last().ToUpperInvariant();
        switch (typeName)
        {
            case "VARCHAR":
            case "NVARCHAR":
            case "CHAR":
            case "NCHAR":
            case "BINARY":
            case "VARBINARY":
                var length = 0;
                return $"{typeName}{(length == -1 ? "(MAX)" : $"({length})")}";
            case "DECIMAL":
            case "NUMERIC":
                var precision = 0;
                var scale = 0;
                return $"{typeName}({precision}, {scale})";
            default:
                return typeName;
        }
    }
}