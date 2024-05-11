using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace Minfold.SqlJson;

internal class SqlModelVisitor : TSqlFragmentVisitor
{
    public Scope Root { get; set; }
    public Query Query { get; set; }

    void SolveQuery(Scope scope, QuerySpecification qs, Query parent, int columnIndex, Dictionary<string, int> starsOffset)
    {
        Query query = new Query
        {
            QuerySpecification = qs,
            ColumnIndex = columnIndex,
            StarsOffset = starsOffset
        };
        
        StringBuilder sb = new StringBuilder();
        
        if (qs.ForClause is JsonForClause jsonForClause)
        {
            int start = qs.ForClause.LastTokenIndex - qs.ForClause.FragmentLength;
            int len = qs.ForClause.FragmentLength;

            for (int i = qs.FirstTokenIndex; i < qs.LastTokenIndex; i++)
            {
                if (i >= start && i <= start + len)
                {
                    continue;
                }

                sb.Append(qs.ScriptTokenStream[i].Text);
            }

            query.TransformedSql = sb.ToString();
            
            foreach (JsonForClauseOption option in jsonForClause.Options)
            {
                query.JsonOptions |= option.OptionKind;
            }
            
            sb.Clear();
        }
        
        for (int i = qs.FirstTokenIndex; i <= qs.LastTokenIndex; i++)
        {
            sb.Append(qs.ScriptTokenStream[i].Text);
        }
        
        query.RawSql = sb.ToString();

        void SolveSelectScalarCol(ScalarExpression scalarExpression, SelectScalarExpression? selScalar, int cIndex, Dictionary<string, int> starsOffset)
        {
            switch (scalarExpression)
            {
                case ScalarSubquery { QueryExpression: QuerySpecification subquerySpec }:
                {
                    SolveQuery(scope, subquerySpec, query, cIndex, starsOffset);
                    break;
                }      
                case ParenthesisExpression parentExpr:
                {
                    SolveSelectScalarCol(parentExpr.Expression, null, cIndex, starsOffset);
                    break;
                }
                case SearchedCaseExpression searchedCaseExpression:
                {
                    foreach (SearchedWhenClause whenBranch in searchedCaseExpression.WhenClauses)
                    {
                        SolveSelectScalarCol(whenBranch.ThenExpression, null, cIndex, starsOffset);
                    }

                    SolveSelectScalarCol(searchedCaseExpression.ElseExpression, null, cIndex, starsOffset);
                    break;
                }
            }
        }

        Dictionary<string, int> so = [];
        Scope localScope = new Scope(scope, qs);
        scope.Childs.Add(localScope);

        if (qs.FromClause?.TableReferences is not null)
        {
            foreach (TableReference source in qs.FromClause.TableReferences)
            {
                string? tbl, alias;

                switch (source)
                {
                    case NamedTableReference namedFrom:
                        tbl = namedFrom.SchemaObject.BaseIdentifier.Value;
                        alias = namedFrom.Alias?.Value;

                        localScope.Identifiers.Add(new ScopeIdentifier(alias ?? tbl, tbl, false));
                        break;
                    case QueryDerivedTable queryDerivedTable:
                    {
                        alias = queryDerivedTable.Alias?.Value;

                        if (queryDerivedTable.QueryExpression is QuerySpecification fromQs)
                        {
                            localScope.Identifiers.Add(new ScopeIdentifier(alias, SolveSelect(fromQs, scope), false));
                        }

                        break;
                    }
                    case QualifiedJoin qualifiedJoin:
                    {
                        bool nullable = qualifiedJoin.QualifiedJoinType is QualifiedJoinType.LeftOuter or QualifiedJoinType.FullOuter;

                        if (qualifiedJoin.FirstTableReference is NamedTableReference namedJoin1)
                        {
                            alias = namedJoin1.Alias?.Value;
                            localScope.Identifiers.Add(new ScopeIdentifier(alias, namedJoin1.SchemaObject.BaseIdentifier.Value, nullable));
                        }
                        
                        if (qualifiedJoin.SecondTableReference is NamedTableReference namedJoin2)
                        {
                            alias = namedJoin2.Alias?.Value;
                            localScope.Identifiers.Add(new ScopeIdentifier(alias, namedJoin2.SchemaObject.BaseIdentifier.Value, nullable));
                        }

                        break;
                    }
                }
            }   
        }
        
        for (int i = 0; i < qs.SelectElements.Count; i++)
        {
            switch (qs.SelectElements[i])
            {
                case SelectStarExpression selStar:
                {
                    if (selStar.Qualifier is null || selStar.Qualifier.Identifiers.Count is 1 && selStar.Qualifier.Identifiers[0].Value is "*")
                    {
                        
                    }
                    else
                    {
                        
                    }
                    
                    break;
                }
                case SelectScalarExpression selScalar:
                {
                    SolveSelectScalarCol(selScalar.Expression, selScalar, i, []);
                    so.Clear();
                    break;
                }
            }
        }

        query.Scope = localScope;

        Scope tmpScope = SolveSelect(qs, scope);
        
        query.Scope.SelectColumns.Clear();
        query.Scope.SelectColumns.AddRange(tmpScope.SelectColumns);
        
        //Scope queryScope = SolveSelect(qs, scope);
        //query.Scope = queryScope;
        parent.Childs.Add(query);
    }
    
    Scope SolveSelect(QuerySpecification qs, Scope? parent)
    {
        Scope localScope = new Scope(parent, qs);
        parent?.Childs.Add(localScope);

        if (qs.ForClause is JsonForClause jsonForClause)
        {
            foreach (JsonForClauseOption option in jsonForClause.Options)
            {
                localScope.JsonOptions |= option.OptionKind;
            }
        }

        if (qs.FromClause?.TableReferences is not null)
        {
            foreach (TableReference fromTbl in qs.FromClause.TableReferences)
            {
                string? tbl, alias;

                switch (fromTbl)
                {
                    case NamedTableReference namedFrom:
                        tbl = namedFrom.SchemaObject.BaseIdentifier.Value;
                        alias = namedFrom.Alias?.Value;

                        localScope.Identifiers.Add(new ScopeIdentifier(alias ?? tbl, tbl, false));
                        break;
                    case QueryDerivedTable queryDerivedTable:
                    {
                        alias = queryDerivedTable.Alias?.Value;

                        if (queryDerivedTable.QueryExpression is QuerySpecification fromQs)
                        {
                            localScope.Identifiers.Add(new ScopeIdentifier(alias, SolveSelect(fromQs, localScope), false));
                        }

                        break;
                    }
                    case QualifiedJoin qualifiedJoin:
                    {
                        bool nullable = qualifiedJoin.QualifiedJoinType is QualifiedJoinType.LeftOuter or QualifiedJoinType.FullOuter;

                        if (qualifiedJoin.FirstTableReference is NamedTableReference namedJoin1)
                        {
                            alias = namedJoin1.Alias?.Value;
                            localScope.Identifiers.Add(new ScopeIdentifier(alias, namedJoin1.SchemaObject.BaseIdentifier.Value, nullable));
                        }
                        
                        if (qualifiedJoin.SecondTableReference is NamedTableReference namedJoin2)
                        {
                            alias = namedJoin2.Alias?.Value;
                            localScope.Identifiers.Add(new ScopeIdentifier(alias, namedJoin2.SchemaObject.BaseIdentifier.Value, nullable));
                        }

                        break;
                    }
                }
            }
        }
        
        SelectColumn? SolveSelectScalarCol(ScalarExpression scalarExpression, SelectScalarExpression? selScalar)
        {
            string? alias = selScalar?.ColumnName?.Value;
            
            switch (scalarExpression)
            {
                case ColumnReferenceExpression colRef when colRef.MultiPartIdentifier.Identifiers.Count is 1:
                {
                    return new SelectColumn(SelectColumnTypes.TableColumn, alias, colRef.MultiPartIdentifier.Identifiers[0].Value, null, null, colRef);
                }
                case ColumnReferenceExpression colRef when colRef.MultiPartIdentifier.Identifiers.Count is 2:
                {
                    return new SelectColumn(SelectColumnTypes.TableColumn, alias, colRef.MultiPartIdentifier.Identifiers[1].Value, colRef.MultiPartIdentifier.Identifiers[0].Value, null, colRef);
                }
                case ScalarSubquery { QueryExpression: QuerySpecification subquerySpec }:
                {
                    return new SelectColumn(SelectColumnTypes.Query, SolveSelect(subquerySpec, localScope), alias, scalarExpression);
                }
                case Literal literal:
                {
                    return new SelectColumn(SelectColumnTypes.LiteralValue, alias, null, null, literal.Value, literal)
                    {
                        LiteralType = literal.ToCsType()
                    };
                }
                case CastCall castCall:
                {
                    SelectColumn? castQuery = SolveSelectCol(castCall.Parameter);

                    if (castQuery is not null)
                    {
                        if (castCall.DataType is SqlDataTypeReference dataTypeReference)
                        {
                            castQuery.TransformedType = SelectColumnTypes.LiteralValue;
                            castQuery.TransformedLiteralType = dataTypeReference.SqlDataTypeOption.ToCsType();
                            castQuery.Nullable = true;
                        }

                        castQuery.Alias = alias;
                    }
                    
                    return castQuery;
                }
                case SearchedCaseExpression searchedCaseExpression:
                {
                    List<SelectColumn> accu = [];
                    
                    foreach (SearchedWhenClause whenClause in searchedCaseExpression.WhenClauses)
                    {
                        SelectColumn? branchSelect = SolveSelectScalarCol(whenClause.ThenExpression, null);

                        if (branchSelect is not null)
                        {
                            accu.Add(branchSelect);
                        }
                    }

                    SelectColumn? elseSelect = SolveSelectScalarCol(searchedCaseExpression.ElseExpression, null);
                    
                    if (elseSelect is not null)
                    {
                        accu.Add(elseSelect);
                    }

                    foreach (SelectColumn x in accu)
                    {
                        if (x.Type is not SelectColumnTypes.LiteralValue)
                        {
                            return new SelectColumn(SelectColumnTypes.OneOf, alias, searchedCaseExpression, accu);
                        }
                    }
                    
                    return null;
                }
                case BinaryExpression binExpr:
                {
                    SelectColumn? lhs = SolveSelectCol(binExpr.FirstExpression);
                    SelectColumn? rhs = SolveSelectCol(binExpr.SecondExpression);

                    if (lhs?.Type is SelectColumnTypes.LiteralValue && rhs?.Type is SelectColumnTypes.LiteralValue && lhs.LiteralType is not null && rhs.LiteralType is not null)
                    {
                        int lhsP = lhs.LiteralType.Value.ImplicitConversionPriority();
                        int rhsP = rhs.LiteralType.Value.ImplicitConversionPriority();

                        return new SelectColumn(SelectColumnTypes.LiteralValue, alias, null, null, null, binExpr)
                        {
                            LiteralType = lhsP > rhsP ? lhs.LiteralType.Value : rhs.LiteralType.Value,
                            Nullable = lhs.LiteralType is SqlDbTypeExt.Null || rhs.LiteralType is SqlDbTypeExt.Null || lhs.Nullable || rhs.Nullable
                        };
                    }
                    
                    int z = 0;
                    
                    return null;
                }
                case ParenthesisExpression parentExpr:
                {
                    return SolveSelectCol(parentExpr.Expression);
                }
                case FunctionCall functionCall:
                {
                    if (BuiltInFunctions.Common.TryGetValue(functionCall.FunctionName.Value, out Nullable<SqlDbTypeExt>? builtInType))
                    {
                        if (builtInType.Value is not (SqlDbTypeExt.ArgMixed or SqlDbTypeExt.Unknown or SqlDbTypeExt.CsIdentifier))
                        {
                            return new SelectColumn(SelectColumnTypes.LiteralValue, alias, null, null, null, functionCall)
                            {
                                LiteralType = builtInType.Value,
                                Nullable = builtInType.CanBeNull
                            };   
                        }
                    }
                    
                    return null;
                }
                default:
                    return null;
            }
        }
        
        SelectColumn? SolveSelectCol(TSqlFragment selCol)
        {
            switch (selCol)
            {
                case SelectStarExpression selStar:
                {
                    return new SelectColumn(SelectColumnTypes.StarDelayed, null, null, null, selStar, localScope)
                    {
                        Identifier = selStar.Qualifier?.Identifiers?.Select(x => x.Value).ToCsv(".")
                    };
                }
                case ScalarExpression scalarExpression:
                {
                    return SolveSelectScalarCol(scalarExpression, null);
                }
                case SelectScalarExpression selScalar:
                {
                    return SolveSelectScalarCol(selScalar.Expression, selScalar);
                }
            }

            return null;
        }
        
        foreach (SelectElement selCol in qs.SelectElements)
        {
            SelectColumn? column = SolveSelectCol(selCol);

            if (column is not null)
            {
                localScope.SelectColumns.Add(column);
            }
        }

        return localScope;
    }
    
    public override void Visit(TSqlStatement node)
    {
        if (node is SelectStatement select)
        {
            Scope root = new Scope(null, null);
            Query rootQuery = new Query();
            
            if (select.QueryExpression is QuerySpecification qs)
            {
                SolveQuery(root, qs, rootQuery, 0, []);
                //root = SolveSelect(qs, root);
            }
            
            Root = root;
            Query = rootQuery;
            //Query.Scope = root;
        }

        base.Visit(node);
    }
}