using System;
using System.Collections.Generic;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace Minfold.SqlJson;

public enum MinfoldSqlResultTypes
{
    Unknown,
    Ok,
    SqlSyntaxInvalid,
    DatabaseConnectionFailed,
    MappingAmbiguities
}

public class MinfoldSqlResult
{
    public IList<ParseError>? ParseErrors { get; set; }
    public string? GeneratedCode { get; set; }
    public MinfoldSqlResultTypes ResultType { get; set; }
    public Exception? Exception { get; set; }
    public MappedModelAmbiguitiesAccumulator? MappingAmbiguities { get; set; }
}