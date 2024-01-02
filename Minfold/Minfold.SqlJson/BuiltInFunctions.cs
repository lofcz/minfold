using System.Collections.Generic;

namespace Minfold.SqlJson;

public class Nullable<T>(T value, bool nullable)
{
    public T Value { get; set; } = value;
    public bool CanBeNull { get; set; } = nullable;
}

public static class BuiltInFunctions
{
    public static readonly Dictionary<string, Nullable<SqlDbTypeExt>> Common = new Dictionary<string, Nullable<SqlDbTypeExt>>
    {
        // date
        { "current_timestamp", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.DateTime2, false) },
        { "dateadd", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.DateTime2, true) },
        { "datediff", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.DateTime2, true) },
        { "datefromparts", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.DateTime2, true) },
        { "datename", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.DateTime2, true) },
        { "datepart", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.DateTime2, true) },
        { "day", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.DateTime2, true) },
        { "getdate", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.DateTime2, false) },
        { "getutcdate", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.DateTime2, false) },
        { "isdate", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.DateTime2, true) },
        { "month", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.DateTime2, true) },
        { "sysdatetime", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.DateTime2, false) },
        { "year", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.DateTime2, true) },
        // numeric
        { "abs", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "acos", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "asin", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "atan", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "atn2", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "avg", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "ceiling", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "count", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "cos", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "cot", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "degrees", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "exp", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "floor", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "log", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "log10", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "max", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "min", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "pi", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "power", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "radians", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "rand", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "round", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "sign", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "sin", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "sqrt", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "square", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "sum", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "tan", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "greatest", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        { "least", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Real, true) },
        // args
        { "choose", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.ArgMixed, true) },
        { "iif", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.ArgMixed, true) },
        // string
        { "acii", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "char", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "charindex", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "concat", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "concat_ws", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "datalength", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "difference", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "format", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "left", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "len", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "lower", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "ltrim", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "nchar", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "patindex", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "quotename", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "replace", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "replicate", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "reverse", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "right", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "rtrim", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "soundex", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "space", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "str", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "stuff", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "substring", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "translate", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "trim", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "unicode", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "upper", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        // string system
        { "current_user", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "session_user", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "system_user", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "user_name", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        { "sessionproperty", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.NVarChar, true) },
        // bool
        { "isnumeric", new Nullable<SqlDbTypeExt>(SqlDbTypeExt.Bit, false) }
    };
}