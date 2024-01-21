using OpenDBDiff.Abstractions.Schema.Model;
using OpenDBDiff.SqlServer.Schema.Generates;
using OpenDBDiff.SqlServer.Schema.Model;
using OpenDBDiff.SqlServer.Schema.Options;

namespace Minfold;

public static class Synchronizer
{
    public static string Synchronize()
    {
        Generate sql = new Generate
        {
            ConnectionString = "C1",
            Options = new SqlOption
            {
                 Script = new SqlOptionScript
                 {
                     DiffHeader = string.Empty
                 }
            }
        };
        Database sourceDatabase = sql.Process();

        sql.ConnectionString = "C2";
        Database destinationDatabase = sql.Process();

        Database diff = Generate.Compare(destinationDatabase, sourceDatabase);
        string updateScript = diff.ToSqlDiff(new List<ISchemaBase>()).ToSQL();

        return updateScript;
    }
}