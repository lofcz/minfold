
using SqlParser;

namespace Minfold.Migrations;

public class Class1
{
    public static void Parse()
    {
        var sql = """
                  ALTER TABLE dbo.HypeTable3 SET (LOCK_ESCALATION = TABLE)
                  """;

        var ast =  new Parser().ParseSql(sql);

        
        
        int z = 0;
    }
}