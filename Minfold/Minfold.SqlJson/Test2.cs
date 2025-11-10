using System.Threading.Tasks;
using SqlParser;
using SqlParser.Ast;

namespace Minfold.SqlJson;

public class Test2
{
    public static async Task Test()
    {
        var sql = """
                    select top 10 *
                    from my_table
                    where abc > 99
                    order by xyz desc
                  """;

        Sequence<Statement> ast = new SqlQueryParser().Parse(sql);
        Statement source = ast[0];

        Statement.Select select = source.AsSelect();
        Select select2 = select.Query.Body.AsSelect();
    }
}