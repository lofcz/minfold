using System.Data;
using System.Text;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.Data.SqlClient;
namespace Minfold;

public class SqlService
{
    private string connString;

    public SqlService(string connString)
    {
        this.connString = connString;
    }

    public async Task<Exception?> TestConnection()
    {
        await using SqlConnectionResult conn = await Connect();
        return conn.Exception;
    }

    static string SqlSchema(string dbName, List<string>? tables)
    {
        StringBuilder? sb = null;

        if (tables is not null)
        {
            sb = new StringBuilder();

            for (int i = 0; i < tables.Count; i++)
            {
                if (i == tables.Count - 1)
                {
                    sb.Append($"@t{i}");
                    continue;
                }
                
                sb.Append($"@t{i}, ");
            }
        }
        
        return $$"""
         use {{dbName}}
         select col.TABLE_NAME, col.COLUMN_NAME, col.ORDINAL_POSITION, col.IS_NULLABLE, col.DATA_TYPE,
                 columnproperty(object_id(col.TABLE_SCHEMA + '.' + col.TABLE_NAME), col.COLUMN_NAME, 'IsIdentity') as IS_IDENTITY,
                 columnproperty(object_id(col.TABLE_SCHEMA + '.' + col.TABLE_NAME), col.COLUMN_NAME, 'IsComputed') as IS_COMPUTED,
                isnull(j.pk, cast(0 as bit)) as IS_PRIMARY,
                 cc.definition as COMPUTED_DEFINITION
         from information_schema.COLUMNS col
         left join sys.computed_columns cc on cc.object_id = object_id(col.TABLE_SCHEMA + '.' + col.TABLE_NAME) and cc.column_id = columnproperty(object_id(col.TABLE_SCHEMA + '.' + col.TABLE_NAME), col.COLUMN_NAME, 'ColumnId')
         left join (select k.COLUMN_NAME, k.TABLE_NAME, iif(k.CONSTRAINT_NAME is null, 0, 1) as pk
                    from information_schema.TABLE_CONSTRAINTS AS c
                    join information_schema.KEY_COLUMN_USAGE AS k on c.TABLE_NAME = k.TABLE_NAME
                 		and c.CONSTRAINT_CATALOG = k.CONSTRAINT_CATALOG
                 		and c.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
                 		and c.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                    where c.CONSTRAINT_TYPE = 'PRIMARY KEY'
                   ) j on col.COLUMN_NAME = j.COLUMN_NAME and j.TABLE_NAME = col.TABLE_NAME
         where TABLE_SCHEMA = 'dbo' {{(tables is null ? string.Empty : $"and col.TABLE_NAME in ({sb})")}}
         order by col.TABLE_NAME, col.COLUMN_NAME
         """;
    }

    public async Task<ResultOrException<int>> Execute(string sql)
    {
        await using SqlConnectionResult conn = await Connect();

        if (conn.Exception is not null)
        {
            return new ResultOrException<int>(0, conn.Exception);
        }

        try
        {
            SqlCommand command = new(sql, conn.Connection);
            await using SqlDataReader reader = await command.ExecuteReaderAsync();
            return new ResultOrException<int>(1, null);
        }
        catch (Exception e)
        {
            return new ResultOrException<int>(0, e);
        }
    }

    public async Task<ResultOrException<List<SqlResultSetColumn>>> DescribeSelect(string database, string selectQuery)
    {
        await using SqlConnectionResult conn = await Connect();

        if (conn.Exception is not null)
        {
            return new ResultOrException<List<SqlResultSetColumn>>(null, conn.Exception);
        }
        
        SqlCommand command = new("exec sp_describe_first_result_set @tsql = @sql", conn.Connection);
        command.Parameters.AddWithValue("@sql", selectQuery);
        
        await using SqlDataReader reader = await command.ExecuteReaderAsync();
        List<SqlResultSetColumn> cols = [];

        int colPosShrink = 0;
        
        while (reader.Read())
        {
            bool isHidden = reader.GetBoolean(0);
            int colPosition = reader.GetInt32(1) - 1;
            string? name = reader.GetValue(2) as string;
            bool isNullable = reader.GetBoolean(3);
            string typeName = reader.GetString(5);

            if (!isHidden)
            {
                cols.Add(new SqlResultSetColumn(colPosition - colPosShrink, name, isNullable, typeName.ToSqlDbType()));
            }
            else
            {
                colPosShrink++;
            }
        }

        return new ResultOrException<List<SqlResultSetColumn>>(cols.OrderBy(x => x.Position).ToList(), null);
    }

    public async Task<ResultOrException<Dictionary<string, SqlTable>>> GetSchema(string dbName, List<string>? selectTables = null)
    {
        await using SqlConnectionResult conn = await Connect();

        if (conn.Exception is not null)
        {
            return new ResultOrException<Dictionary<string, SqlTable>>(null, conn.Exception);
        }

        SqlCommand command = new(SqlSchema(dbName, selectTables), conn.Connection);

        if (selectTables is not null)
        {
            for (int i = 0; i < selectTables.Count; i++)
            {
                command.Parameters.AddWithValue($"t{i}", selectTables[i]);
            }
        }
        
        await using SqlDataReader reader = await command.ExecuteReaderAsync();

        Dictionary<string, SqlTable> tables = new();
        
        while (reader.Read())
        {
            string tableName = reader.GetString(0);
            string columnName = reader.GetString(1);
            int ordinalPosition = reader.GetInt32(2);
            bool isNullable = reader.GetString(3) is "YES";
            string dataType = reader.GetString(4);
            bool isIdentity = reader.GetInt32(5) is 1;
            bool isComputed = reader.GetInt32(6) is 1;
            bool isPk = reader.GetInt32(7) is 1;
            string? computedSql = reader.GetValue(8) as string;
            
            if (!(Enum.TryParse(typeof(SqlDbType), dataType, true, out object? dataTypeObject) && dataTypeObject is SqlDbType dt))
            {
                continue;
            }
            
            SqlTableColumn column = new(columnName, ordinalPosition, isNullable, isIdentity, (SqlDbTypeExt)dt, [], isComputed, isPk, computedSql);
            
            if (tables.TryGetValue(tableName.ToLowerInvariant(), out SqlTable? table))
            {
                table.Columns.Add(column.Name.ToLowerInvariant(), column);
            }
            else
            {
                tables.TryAdd(tableName.ToLowerInvariant(), new SqlTable(tableName, new Dictionary<string, SqlTableColumn> { {column.Name.ToLowerInvariant(), column} }));
            }
        }

        return new ResultOrException<Dictionary<string, SqlTable>>(tables, null);
    }
    
    public async Task<ResultOrException<Dictionary<string, List<SqlForeignKey>>>> GetForeignKeys(IList<string> tables)
    {
        await using SqlConnectionResult conn = await Connect();

        if (conn.Exception is not null)
        {
            return new ResultOrException<Dictionary<string, List<SqlForeignKey>>>(null, conn.Exception);
        }

        SqlCommand command = new()
        {
            Connection = conn.Connection
        };
        
        string[] parameters = new string[tables.Count];
        
        for (int i = 0; i < tables.Count; i++)
        {
            parameters[i] = $"@p{i}";
            command.Parameters.AddWithValue(parameters[i], tables[i]);
        }

        command.CommandText = $"""
           select f.name as 'name', object_name(f.parent_object_id) as 'table', col_name(fc.parent_object_id,fc.parent_column_id) as 'column', object_name(t.object_id) as 'refTable', col_name(t.object_id,fc.referenced_column_id) as 'refColumn', f.is_disabled as 'disabled'
           from sys.foreign_keys as f
           cross join sys.foreign_key_columns as fc
           cross join sys.tables t
           where f.object_id = fc.constraint_object_id and t.object_id = fc.referenced_object_id and object_name(t.object_id) in ({string.Join(", ", parameters)})
       """;
        
        await using SqlDataReader reader = await command.ExecuteReaderAsync();

        Dictionary<string, List<SqlForeignKey>> foreignKeys = new();
        
        while (reader.Read())
        {
            string fkName = reader.GetString(0);
            string tableName = reader.GetString(1);
            string column = reader.GetString(2);
            string refTable  = reader.GetString(3);
            string refColumn = reader.GetString(4);
            bool notEnforced = reader.GetBoolean(5);

            SqlForeignKey key = new SqlForeignKey(fkName, tableName, column, refTable, refColumn, notEnforced);

            if (foreignKeys.TryGetValue(tableName.ToLowerInvariant(), out List<SqlForeignKey>? keys))
            {
                keys.Add(key);
            }
            else
            {
                foreignKeys.TryAdd(tableName.ToLowerInvariant(), [key]);
            }
        }

        return new ResultOrException<Dictionary<string, List<SqlForeignKey>>>(foreignKeys, null);
    }

    private async Task<SqlConnectionResult> Connect()
    {
        try
        {
            SqlConnection conn = new SqlConnection(connString);
            await conn.OpenAsync();
            return new SqlConnectionResult(conn, null);
        }
        catch (Exception e)
        {
            return new SqlConnectionResult(null, e);
        }
    }
}