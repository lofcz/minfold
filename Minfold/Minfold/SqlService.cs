using System.Data;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.Data.SqlClient;
namespace Minfold;

internal class SqlService
{
    private string connString;

    public SqlService(string connString)
    {
        this.connString = connString;
    }
    
    public async Task<Dictionary<string, SqlTable>> GetSchema(string dbName)
    {
        await using SqlConnection conn = Connect();
        await conn.OpenAsync();

        SqlCommand command = new($$"""
            use {{dbName}}
            select TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE,
                   columnproperty(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') as IS_IDENTITY,
                   columnproperty(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsComputed') as IS_COMPUTED
            from information_schema.columns
            where TABLE_SCHEMA = 'dbo' 
            order by TABLE_NAME, COLUMN_NAME
        """, conn);
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
            
            if (!(Enum.TryParse(typeof(SqlDbType), dataType, true, out object? dataTypeObject) && dataTypeObject is SqlDbType dt))
            {
                continue;
            }
            
            SqlTableColumn column = new(columnName, ordinalPosition, isNullable, isIdentity, (SqlDbTypeExt)dt, [], isComputed);
            
            if (tables.TryGetValue(tableName.ToLowerInvariant(), out SqlTable? table))
            {
                table.Columns.Add(column.Name.ToLowerInvariant(), column);
            }
            else
            {
                tables.TryAdd(tableName.ToLowerInvariant(), new SqlTable(tableName, new Dictionary<string, SqlTableColumn> { {column.Name.ToLowerInvariant(), column} }));
            }
        }

        return tables;
    }
    
    public async Task<Dictionary<string, List<SqlForeignKey>>> GetForeignKeys(IList<string> tables)
    {
        await using SqlConnection conn = Connect();
        await conn.OpenAsync();

        SqlCommand command = new()
        {
            Connection = conn
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

        return foreignKeys;
    }

    private SqlConnection Connect()
    {
        return new SqlConnection(connString);
    }
}