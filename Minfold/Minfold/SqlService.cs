using System.Collections.Concurrent;
using System.Data;
using System.Text;
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

    static string sqlTableCreateScriptSql = """
                     DECLARE @table_name SYSNAME
                     SELECT @table_name = @tableName

                     DECLARE
                           @object_name SYSNAME
                         , @object_id INT

                     SELECT
                           @object_name = '[' + s.name + '].[' + o.name + ']'
                         , @object_id = o.[object_id]
                     FROM sys.objects o WITH (NOWAIT)
                     JOIN sys.schemas s WITH (NOWAIT) ON o.[schema_id] = s.[schema_id]
                     WHERE s.name + '.' + o.name = @table_name
                         AND o.[type] = 'U'
                         AND o.is_ms_shipped = 0

                     DECLARE @SQL NVARCHAR(MAX) = ''

                     ;WITH index_column AS
                     (
                         SELECT
                               ic.[object_id]
                             , ic.index_id
                             , ic.is_descending_key
                             , ic.is_included_column
                             , c.name
                         FROM sys.index_columns ic WITH (NOWAIT)
                         JOIN sys.columns c WITH (NOWAIT) ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
                         WHERE ic.[object_id] = @object_id
                     ),
                     fk_columns AS
                     (
                          SELECT
                               k.constraint_object_id
                             , cname = c.name
                             , rcname = rc.name
                         FROM sys.foreign_key_columns k WITH (NOWAIT)
                         JOIN sys.columns rc WITH (NOWAIT) ON rc.[object_id] = k.referenced_object_id AND rc.column_id = k.referenced_column_id
                         JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = k.parent_object_id AND c.column_id = k.parent_column_id
                         WHERE k.parent_object_id = @object_id
                     )
                     SELECT @SQL = 'CREATE TABLE ' + @object_name + CHAR(13) + '(' + CHAR(13) + STUFF((
                         SELECT CHAR(9) + ', [' + c.name + '] ' +
                             CASE WHEN c.is_computed = 1
                                 THEN 'AS ' + cc.[definition]
                                 ELSE UPPER(tp.name) +
                                     CASE WHEN tp.name IN ('varchar', 'char', 'varbinary', 'binary', 'text')
                                            THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR(5)) END + ')'
                                          WHEN tp.name IN ('nvarchar', 'nchar', 'ntext')
                                            THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + ')'
                                          WHEN tp.name IN ('datetime2', 'time2', 'datetimeoffset')
                                            THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'
                                         WHEN tp.name IN ('decimal', 'numeric')
                                            THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ',' + CAST(c.scale AS VARCHAR(5)) + ')'
                                         ELSE ''
                                     END +
                                     CASE WHEN c.collation_name IS NOT NULL THEN ' COLLATE ' + c.collation_name ELSE '' END +
                                     CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END +
                     				CASE WHEN dc.[definition] IS NOT NULL THEN ' CONSTRAINT ' + quotename(dc.name) + ' DEFAULT' + dc.[definition] ELSE '' END +
                     				CASE WHEN ck.[definition] IS NOT NULL THEN ' CONSTRAINT ' + quotename(ck.name) + ' CHECK' + ck.[definition] ELSE '' END +
                     				CASE WHEN ic.is_identity = 1 THEN ' IDENTITY(' + CAST(ISNULL(ic.seed_value, '0') AS VARCHAR) + ',' + CAST(ISNULL(ic.increment_value, '1') AS VARCHAR) + ')' ELSE '' END
                     		END + CHAR(13)
                         FROM sys.columns c WITH (NOWAIT)
                         JOIN sys.types tp WITH (NOWAIT) ON c.user_type_id = tp.user_type_id
                         LEFT JOIN sys.computed_columns cc WITH (NOWAIT) ON c.[object_id] = cc.[object_id] AND c.column_id = cc.column_id
                         LEFT JOIN sys.identity_columns ic WITH (NOWAIT) ON c.is_identity = 1 AND c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
                     	LEFT JOIN sys.default_constraints dc WITH (NOWAIT) ON c.default_object_id != 0 AND c.[object_id] = dc.parent_object_id AND c.column_id = dc.parent_column_id
                     	LEFT JOIN sys.check_constraints ck WITH (NOWAIT) ON c.[object_id] = ck.parent_object_id AND c.column_id = ck.parent_column_id
                        LEFT JOIN (
                         SELECT ic.column_id, ic.object_id
                         FROM sys.index_columns ic
                         JOIN sys.indexes i ON ic.object_id = i.object_id 
                             AND ic.index_id = i.index_id
                         WHERE i.is_primary_key = 1
                        ) pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id
                         WHERE c.[object_id] = @object_id
                         ORDER BY CASE WHEN pk.column_id IS NOT NULL THEN 0 ELSE 1 END, c.name COLLATE Latin1_General_CI_AS
                         FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, CHAR(9) + ' ')
                         + ISNULL((SELECT CHAR(9) + ', CONSTRAINT [' + k.name + '] PRIMARY KEY (' +
                                         (SELECT STUFF((
                                              SELECT ', [' + c.name + '] ' + CASE WHEN ic.is_descending_key = 1 THEN 'DESC' ELSE 'ASC' END
                                              FROM sys.index_columns ic WITH (NOWAIT)
                                              JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
                                              WHERE ic.is_included_column = 0
                                                  AND ic.[object_id] = k.parent_object_id
                                                  AND ic.index_id = k.unique_index_id
                                              ORDER BY c.name COLLATE Latin1_General_CI_AS
                                              FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, ''))
                                 + ')' + CHAR(13)
                                 FROM sys.key_constraints k WITH (NOWAIT)
                                 WHERE k.parent_object_id = @object_id
                                     AND k.[type] = 'PK'), '') + ')'  + CHAR(13)
                         + ISNULL((SELECT (
                             SELECT CHAR(13) +
                                  'ALTER TABLE ' + @object_name + ' WITH'
                                 + CASE WHEN fk.is_not_trusted = 1
                                     THEN ' NOCHECK'
                                     ELSE ' CHECK'
                                   END +
                                   ' ADD CONSTRAINT [' + fk.name  + '] FOREIGN KEY('
                                   + STUFF((
                                     SELECT ', [' + k.cname + ']'
                                     FROM fk_columns k
                                     WHERE k.constraint_object_id = fk.[object_id]
                                     ORDER BY k.cname COLLATE Latin1_General_CI_AS
                                     FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
                                    + ')' +
                                   ' REFERENCES [' + SCHEMA_NAME(ro.[schema_id]) + '].[' + ro.name + '] ('
                                   + STUFF((
                                     SELECT ', [' + k.rcname + ']'
                                     FROM fk_columns k
                                     WHERE k.constraint_object_id = fk.[object_id]
                                     ORDER BY k.rcname COLLATE Latin1_General_CI_AS
                                     FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
                                    + ')'
                                 + CASE WHEN fk.is_not_for_replication = 1 THEN ' NOT FOR REPLICATION' ELSE '' END
                                 + CASE
                                     WHEN fk.delete_referential_action = 1 THEN ' ON DELETE CASCADE'
                                     WHEN fk.delete_referential_action = 2 THEN ' ON DELETE SET NULL'
                                     WHEN fk.delete_referential_action = 3 THEN ' ON DELETE SET DEFAULT'
                                     ELSE ''
                                   END
                                 + CASE
                                     WHEN fk.update_referential_action = 1 THEN ' ON UPDATE CASCADE'
                                     WHEN fk.update_referential_action = 2 THEN ' ON UPDATE SET NULL'
                                     WHEN fk.update_referential_action = 3 THEN ' ON UPDATE SET DEFAULT'
                                     ELSE ''
                                   END
                                 + CHAR(13) + 'ALTER TABLE ' + @object_name + ' CHECK CONSTRAINT [' + fk.name  + ']' + CHAR(13)
                             FROM sys.foreign_keys fk WITH (NOWAIT)
                             JOIN sys.objects ro WITH (NOWAIT) ON ro.[object_id] = fk.referenced_object_id
                             WHERE fk.parent_object_id = @object_id
                             ORDER BY fk.name COLLATE Latin1_General_CI_AS
                             FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)')), '')
                         + ISNULL(((SELECT
                              CHAR(13) + 'CREATE' + CASE WHEN i.is_unique = 1 THEN ' UNIQUE' ELSE '' END
                                     + ' NONCLUSTERED INDEX [' + i.name + '] ON ' + @object_name + ' (' +
                                     STUFF((
                                     SELECT ', [' + c.name + ']' + CASE WHEN c.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
                                     FROM index_column c
                                     WHERE c.is_included_column = 0
                                         AND c.index_id = i.index_id
                                     ORDER BY c.name COLLATE Latin1_General_CI_AS
                                     FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')'
                                     + ISNULL(CHAR(13) + 'INCLUDE (' +
                                         STUFF((
                                         SELECT ', [' + c.name + ']'
                                         FROM index_column c
                                         WHERE c.is_included_column = 1
                                             AND c.index_id = i.index_id
                                         ORDER BY c.name COLLATE Latin1_General_CI_AS
                                         FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')', '')  + CHAR(13)
                             FROM sys.indexes i WITH (NOWAIT)
                             WHERE i.[object_id] = @object_id
                                 AND i.is_primary_key = 0
                                 AND i.[type] = 2
                             ORDER BY i.name COLLATE Latin1_General_CI_AS
                             FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
                         ), '')

                     SELECT @SQL
                     """;
    
    public async Task<ResultOrException<string>> SqlTableCreateScript(string tableName)
    {
        await using SqlConnectionResult conn = await Connect();

        if (conn.Exception is not null)
        {
            return new ResultOrException<string>(null, conn.Exception);
        }
        
        SqlCommand command = new SqlCommand(sqlTableCreateScriptSql, conn.Connection);
        command.Parameters.AddWithValue("@tableName", tableName);
        
        await using SqlDataReader reader = await command.ExecuteReaderAsync();

        int colPosShrink = 0;
        string? sqlScript = null;
        
        while (reader.Read())
        {
            sqlScript = reader.GetValue(0) as string;
        }

        return new ResultOrException<string>(sqlScript ?? string.Empty, null);
    }

    static string SqlColumnCount(string dbName, List<string> tables)
    {
        StringBuilder sb = new StringBuilder();
        
        for (int i = 0; i < tables.Count; i++)
        {
            if (i == tables.Count - 1)
            {
                sb.Append($"@t{i}");
                continue;
            }
            
            sb.Append($"@t{i}, ");
        }
        
        return $$"""
                 select TABLE_NAME as tbl, count(c.COLUMN_NAME) as cols
                 from information_schema.columns c
                 where c.TABLE_CATALOG = '{{dbName}}' and c.TABLE_SCHEMA = 'dbo' and c.TABLE_NAME in ({{sb}})
                 group by c.TABLE_NAME
                 """;
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
                   use [{{dbName}}]
                   select col.TABLE_NAME, 
                   col.COLUMN_NAME, 
                   col.ORDINAL_POSITION, 
                   col.IS_NULLABLE, 
                   col.DATA_TYPE,
                   columnproperty(object_id(col.TABLE_SCHEMA + '.' + col.TABLE_NAME), col.COLUMN_NAME, 'IsIdentity') as IS_IDENTITY,
                   columnproperty(object_id(col.TABLE_SCHEMA + '.' + col.TABLE_NAME), col.COLUMN_NAME, 'IsComputed') as IS_COMPUTED,
                   isnull(j.pk, cast(0 as bit)) as IS_PRIMARY,
                   cc.definition as COMPUTED_DEFINITION, 
                   case
                       when DATA_TYPE in('datetime2', 'datetime', 'time', 'timestamp') then DATETIME_PRECISION
                       when DATA_TYPE in ('varchar', 'nvarchar', 'text', 'binary', 'varbinary', 'blob') then CHARACTER_MAXIMUM_LENGTH
                       else null
                   end as LENGTH_OR_PRECISION
                    from information_schema.COLUMNS col
                    inner join sys.objects o 
                        on object_id(col.TABLE_SCHEMA + '.' + col.TABLE_NAME) = o.object_id
                        and o.type = 'U'
                        and cast(case 
                            when o.is_ms_shipped = 1 then 1
                            when exists (
                                select 1 
                                from sys.extended_properties 
                                where major_id = o.object_id 
                                    and minor_id = 0 
                                    and class = 1 
                                    and name = N'microsoft_database_tools_support'
                            ) then 1
                            else 0
                        end as bit) = 0
                    left join sys.computed_columns cc 
                        on cc.object_id = object_id(col.TABLE_SCHEMA + '.' + col.TABLE_NAME) 
                        and cc.column_id = columnproperty(object_id(col.TABLE_SCHEMA + '.' + col.TABLE_NAME), col.COLUMN_NAME, 'ColumnId')
                    left join (
                        select k.COLUMN_NAME, 
                               k.TABLE_NAME, 
                               iif(k.CONSTRAINT_NAME is null, 0, 1) as pk
                        from information_schema.TABLE_CONSTRAINTS AS c
                        join information_schema.KEY_COLUMN_USAGE AS k 
                            on c.TABLE_NAME = k.TABLE_NAME
                            and c.CONSTRAINT_CATALOG = k.CONSTRAINT_CATALOG
                            and c.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
                            and c.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                        where c.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    ) j on col.COLUMN_NAME = j.COLUMN_NAME and j.TABLE_NAME = col.TABLE_NAME
                    where TABLE_SCHEMA = 'dbo' 
                        {{(tables is null ? string.Empty : $"and col.TABLE_NAME in ({sb})")}}
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
            SqlCommand command = new SqlCommand(sql, conn.Connection);
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
        
        SqlCommand command = new SqlCommand("exec sp_describe_first_result_set @tsql = @sql", conn.Connection);
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

    public async Task<ResultOrException<ConcurrentDictionary<string, int>>> GetTableColumnCount(string dbName, List<string> tables)
    {
        await using SqlConnectionResult conn = await Connect();

        if (conn.Exception is not null)
        {
            return new ResultOrException<ConcurrentDictionary<string, int>>(null, conn.Exception);
        }
        
        SqlCommand command = new SqlCommand(SqlColumnCount(dbName, tables), conn.Connection);
        
        for (int i = 0; i < tables.Count; i++)
        {
            command.Parameters.AddWithValue($"t{i}", tables[i]);
        }
        
        await using SqlDataReader reader = await command.ExecuteReaderAsync();

        ConcurrentDictionary<string, int> tablesMap = new ConcurrentDictionary<string, int>();
        
        while (reader.Read())
        {
            string tableName = reader.GetString(0);
            int columns = reader.GetInt32(1);
           
            tablesMap.TryAdd(tableName.ToLowerInvariant(), columns);
        }

        return new ResultOrException<ConcurrentDictionary<string, int>>(tablesMap, null);
    }

    public async Task<ResultOrException<ConcurrentDictionary<string, SqlTable>>> GetSchema(string dbName, List<string>? selectTables = null, List<string>? excludeTables = null)
    {
        await using SqlConnectionResult conn = await Connect();

        if (conn.Exception is not null)
        {
            return new ResultOrException<ConcurrentDictionary<string, SqlTable>>(null, conn.Exception);
        }

        string sql = SqlSchema(dbName, selectTables);
        SqlCommand command = new SqlCommand(sql, conn.Connection);
   
        if (selectTables is not null)
        {
            for (int i = 0; i < selectTables.Count; i++)
            {
                command.Parameters.AddWithValue($"t{i}", selectTables[i]);
            }
        }
        
        await using SqlDataReader reader = await command.ExecuteReaderAsync();

        ConcurrentDictionary<string, SqlTable> tables = new ConcurrentDictionary<string, SqlTable>();
        HashSet<string> excludeSet = excludeTables?.Select(t => t.ToLowerInvariant()).ToHashSet() ?? new HashSet<string>();
        
        while (reader.Read())
        {
            string tableName = reader.GetString(0);
            
            // Skip excluded tables
            if (excludeSet.Contains(tableName.ToLowerInvariant()))
            {
                continue;
            }
            
            string columnName = reader.GetString(1);
            int ordinalPosition = reader.GetInt32(2);
            bool isNullable = reader.GetString(3) is "YES";
            string dataType = reader.GetString(4);
            bool isIdentity = reader.GetInt32(5) is 1;
            bool isComputed = reader.GetInt32(6) is 1;
            bool isPk = reader.GetInt32(7) is 1;
            string? computedSql = reader.GetValue(8) as string;
            int? lengthOrPrecision = reader.GetValue(9) as int?;
            
            if (!(Enum.TryParse(typeof(SqlDbType), dataType, true, out object? dataTypeObject) && dataTypeObject is SqlDbType dt))
            {
                continue;
            }
            
            SqlTableColumn column = new SqlTableColumn(columnName, ordinalPosition, isNullable, isIdentity, (SqlDbTypeExt)dt, [], isComputed, isPk, computedSql, lengthOrPrecision);
            
            if (tables.TryGetValue(tableName.ToLowerInvariant(), out SqlTable? table))
            {
                table.Columns.Add(column.Name.ToLowerInvariant(), column);
            }
            else
            {
                tables.TryAdd(tableName.ToLowerInvariant(), new SqlTable(tableName, new Dictionary<string, SqlTableColumn> { {column.Name.ToLowerInvariant(), column} }));
            }
        }

        return new ResultOrException<ConcurrentDictionary<string, SqlTable>>(tables, null);
    }
    
    public async Task<ResultOrException<Dictionary<string, List<SqlForeignKey>>>> GetForeignKeys(IList<string> tables)
    {
        await using SqlConnectionResult conn = await Connect();

        if (conn.Exception is not null)
        {
            return new ResultOrException<Dictionary<string, List<SqlForeignKey>>>(null, conn.Exception);
        }

        SqlCommand command = new SqlCommand
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
           select f.name as 'name', object_name(f.parent_object_id) as 'table', col_name(fc.parent_object_id,fc.parent_column_id) as 'column', object_name(t.object_id) as 'refTable', col_name(t.object_id,fc.referenced_column_id) as 'refColumn', f.is_not_trusted as 'notEnforced', f.is_not_for_replication as 'notForReplication', f.delete_referential_action as 'deleteAction', f.update_referential_action as 'updateAction'
           from sys.foreign_keys as f
           cross join sys.foreign_key_columns as fc
           cross join sys.tables t
           where f.object_id = fc.constraint_object_id and t.object_id = fc.referenced_object_id and object_name(t.object_id) in ({string.Join(", ", parameters)})
       """;
        
        await using SqlDataReader reader = await command.ExecuteReaderAsync();

        Dictionary<string, List<SqlForeignKey>> foreignKeys = new Dictionary<string, List<SqlForeignKey>>();
        
        while (reader.Read())
        {
            string fkName = reader.GetString(0);
            string tableName = reader.GetString(1);
            string column = reader.GetString(2);
            string refTable  = reader.GetString(3);
            string refColumn = reader.GetString(4);
            bool notEnforced = reader.GetBoolean(5);
            bool notForReplication = reader.GetBoolean(6);
            int deleteAction = Convert.ToInt32(reader.GetValue(7));
            int updateAction = Convert.ToInt32(reader.GetValue(8));

            SqlForeignKey key = new SqlForeignKey(fkName, tableName, column, refTable, refColumn, notEnforced, notForReplication, deleteAction, updateAction);

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