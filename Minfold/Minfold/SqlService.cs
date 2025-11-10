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
    
    static string SqlSchema(string dbName, List<string>? tables, List<string> schemas)
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

        // Build schema filter
        StringBuilder schemaFilter = new StringBuilder();
        for (int i = 0; i < schemas.Count; i++)
        {
            if (i == 0)
            {
                schemaFilter.Append($"col.TABLE_SCHEMA = '{schemas[i]}'");
            }
            else
            {
                schemaFilter.Append($" OR col.TABLE_SCHEMA = '{schemas[i]}'");
            }
        }

        return $$"""
                   use [{{dbName}}]
                   select col.TABLE_SCHEMA,
                   col.TABLE_NAME, 
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
                   end as LENGTH_OR_PRECISION,
                   ic.seed_value as IDENTITY_SEED,
                   ic.increment_value as IDENTITY_INCR,
                   dc.name as DEFAULT_CONSTRAINT_NAME,
                   dc.definition as DEFAULT_CONSTRAINT_VALUE
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
                    left join sys.identity_columns ic
                        on ic.object_id = object_id(col.TABLE_SCHEMA + '.' + col.TABLE_NAME)
                        and ic.column_id = columnproperty(object_id(col.TABLE_SCHEMA + '.' + col.TABLE_NAME), col.COLUMN_NAME, 'ColumnId')
                    left join sys.default_constraints dc
                        on dc.parent_object_id = object_id(col.TABLE_SCHEMA + '.' + col.TABLE_NAME)
                        and dc.parent_column_id = columnproperty(object_id(col.TABLE_SCHEMA + '.' + col.TABLE_NAME), col.COLUMN_NAME, 'ColumnId')
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
                    where ({{schemaFilter}})
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

    public async Task<ResultOrException<ConcurrentDictionary<string, SqlSequence>>> GetSequences(string dbName, List<string>? schemas = null)
    {
        await using SqlConnectionResult conn = await Connect();

        if (conn.Exception is not null)
        {
            return new ResultOrException<ConcurrentDictionary<string, SqlSequence>>(null, conn.Exception);
        }

        List<string> allowedSchemas = schemas ?? ["dbo"];
        StringBuilder schemaFilter = new StringBuilder();
        for (int i = 0; i < allowedSchemas.Count; i++)
        {
            if (i == 0)
            {
                schemaFilter.Append($"SCHEMA_NAME(s.schema_id) = '{allowedSchemas[i]}'");
            }
            else
            {
                schemaFilter.Append($" OR SCHEMA_NAME(s.schema_id) = '{allowedSchemas[i]}'");
            }
        }

        string sql = $"""
            USE [{dbName}];
            SELECT 
                SCHEMA_NAME(s.schema_id) AS SEQUENCE_SCHEMA,
                s.name AS SEQUENCE_NAME,
                t.name AS DATA_TYPE,
                s.start_value AS START_VALUE,
                s.increment AS INCREMENT,
                s.minimum_value AS MIN_VALUE,
                s.maximum_value AS MAX_VALUE,
                s.is_cycling AS CYCLE,
                s.cache_size AS CACHE_SIZE,
                OBJECT_DEFINITION(OBJECT_ID(SCHEMA_NAME(s.schema_id) + '.' + s.name, 'SO')) AS DEFINITION
            FROM sys.sequences s
            INNER JOIN sys.types t ON s.user_type_id = t.user_type_id
            WHERE s.is_ms_shipped = 0
                AND ({schemaFilter})
            ORDER BY s.name
            """;

        SqlCommand command = new SqlCommand(sql, conn.Connection);
        await using SqlDataReader reader = await command.ExecuteReaderAsync();

        ConcurrentDictionary<string, SqlSequence> sequences = new ConcurrentDictionary<string, SqlSequence>(StringComparer.OrdinalIgnoreCase);

        while (await reader.ReadAsync())
        {
            string sequenceSchema = reader.GetString(0);
            string sequenceName = reader.GetString(1);
            string dataType = reader.GetString(2);
            // Sequence values can be int or bigint depending on sequence type, so use Convert.ToInt64 to handle both
            long? startValue = reader.IsDBNull(3) ? null : Convert.ToInt64(reader.GetValue(3));
            long? increment = reader.IsDBNull(4) ? null : Convert.ToInt64(reader.GetValue(4));
            long? minValue = reader.IsDBNull(5) ? null : Convert.ToInt64(reader.GetValue(5));
            long? maxValue = reader.IsDBNull(6) ? null : Convert.ToInt64(reader.GetValue(6));
            bool cycle = reader.GetBoolean(7);
            long? cacheSize = reader.IsDBNull(8) ? null : Convert.ToInt64(reader.GetValue(8));
            string? definition = reader.IsDBNull(9) ? null : reader.GetString(9);

            SqlSequence sequence = new SqlSequence(sequenceName, dataType, startValue, increment, minValue, maxValue, cycle, cacheSize, definition, sequenceSchema);
            sequences.TryAdd(sequenceName.ToLowerInvariant(), sequence);
        }

        return new ResultOrException<ConcurrentDictionary<string, SqlSequence>>(sequences, null);
    }

    public async Task<ResultOrException<ConcurrentDictionary<string, SqlStoredProcedure>>> GetStoredProcedures(string dbName, List<string>? schemas = null)
    {
        await using SqlConnectionResult conn = await Connect();

        if (conn.Exception is not null)
        {
            return new ResultOrException<ConcurrentDictionary<string, SqlStoredProcedure>>(null, conn.Exception);
        }

        List<string> allowedSchemas = schemas ?? ["dbo"];
        StringBuilder schemaFilter = new StringBuilder();
        for (int i = 0; i < allowedSchemas.Count; i++)
        {
            if (i == 0)
            {
                schemaFilter.Append($"s.name = '{allowedSchemas[i]}'");
            }
            else
            {
                schemaFilter.Append($" OR s.name = '{allowedSchemas[i]}'");
            }
        }

        string sql = $"""
            USE [{dbName}];
            SELECT 
                s.name AS PROCEDURE_SCHEMA,
                p.name AS PROCEDURE_NAME,
                OBJECT_DEFINITION(p.object_id) AS DEFINITION
            FROM sys.procedures p
            INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
            WHERE p.type = 'P'
                AND p.is_ms_shipped = 0
                AND ({schemaFilter})
            ORDER BY p.name
            """;

        SqlCommand command = new SqlCommand(sql, conn.Connection);
        await using SqlDataReader reader = await command.ExecuteReaderAsync();

        ConcurrentDictionary<string, SqlStoredProcedure> procedures = new ConcurrentDictionary<string, SqlStoredProcedure>(StringComparer.OrdinalIgnoreCase);

        while (await reader.ReadAsync())
        {
            string procedureSchema = reader.GetString(0);
            string procedureName = reader.GetString(1);
            string definition = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);

            SqlStoredProcedure procedure = new SqlStoredProcedure(procedureName, definition, procedureSchema);
            procedures.TryAdd(procedureName.ToLowerInvariant(), procedure);
        }

        return new ResultOrException<ConcurrentDictionary<string, SqlStoredProcedure>>(procedures, null);
    }

    public async Task<ResultOrException<ConcurrentDictionary<string, SqlTable>>> GetSchema(string dbName, List<string>? selectTables = null, List<string>? excludeTables = null, List<string>? schemas = null)
    {
        await using SqlConnectionResult conn = await Connect();

        if (conn.Exception is not null)
        {
            return new ResultOrException<ConcurrentDictionary<string, SqlTable>>(null, conn.Exception);
        }

        List<string> allowedSchemas = schemas ?? ["dbo"];
        string sql = SqlSchema(dbName, selectTables, allowedSchemas);
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
            string tableSchema = reader.GetString(0);
            string tableName = reader.GetString(1);
            
            // Skip excluded tables
            if (excludeSet.Contains(tableName.ToLowerInvariant()))
            {
                continue;
            }
            
            string columnName = reader.GetString(2);
            int ordinalPosition = reader.GetInt32(3);
            bool isNullable = reader.GetString(4) is "YES";
            string dataType = reader.GetString(5);
            bool isIdentity = reader.GetInt32(6) is 1;
            bool isComputed = reader.GetInt32(7) is 1;
            bool isPk = reader.GetInt32(8) is 1;
            string? computedSql = reader.GetValue(9) as string;
            int? lengthOrPrecision = reader.GetValue(10) as int?;
            
            // Read identity seed and increment (sql_variant, need to convert)
            long? identitySeed = null;
            long? identityIncrement = null;
            if (isIdentity)
            {
                object? seedValue = reader.GetValue(11);
                object? incrementValue = reader.GetValue(12);
                
                if (seedValue != null && seedValue != DBNull.Value)
                {
                    // sql_variant can be various numeric types, convert to long
                    identitySeed = Convert.ToInt64(seedValue);
                }
                
                if (incrementValue != null && incrementValue != DBNull.Value)
                {
                    identityIncrement = Convert.ToInt64(incrementValue);
                }
            }
            
            // Read default constraint information
            string? defaultConstraintName = reader.GetValue(13) as string;
            string? defaultConstraintValue = reader.GetValue(14) as string;
            
            SqlDbTypeExt sqlDbTypeExt;
            if (Enum.TryParse(typeof(SqlDbType), dataType, true, out object? dataTypeObject) && dataTypeObject is SqlDbType dt)
            {
                sqlDbTypeExt = (SqlDbTypeExt)dt;
            }
            else
            {
                // Fallback for types not in SqlDbType enum (e.g., Json in SQL Server 2025)
                sqlDbTypeExt = dataType.ToSqlDbType();
                if (sqlDbTypeExt == SqlDbTypeExt.Unknown)
                {
                    continue;
                }
            }
            
            SqlTableColumn column = new SqlTableColumn(columnName, ordinalPosition, isNullable, isIdentity, sqlDbTypeExt, [], isComputed, isPk, computedSql, lengthOrPrecision, identitySeed, identityIncrement, defaultConstraintName, defaultConstraintValue);
            
            if (tables.TryGetValue(tableName.ToLowerInvariant(), out SqlTable? table))
            {
                table.Columns.Add(column.Name.ToLowerInvariant(), column);
            }
            else
            {
                tables.TryAdd(tableName.ToLowerInvariant(), new SqlTable(tableName, new Dictionary<string, SqlTableColumn> { {column.Name.ToLowerInvariant(), column} }, new List<SqlIndex>(), tableSchema));
            }
        }
        
        // Query indexes for all tables
        await reader.CloseAsync();
        foreach (KeyValuePair<string, SqlTable> tablePair in tables)
        {
            string tableName = tablePair.Key;
            SqlTable table = tablePair.Value;
            
            if (excludeSet.Contains(tableName.ToLowerInvariant()))
            {
                continue;
            }
            
            string indexQuery = $"""
                SELECT 
                    i.name AS INDEX_NAME,
                    i.is_unique,
                    STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS COLUMN_NAMES
                FROM sys.indexes i
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE i.object_id = OBJECT_ID('[{table.Schema}].[{tableName}]')
                    AND i.is_primary_key = 0
                    AND i.type = 2
                GROUP BY i.name, i.is_unique
                """;
            
            SqlCommand indexCommand = new SqlCommand($"USE [{dbName}]; {indexQuery}", conn.Connection);
            await using SqlDataReader indexReader = await indexCommand.ExecuteReaderAsync();
            
            List<SqlIndex> indexes = new List<SqlIndex>();
            while (await indexReader.ReadAsync())
            {
                string indexName = indexReader.GetString(0);
                bool isUnique = indexReader.GetBoolean(1);
                string columnNamesStr = indexReader.GetString(2);
                List<string> columnNames = columnNamesStr.Split(',').ToList();
                
                indexes.Add(new SqlIndex(indexName, tableName, columnNames, isUnique, table.Schema));
            }
            
            await indexReader.CloseAsync();
            
            if (tables.TryGetValue(tableName.ToLowerInvariant(), out SqlTable? tableToUpdate))
            {
                tables[tableName.ToLowerInvariant()] = tableToUpdate with { Indexes = indexes };
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
        
        // If no tables provided, return empty result (avoid SQL syntax error with empty IN clause)
        if (tables.Count == 0)
        {
            return new ResultOrException<Dictionary<string, List<SqlForeignKey>>>(new Dictionary<string, List<SqlForeignKey>>(), null);
        }
        
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