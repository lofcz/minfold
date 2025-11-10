#nullable enable // Enable nullable reference types

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis; // For StringSyntaxAttribute if needed later
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading; // For CancellationToken, Interlocked
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlPocoGeneratorTool
{
    /// <summary>
    /// Represents a database column containing JSON, intended to be deserialized into T.
    /// This is primarily a marker type for code generation.
    /// </summary>
    /// <typeparam name="T">The C# type the JSON should represent.</typeparam>
    public struct Json<T>
    {
        public string? RawJson { get; set; }
        public override string ToString() => RawJson ?? string.Empty;
    }

    public class SqlModelGenerator
    {
        private readonly string _connectionString;
        private int _classCounter = 0;

        // Stores the results of the description phase and generation phase
        // Key: Unique ID (e.g., "poco_0", "poco_1")
        private readonly ConcurrentDictionary<string, PocoInProgress> _generationResults =
            new ConcurrentDictionary<string, PocoInProgress>();

        // --- Helper Classes ---

        private class PocoInProgress
        {
            public string UniqueId { get; }
            // Use StringSyntaxAttribute if targeting .NET 7+ for better tooling
            // [StringSyntax("Sql")]
            public string Sql { get; }
            public string DesiredBaseClassName { get; }

            // Backing field for thread-safe status updates
            internal volatile int _statusAsInt = (int)GenerationStatus.Pending;
            public GenerationStatus Status => (GenerationStatus)_statusAsInt;

            public List<ColumnInfo>? Schema { get; set; }
            // Stores confirmed JSON columns needing nested generation
            public List<JsonColumnInfo> JsonColumns { get; } = new List<JsonColumnInfo>();
            public string? GeneratedClassName { get; set; }
            public string? GeneratedCode { get; set; }
            public List<string> Errors { get; } = new List<string>();
            public CancellationTokenSource? Cts { get; set; } // Optional cancellation support

            public PocoInProgress(string uniqueId, string sql, string desiredBaseClassName)
            {
                 UniqueId = uniqueId;
                 Sql = sql;
                 DesiredBaseClassName = desiredBaseClassName;
            }
        }

        // Stores info about a JSON column confirmed by AST analysis of the current SELECT list
        private class JsonColumnInfo
        {
            public string SchemaColumnNameOrPlaceholder { get; set; } = ""; // Name from sp_describe OR placeholder (key for matching during generation)
            public bool WithoutArrayWrapper { get; set; } = false;
            // [StringSyntax("Sql")]
            public string DescriptionSql { get; set; } = ""; // SQL used to describe the inner structure
            public string NestedPocoUniqueId { get; set; } = ""; // Link to the PocoInProgress for the nested type
            public string? NestedGeneratedClassName { get; set; } // Filled in during generation phase
        }

        // Stores info about a column from sp_describe_first_result_set
        private class ColumnInfo
        {
            public string? Name { get; set; }
            public string? SqlTypeName { get; set; }
            public bool IsNullable { get; set; }
            public int Ordinal { get; set; } // Store original position (0-based)
            public bool IsPotentialJsonContainer { get; set; } // Flag based on SqlTypeName
        }

        private enum GenerationStatus
        {
            Pending,
            Describing,
            Described,
            Generating,
            Generated,
            Failed
        }

        // --- Constructor ---

        public SqlModelGenerator(string connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        // --- Public Entry Point ---

        /// <summary>
        /// Generates C# POCO class definitions for the given SQL query.
        /// </summary>
        /// <param name="sql">The T-SQL SELECT query.</param>
        /// <param name="rootClassName">The base name for the root generated class.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A string containing the generated C# code, including all necessary classes.</returns>
        /// <exception cref="ArgumentNullException">Thrown if sql is null.</exception>
        /// <exception cref="ArgumentException">Thrown if sql is empty or whitespace.</exception>
        /// <exception cref="OperationCanceledException">Thrown if cancellation is requested.</exception>
        /// <exception cref="SqlPocoGenerationException">Thrown for critical errors during generation.</exception>
        public async Task<string> GenerateModelClassesAsync(
            // [StringSyntax("Sql")] // Requires .NET 7+
            string sql,
            string rootClassName = "GeneratedPoco",
            CancellationToken cancellationToken = default)
        {
            if (sql == null) throw new ArgumentNullException(nameof(sql));
            if (string.IsNullOrWhiteSpace(sql)) throw new ArgumentException("SQL query cannot be empty.", nameof(sql));

            _generationResults.Clear();
            _classCounter = 0; // Reset counter for each top-level call

            var initialId = $"poco_{_classCounter}"; // Start counter at 0
            var initialPoco = new PocoInProgress(initialId, sql, rootClassName) { Cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken) };

            if (!_generationResults.TryAdd(initialId, initialPoco))
            {
                // Should not happen with a clear dictionary
                throw new InvalidOperationException("Failed to initialize generation process.");
            }

            var queue = new Queue<PocoInProgress>();
            queue.Enqueue(initialPoco);

            try
            {
                // Phase 1: Describe Schemas (BFS, sequential processing)
                await ProcessDescriptionQueueAsync(queue, cancellationToken);

                cancellationToken.ThrowIfCancellationRequested();

                // Phase 2: Generate Code (Recursive with Memoization)
                var processedIds = new HashSet<string>(); // To avoid duplicates in final output

                // Trigger generation starting from the root
                await GeneratePocoCodeRecursiveAsync(initialId, cancellationToken);

                cancellationToken.ThrowIfCancellationRequested();

                // Collect successfully generated code and errors
                var allGeneratedCode = new List<string>();
                var allErrors = new List<string>();

                foreach (var result in _generationResults.Values.OrderBy(r => r.GeneratedClassName ?? r.DesiredBaseClassName)) // Order for consistency
                {
                    if (result.Status == GenerationStatus.Generated && !string.IsNullOrEmpty(result.GeneratedCode) && !string.IsNullOrEmpty(result.GeneratedClassName) && processedIds.Add(result.GeneratedClassName))
                    {
                        allGeneratedCode.Add(result.GeneratedCode);
                    }
                    if (result.Errors.Any())
                    {
                        // Collect errors for potential reporting
                        allErrors.AddRange(result.Errors.Select(e => $"Error in '{result.DesiredBaseClassName}' (ID: {result.UniqueId}): {e}"));
                    }
                }

                if (!allGeneratedCode.Any())
                {
                    string errorMessage = "No POCO classes were successfully generated.";
                    if (allErrors.Any())
                    {
                        errorMessage += " Errors occurred:\n" + string.Join("\n", allErrors);
                    }
                    else
                    {
                         errorMessage += " The query might not produce results or encountered configuration issues.";
                    }
                    // In an analyzer, create a diagnostic. Here, we throw or return an error string.
                    throw new SqlPocoGenerationException(errorMessage);
                    // Or: return $"// {errorMessage}";
                }

                // Assemble the final code string
                var finalOutput = new StringBuilder();
                finalOutput.AppendLine("using System;");
                finalOutput.AppendLine("using System.Collections.Generic;");
                finalOutput.AppendLine($"using {typeof(Json<>).Namespace}; // For Json<T>"); // Use actual namespace
                finalOutput.AppendLine();
                finalOutput.AppendLine("namespace GeneratedModels // TODO: Make namespace configurable");
                finalOutput.AppendLine("{");

                // Use '\n' for joining lines, suitable for source code.
                finalOutput.Append(string.Join("\n\n", allGeneratedCode));

                finalOutput.AppendLine("\n}"); // Add newline before closing brace

                return finalOutput.ToString();
            }
            catch (OperationCanceledException)
            {
                 // Log cancellation if needed, then rethrow
                 throw;
            }
            catch (Exception ex) when (ex is not SqlPocoGenerationException and not OperationCanceledException)
            {
                // Wrap unexpected exceptions
                throw new SqlPocoGenerationException($"An unexpected error occurred during POCO generation: {ex.Message}", ex);
            }
            finally
            {
                // Clean up cancellation token sources
                foreach(var poco in _generationResults.Values)
                {
                    poco.Cts?.Dispose();
                }
            }
        }

        // --- Phase 1: Schema Description (Sequential Processing) ---

        private async Task ProcessDescriptionQueueAsync(Queue<PocoInProgress> queue, CancellationToken cancellationToken)
        {
            // Keep sequential processing
            while (queue.Count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var currentPoco = queue.Dequeue();
                await DescribeSinglePocoAsync(currentPoco, queue, cancellationToken);
            }
        }

        private async Task DescribeSinglePocoAsync(PocoInProgress currentPoco, Queue<PocoInProgress> queue, CancellationToken cancellationToken)
        {
             if (Interlocked.CompareExchange(ref currentPoco._statusAsInt, (int)GenerationStatus.Describing, (int)GenerationStatus.Pending) != (int)GenerationStatus.Pending) { return; }
             var token = currentPoco.Cts?.Token ?? cancellationToken;

             try
             {
                 token.ThrowIfCancellationRequested();

                 // 1. Describe Schema & Identify Potential JSON Containers
                 currentPoco.Schema = await DescribeSchemaAsync(currentPoco.Sql, token);
                 if (currentPoco.Schema == null) { LogAndFail(currentPoco, "Schema description failed."); return; }
                 if (!currentPoco.Schema.Any()) { currentPoco.Errors.Add("Warning: Query described no columns."); Interlocked.CompareExchange(ref currentPoco._statusAsInt, (int)GenerationStatus.Described, (int)GenerationStatus.Describing); return; }
                 foreach(var col in currentPoco.Schema) { col.IsPotentialJsonContainer = IsPotentialJsonType(col.SqlTypeName); }

                 token.ThrowIfCancellationRequested();

                 // 2. Parse SQL
                 var parser = new TSql160Parser(true, SqlEngineType.All);
                 IList<ParseError> errors;
                 TSqlFragment sqlFragment = parser.Parse(new StringReader(currentPoco.Sql), out errors);
                 if (errors.Any()) { LogAndFail(currentPoco, $"SQL Parsing failed: {string.Join("; ", errors.Select(e => $"L{e.Line}/C{e.Column}: {e.Message}"))}"); return; }

                 token.ThrowIfCancellationRequested();

                 // 3. Visit AST to get details about each top-level select element
                 var selectElementVisitor = new SelectElementVisitor();
                 sqlFragment.Accept(selectElementVisitor);
                 // selectElementsInfo: List<SelectElementVisitor.SelectElementDetails> ordered by position
                 var selectElementsInfo = selectElementVisitor.SelectElements;

                 // 4. Correlate Schema Columns with Select Element Details by Ordinal Position
                 if (currentPoco.Schema.Count != selectElementsInfo.Count)
                 {
                     // This indicates a mismatch, possibly due to SELECT * expansion differences or parser issues.
                     // Log a warning or error. For now, proceed but correlation might be wrong.
                     currentPoco.Errors.Add($"Warning: Schema column count ({currentPoco.Schema.Count}) differs from parsed select element count ({selectElementsInfo.Count}). Correlation might be inaccurate.");
                 }

                 for (int i = 0; i < currentPoco.Schema.Count; i++)
                 {
                     token.ThrowIfCancellationRequested();
                     var schemaCol = currentPoco.Schema[i];

                     // Find corresponding select element detail by ordinal index
                     if (i >= selectElementsInfo.Count)
                     {
                         currentPoco.Errors.Add($"Warning: Could not find parsed select element details for schema column at index {i}.");
                         continue; // Skip if counts mismatch
                     }
                     var elementInfo = selectElementsInfo[i];

                     // Check if the AST element at this position was identified as a direct JSON subquery
                     if (elementInfo.IsDirectJsonSubquery)
                     {
                         if (elementInfo.InnerQuerySpecFragment == null || elementInfo.OuterQuerySpecFragment == null) {
                             currentPoco.Errors.Add($"AST fragments missing for direct JSON column (Schema: '{schemaCol.Name ?? "N/A"}', Pos: {schemaCol.Ordinal})."); continue; }

                         string descriptionQuery = ConstructDescriptionQuery(
                             elementInfo.CteFragment, elementInfo.OuterQuerySpecFragment, elementInfo.InnerQuerySpecFragment);

                         if (string.IsNullOrEmpty(descriptionQuery)) {
                             currentPoco.Errors.Add($"Failed to construct description query for direct JSON column (Schema: '{schemaCol.Name ?? "N/A"}', Pos: {schemaCol.Ordinal})."); continue; }

                         var nestedUniqueId = $"poco_{Interlocked.Increment(ref _classCounter)}";
                         // Use schema name if available, otherwise placeholder based on ordinal for nested class name base
                         string schemaNameOrPlaceholder = schemaCol.Name ?? $"JsonCol{schemaCol.Ordinal}";
                         var nestedBaseName = $"{currentPoco.DesiredBaseClassName}_{SanitizeName(schemaNameOrPlaceholder)}";

                         var jsonInfo = new JsonColumnInfo {
                             // Use the schema name OR placeholder as the key for generation lookup
                             SchemaColumnNameOrPlaceholder = schemaNameOrPlaceholder,
                             WithoutArrayWrapper = elementInfo.WithoutArrayWrapper,
                             DescriptionSql = descriptionQuery, NestedPocoUniqueId = nestedUniqueId };
                         currentPoco.JsonColumns.Add(jsonInfo); // Add to list of confirmed JSON columns

                         var nestedPoco = new PocoInProgress(nestedUniqueId, descriptionQuery, nestedBaseName) { Cts = CancellationTokenSource.CreateLinkedTokenSource(token) };
                         if (_generationResults.TryAdd(nestedUniqueId, nestedPoco)) { queue.Enqueue(nestedPoco); }
                         else { nestedPoco.Cts?.Dispose(); }
                     }
                 } // End for loop through schema columns

                 Interlocked.CompareExchange(ref currentPoco._statusAsInt, (int)GenerationStatus.Described, (int)GenerationStatus.Describing);
             }
             catch (OperationCanceledException) { LogAndFail(currentPoco, "Operation cancelled during description."); }
             catch (Exception ex) { LogAndFail(currentPoco, $"Unexpected error during description phase: {ex.Message}\n{ex.StackTrace}"); }
        }

        // Helper to check if SQL type name suggests a JSON container
        private bool IsPotentialJsonType(string? sqlTypeName)
        {
            if (string.IsNullOrEmpty(sqlTypeName)) return false;
            string typeNameOnly = sqlTypeName.Split('(')[0].ToLowerInvariant();
            return typeNameOnly is "nvarchar" or "varchar" or "ntext" or "text" or "xml";
        }


        // --- Phase 2: Code Generation ---
        private async Task<string?> GeneratePocoCodeRecursiveAsync(string uniqueId, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_generationResults.TryGetValue(uniqueId, out var currentPoco)) { throw new InvalidOperationException($"PocoInProgress not found for ID: {uniqueId}"); }
            var originalStatus = Interlocked.CompareExchange(ref currentPoco._statusAsInt, (int)GenerationStatus.Generating, (int)GenerationStatus.Described);
            switch ((GenerationStatus)originalStatus) { /* ... handle Generated, Generating, Failed, Pending, Describing ... */
                case GenerationStatus.Generated: return currentPoco.GeneratedClassName;
                case GenerationStatus.Generating: LogAndFail(currentPoco, "Circular dependency detected."); return null;
                case GenerationStatus.Failed: return null;
                case GenerationStatus.Pending: case GenerationStatus.Describing: LogAndFail(currentPoco, "Generation called before description complete."); return null;
                case GenerationStatus.Described: break; // Proceed
                default: LogAndFail(currentPoco, $"Unexpected status ({originalStatus})."); return null;
            }

            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (currentPoco.Schema == null) { LogAndFail(currentPoco, "Cannot generate code, schema is null."); return null; }

                // Resolve nested types recursively first for CONFIRMED JSON columns
                var nestedTasks = new List<Task>();
                foreach (var jsonInfo in currentPoco.JsonColumns)
                {
                     nestedTasks.Add(Task.Run(async () => {
                         try {
                             jsonInfo.NestedGeneratedClassName = await GeneratePocoCodeRecursiveAsync(jsonInfo.NestedPocoUniqueId, cancellationToken);
                             if (jsonInfo.NestedGeneratedClassName == null) { lock(currentPoco.Errors) currentPoco.Errors.Add($"Failed nested generation for '{jsonInfo.SchemaColumnNameOrPlaceholder}'."); }
                         } catch (Exception ex) {
                             lock(currentPoco.Errors) currentPoco.Errors.Add($"Error resolving nested type for '{jsonInfo.SchemaColumnNameOrPlaceholder}': {ex.Message}");
                             if (_generationResults.TryGetValue(jsonInfo.NestedPocoUniqueId, out var nestedPoco)) { LogAndFail(nestedPoco, $"Failed recursive call from '{currentPoco.DesiredBaseClassName}': {ex.Message}"); }
                         }
                     }, cancellationToken));
                 }
                await Task.WhenAll(nestedTasks);
                cancellationToken.ThrowIfCancellationRequested();

                // --- Generate C# Code ---
                var sb = new StringBuilder();
                // Determine class name
                string sanitizedBaseName = SanitizeName(currentPoco.DesiredBaseClassName);
                if (!string.IsNullOrEmpty(sanitizedBaseName) && char.IsLower(sanitizedBaseName[0])) { sanitizedBaseName = char.ToUpperInvariant(sanitizedBaseName[0]) + sanitizedBaseName.Substring(1); }
                string counterSuffix = uniqueId.Split('_').LastOrDefault() ?? "0";
                currentPoco.GeneratedClassName = $"{sanitizedBaseName}{(counterSuffix != "0" ? counterSuffix : "")}";
                if (string.IsNullOrEmpty(currentPoco.GeneratedClassName) || currentPoco.GeneratedClassName.StartsWith("_")) { currentPoco.GeneratedClassName = $"GeneratedPoco{counterSuffix}"; }

                sb.AppendLine($"public class {currentPoco.GeneratedClassName}");
                sb.AppendLine("{");

                if (!currentPoco.Schema.Any()) { sb.AppendLine("    // Query described no columns."); }

                var propertyNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var col in currentPoco.Schema) // Iterate through schema from sp_describe
                {
                    // Use schema name OR placeholder for matching and property name generation
                    string schemaNameOrPlaceholder = col.Name ?? $"JsonCol{col.Ordinal}"; // Consistent placeholder
                    string propertyName = SanitizeName(schemaNameOrPlaceholder);
                    // Ensure unique property name
                    int propCounter = 1; string originalPropertyName = propertyName; while (!propertyNames.Add(propertyName)) { propertyName = $"{originalPropertyName}_{propCounter++}"; }

                    string propertyType;
                    // Check if this column was CONFIRMED as JSON using the consistent key
                    var jsonInfo = currentPoco.JsonColumns.FirstOrDefault(j => string.Equals(j.SchemaColumnNameOrPlaceholder, schemaNameOrPlaceholder, StringComparison.OrdinalIgnoreCase));

                    if (jsonInfo != null) // Confirmed JSON column
                    {
                        if (jsonInfo.NestedGeneratedClassName != null) {
                            propertyType = jsonInfo.WithoutArrayWrapper
                                ? $"Json<{jsonInfo.NestedGeneratedClassName}>?"
                                : $"Json<List<{jsonInfo.NestedGeneratedClassName}>?>?";
                        } else {
                            propertyType = "Json<object>?"; // Fallback
                            sb.AppendLine($"    // WARNING: Could not determine nested type for JSON column '{jsonInfo.SchemaColumnNameOrPlaceholder}'. Using object.");
                        }
                    }
                    else if (col.IsPotentialJsonContainer) // Potential JSON container, but not confirmed by AST visitor
                    {
                        propertyType = "string?"; // Map as string?
                        sb.AppendLine($"    // This column ({schemaNameOrPlaceholder}) might contain JSON generated indirectly.");
                    }
                    else // Regular column
                    {
                        propertyType = MapSqlTypeToCSharp(col.SqlTypeName, col.IsNullable);
                    }
                    sb.AppendLine($"    public {propertyType} {propertyName} {{ get; set; }}");
                }
                sb.AppendLine("}");

                currentPoco.GeneratedCode = sb.ToString();
                Interlocked.CompareExchange(ref currentPoco._statusAsInt, (int)GenerationStatus.Generated, (int)GenerationStatus.Generating);
                return currentPoco.GeneratedClassName;
            }
            catch (OperationCanceledException) { LogAndFail(currentPoco, "Operation cancelled during code generation."); return null; }
            catch (Exception ex) { LogAndFail(currentPoco, $"Unexpected error during code generation: {ex.Message}"); return null; }
        }


        // --- Helper Methods (Implementations Included) ---

        private void LogAndFail(PocoInProgress poco, string errorMessage)
        {
            lock(poco.Errors) poco.Errors.Add(errorMessage);
            Interlocked.CompareExchange(ref poco._statusAsInt, (int)GenerationStatus.Failed, (int)GenerationStatus.Generating);
            Interlocked.CompareExchange(ref poco._statusAsInt, (int)GenerationStatus.Failed, (int)GenerationStatus.Describing);
            Interlocked.CompareExchange(ref poco._statusAsInt, (int)GenerationStatus.Failed, (int)GenerationStatus.Pending);
            try { poco.Cts?.Cancel(); } catch (ObjectDisposedException) { /* Ignore */ }
        }

        private async Task<List<ColumnInfo>?> DescribeSchemaAsync(string sql, CancellationToken cancellationToken)
        {
            var columns = new List<ColumnInfo>();
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    await connection.OpenAsync(cancellationToken);
                    using (var command = new SqlCommand("sys.sp_describe_first_result_set", connection))
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.CommandTimeout = 60;
                        command.Parameters.AddWithValue("@tsql", sql);
                        command.Parameters.AddWithValue("@params", null);
                        command.Parameters.AddWithValue("@browse_information_mode", 0);

                        using (var reader = await command.ExecuteReaderAsync(cancellationToken))
                        {
                            int ordinal = 0;
                            while (await reader.ReadAsync(cancellationToken))
                            {
                                columns.Add(new ColumnInfo
                                {
                                    Name = reader["name"] as string,
                                    SqlTypeName = reader["system_type_name"] as string,
                                    IsNullable = reader["is_nullable"] as bool? ?? true,
                                    Ordinal = ordinal++
                                });
                            }
                        }
                    }
                }
                return columns;
            }
            catch (SqlException) { return null; }
            catch (Exception ex) when (ex is not OperationCanceledException) { return null; }
        }

        private string ConstructDescriptionQuery(TSqlFragment? cteFragment, TSqlFragment? outerQuerySpecFragment, TSqlFragment? innerQuerySpecFragment)
        {
             if (innerQuerySpecFragment == null || outerQuerySpecFragment == null) return "";
             QuerySpecification? outerSpec = outerQuerySpecFragment as QuerySpecification ?? FindFirstChild<QuerySpecification>(outerQuerySpecFragment);
             QuerySpecification? innerSpec = innerQuerySpecFragment as QuerySpecification ?? FindFirstChild<QuerySpecification>(innerQuerySpecFragment);
             if (innerSpec?.SelectElements == null || !innerSpec.SelectElements.Any() || outerSpec?.FromClause == null) return "";

             var descriptionSql = new StringBuilder();
             if (cteFragment != null) descriptionSql.AppendLine(cteFragment.ToSqlString());
             descriptionSql.Append("SELECT ");
             var selectElementsSql = new List<string>();
             foreach(var element in innerSpec.SelectElements) selectElementsSql.Add(element.ToSqlString());
             descriptionSql.Append(string.Join(", ", selectElementsSql));
             descriptionSql.AppendLine();
             descriptionSql.Append("FROM ");
             descriptionSql.AppendLine(outerSpec.FromClause.ToSqlString());
             if (outerSpec.WhereClause != null)
             {
                 string whereContent = outerSpec.WhereClause.ToSqlString();
                 var match = Regex.Match(whereContent.TrimStart(), @"^\s*WHERE\s+(.*)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
                 whereContent = match.Success ? match.Groups[1].Value : whereContent;
                 descriptionSql.AppendLine($"WHERE ({whereContent})");
                 descriptionSql.AppendLine("  AND 1=0");
             }
             else { descriptionSql.AppendLine("WHERE 1=0"); }
             return descriptionSql.ToString();
        }

        private T? FindFirstChild<T>(TSqlFragment? parent) where T : TSqlFragment
        {
            if (parent == null) return null;
            var finder = new FindFirstVisitor<T>();
            parent.Accept(finder);
            return finder.FoundFragment;
        }

        private string MapSqlTypeToCSharp(string? sqlTypeName, bool isNullable)
        {
            if (string.IsNullOrEmpty(sqlTypeName)) return isNullable ? "object?" : "object";
            string baseType;
            string typeNameOnly = sqlTypeName.Split('(')[0].ToLowerInvariant();
            baseType = typeNameOnly switch {
                "bigint" => "long", "binary" or "varbinary" or "image" or "rowversion" or "timestamp" => "byte[]",
                "bit" => "bool", "char" or "varchar" or "nchar" or "nvarchar" or "text" or "ntext" or "xml" => "string",
                "date" or "datetime" or "datetime2" or "smalldatetime" => "DateTime", "datetimeoffset" => "DateTimeOffset",
                "decimal" or "numeric" or "money" or "smallmoney" => "decimal", "float" => "double", "int" => "int",
                "real" => "float", "smallint" => "short", "tinyint" => "byte", "uniqueidentifier" => "Guid",
                "time" => "TimeSpan", "sql_variant" => "object", "geography" or "geometry" or "hierarchyid" => "object",
                _ => "object", };
            bool isValueType = baseType is "long" or "bool" or "DateTime" or "DateTimeOffset" or "decimal" or "double" or "int" or "float" or "short" or "byte" or "Guid" or "TimeSpan";
            if (isNullable) { return isValueType ? baseType + "?" : baseType; } else { return baseType; }
        }

        private string SanitizeName(string name)
        {
             if (string.IsNullOrWhiteSpace(name)) return "_InvalidName_";
            string sanitized = Regex.Replace(name, @"[^\p{L}\p{N}_]", "_");
            sanitized = sanitized.Trim('_');
             if (string.IsNullOrEmpty(sanitized)) return "_InvalidName_";
            if (char.IsDigit(sanitized[0])) sanitized = "_" + sanitized;
            var keywords = new HashSet<string> { "class", "struct", "int", "string", "public", "private", "protected", "static", "void", "null", "true", "false", "object", "namespace", "using", "base", "this", "decimal", "double", "float", "long", "short", "byte", "bool", "DateTime", "Guid", "TimeSpan", "var", "const", "readonly", "virtual", "override", "abstract", "sealed", "event", "delegate", "enum", "interface", "params", "ref", "out", "in", "is", "as", "typeof", "sizeof", "checked", "unchecked", "default", "new", "get", "set", "value", "add", "remove", "yield", "return", "break", "continue", "if", "else", "switch", "case", "do", "while", "for", "foreach", "try", "catch", "finally", "throw", "lock", "goto" };
            string nonAtName = sanitized.StartsWith("@") ? sanitized.Substring(1) : sanitized;
            if (keywords.Contains(nonAtName)) { if (!sanitized.StartsWith("@")) { sanitized = "@" + sanitized; } }
            else if (sanitized.StartsWith("@")) { sanitized = nonAtName; }
            if (!string.IsNullOrEmpty(sanitized) && !sanitized.StartsWith("@")) {
                 sanitized = string.Join("", sanitized.Split(new[] { '_' }, StringSplitOptions.RemoveEmptyEntries).Select(s => char.ToUpperInvariant(s[0]) + s.Substring(1))); }
             else if (!string.IsNullOrEmpty(sanitized) && sanitized.StartsWith("@")) {
                 string afterAt = sanitized.Substring(1); afterAt = string.Join("", afterAt.Split(new[] { '_' }, StringSplitOptions.RemoveEmptyEntries).Select(s => char.ToUpperInvariant(s[0]) + s.Substring(1))); sanitized = "@" + afterAt; }
            if (string.IsNullOrEmpty(sanitized)) return "_InvalidName_";
            if (char.IsDigit(sanitized[0])) sanitized = "_" + sanitized;
            if (sanitized != "@" && sanitized.StartsWith("@")) { if (sanitized.Length < 2 || (!char.IsLetter(sanitized[1]) && sanitized[1] != '_')) sanitized = "@_" + sanitized.Substring(1); }
            else if (!sanitized.StartsWith("@")) { if (!char.IsLetter(sanitized[0]) && sanitized[0] != '_') sanitized = "_" + sanitized; }
            return sanitized;
        }

        // --- AST Visitors ---

        // Visitor to find the first node of a specific type
        private class FindFirstVisitor<T> : TSqlFragmentVisitor where T : TSqlFragment
        {
            public T? FoundFragment { get; private set; }
            private bool _found = false;
            public override void Visit(TSqlFragment node) { if (_found) return; if (node is T targetNode) { FoundFragment = targetNode; _found = true; return; } base.Visit(node); }
        }

        // Simple visitor to find if a JSON FOR clause exists within a fragment
        private class FindJsonForClauseVisitor : TSqlFragmentVisitor
        {
            public JsonForClause? JsonForClause { get; private set; }
            public override void ExplicitVisit(QuerySpecification node) { if (JsonForClause == null && node.ForClause is JsonForClause jfc) { JsonForClause = jfc; } if (JsonForClause == null) base.ExplicitVisit(node); }
            public override void Visit(TSqlFragment node) { if (JsonForClause != null) return; base.Visit(node); }
        }

        // Visitor to analyze top-level select elements in the current query context
        private class SelectElementVisitor : TSqlFragmentVisitor
        {
            // Inner class to hold details about each select element
            public class SelectElementDetails
            {
                public int Ordinal { get; set; }
                public string? ExplicitAlias { get; set; }
                public bool IsDirectJsonSubquery { get; set; } = false;
                public bool WithoutArrayWrapper { get; set; } = false;
                public TSqlFragment? InnerQuerySpecFragment { get; set; } // Only set if IsDirectJsonSubquery is true
                public TSqlFragment? OuterQuerySpecFragment { get; set; } // Only set if IsDirectJsonSubquery is true
                public TSqlFragment? CteFragment { get; set; } // Only set if IsDirectJsonSubquery is true
            }

            public List<SelectElementDetails> SelectElements { get; } = new List<SelectElementDetails>();
            private TSqlFragment? _currentCteFragment = null;
            private Stack<TSqlFragment> _queryContextStack = new Stack<TSqlFragment>();
            private int _currentSelectElementIndex = 0;

            public override void ExplicitVisit(WithCtesAndXmlNamespaces node) { _currentCteFragment = node; base.ExplicitVisit(node); }

            // Only process the top-level QuerySpecification or BinaryQueryExpression passed to Accept
            public override void Visit(QuerySpecification node)
            {
                // Prevent recursion into subqueries' select lists by this visitor
                if (_queryContextStack.Count == 0)
                {
                    _queryContextStack.Push(node);
                    _currentSelectElementIndex = 0;
                    if (node.SelectElements != null) { foreach (var element in node.SelectElements) { element.Accept(this); } }
                    _queryContextStack.Pop();
                }
            }
             public override void Visit(BinaryQueryExpression node)
             {
                 // Prevent recursion into subqueries' select lists by this visitor
                 if (_queryContextStack.Count == 0)
                 {
                     _queryContextStack.Push(node);
                     // How to handle select elements across UNIONs? sp_describe gives final shape.
                     // For now, just visit the first query expression to get *some* context.
                     // A more robust solution might need to analyze both sides if structure differs significantly.
                     node.FirstQueryExpression?.Accept(this);
                     // Reset index if needed? Or assume sp_describe handles the combined structure?
                     // Let's assume sp_describe handles it and we only analyze the first part for context.
                     _queryContextStack.Pop();
                 }
             }


            // Process Scalar Expressions
            public override void ExplicitVisit(SelectScalarExpression node)
            {
                if (_queryContextStack.Count != 1) return; // Only process top-level elements

                var details = new SelectElementDetails { Ordinal = _currentSelectElementIndex, ExplicitAlias = node.ColumnName?.Value };

                if (node.Expression is ScalarSubquery subquery && subquery.QueryExpression != null)
                {
                    var jsonFinder = new FindJsonForClauseVisitor();
                    subquery.QueryExpression.Accept(jsonFinder);
                    JsonForClause? jsonForClause = jsonFinder.JsonForClause;

                    if (jsonForClause != null) // It's a direct JSON subquery column
                    {
                        details.IsDirectJsonSubquery = true;
                        details.InnerQuerySpecFragment = subquery.QueryExpression;
                        details.OuterQuerySpecFragment = _queryContextStack.Peek(); // The current QuerySpecification/BinaryQueryExpression
                        details.CteFragment = _currentCteFragment;
                        if (jsonForClause.Options != null) { foreach (var option in jsonForClause.Options) { if (option is JsonForClauseOption jsonOpt && jsonOpt.OptionKind == JsonForClauseOptions.WithoutArrayWrapper) { details.WithoutArrayWrapper = true; break; } } }
                    }
                }
                SelectElements.Add(details);
                _currentSelectElementIndex++;
            }

             // Process Star Expressions
             public override void ExplicitVisit(SelectStarExpression node)
             {
                 if (_queryContextStack.Count != 1) return; // Only process top-level elements
                 // Represent SELECT * as a single element for positional correlation.
                 // sp_describe_first_result_set will expand it.
                 SelectElements.Add(new SelectElementDetails { Ordinal = _currentSelectElementIndex, ExplicitAlias = null }); // No alias, not direct JSON
                 _currentSelectElementIndex++;
             }

             // Process Set Variables
             public override void ExplicitVisit(SelectSetVariable node)
             {
                 if (_queryContextStack.Count != 1) return; // Only process top-level elements
                 SelectElements.Add(new SelectElementDetails { Ordinal = _currentSelectElementIndex, ExplicitAlias = node.Variable?.Name }); // Use variable name as alias? Or null? Let's use null.
                 _currentSelectElementIndex++;
             }
             // Add other SelectElement derived types if necessary

        } // End SelectElementVisitor

    } // End SqlModelGenerator

    // --- TSqlFragment Extension Method ---
    public static class TSqlFragmentExtensions
    {
        public static string ToSqlString(this TSqlFragment fragment)
        {
            var options = new SqlScriptGeneratorOptions
            {
                SqlEngineType = SqlEngineType.All, // Be generic
            };
            var generator = new Sql160ScriptGenerator(options);
            string script;
            generator.GenerateScript(fragment, out script);
            return script;
        }
    }

    // --- Custom Exception Class ---
    public class SqlPocoGenerationException : Exception
    {
        public SqlPocoGenerationException() { }
        public SqlPocoGenerationException(string message) : base(message) { }
        public SqlPocoGenerationException(string message, Exception inner) : base(message, inner) { }
    }

} // End namespace