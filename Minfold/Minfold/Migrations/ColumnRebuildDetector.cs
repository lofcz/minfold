using System.Data;

namespace Minfold;

/// <summary>
/// Detects when column changes require a table rebuild (DROP+ADD) versus a simple ALTER COLUMN statement.
/// Based on OpenDBDiff's sophisticated column change detection logic.
/// </summary>
public static class ColumnRebuildDetector
{
    /// <summary>
    /// Determines if a column change requires a rebuild (DROP+ADD) rather than ALTER COLUMN.
    /// </summary>
    public static bool RequiresRebuild(SqlTableColumn oldColumn, SqlTableColumn newColumn, SqlTable table)
    {
        // Check type changes that require rebuild
        if (RequiresRebuildForTypeChange(oldColumn, newColumn))
        {
            return true;
        }

        // Check identity changes
        if (RequiresRebuildForIdentityChange(oldColumn, newColumn))
        {
            return true;
        }

        // Check computed column changes
        if (RequiresRebuildForComputedChange(oldColumn, newColumn))
        {
            return true;
        }

        // Check position changes with dependencies
        if (RequiresRebuildForPositionChange(oldColumn, newColumn, table))
        {
            return true;
        }

        // Timestamp columns always require rebuild when changed
        if (IsTimestampType(oldColumn.SqlType) || IsTimestampType(newColumn.SqlType))
        {
            return true;
        }

        return false;
    }

    /// <summary>
    /// Checks if a type change requires rebuild (text/ntext/image conversions).
    /// </summary>
    private static bool RequiresRebuildForTypeChange(SqlTableColumn oldColumn, SqlTableColumn newColumn)
    {
        // Check base type (without length/precision)
        SqlDbTypeExt oldBaseType = oldColumn.SqlType;
        SqlDbTypeExt newBaseType = newColumn.SqlType;

        // If base types are the same, check if it's a length/precision change that requires rebuild
        if (oldBaseType == newBaseType)
        {
            // Some type changes with length require rebuild (e.g., varchar to text)
            return RequiresRebuildForLengthChange(oldColumn, newColumn);
        }

        // Converting to text/ntext/image (legacy LOB types) always requires rebuild
        // These are deprecated types that require special handling, even when converting from varchar/nvarchar/varbinary
        if (newBaseType == SqlDbTypeExt.Text && oldBaseType != SqlDbTypeExt.Text)
        {
            return true;
        }

        if (newBaseType == SqlDbTypeExt.NText && oldBaseType != SqlDbTypeExt.NText)
        {
            return true;
        }

        if (newBaseType == SqlDbTypeExt.Image && oldBaseType != SqlDbTypeExt.Image)
        {
            return true;
        }

        // Converting FROM text/ntext/image (legacy LOB types) also always requires rebuild
        // These types cannot be altered directly and must be dropped and recreated
        if (oldBaseType == SqlDbTypeExt.Text && newBaseType != SqlDbTypeExt.Text)
        {
            return true;
        }

        if (oldBaseType == SqlDbTypeExt.NText && newBaseType != SqlDbTypeExt.NText)
        {
            return true;
        }

        if (oldBaseType == SqlDbTypeExt.Image && newBaseType != SqlDbTypeExt.Image)
        {
            return true;
        }

        return false;
    }

    /// <summary>
    /// Checks if a length/precision change requires rebuild (e.g., varchar to text).
    /// </summary>
    private static bool RequiresRebuildForLengthChange(SqlTableColumn oldColumn, SqlTableColumn newColumn)
    {
        // If types are the same, check if changing from varchar/nvarchar to text/ntext via MAX
        if (oldColumn.SqlType == SqlDbTypeExt.VarChar && newColumn.SqlType == SqlDbTypeExt.VarChar)
        {
            // Changing varchar to varchar(MAX) - this is just a length change, no rebuild needed
            // But if changing from varchar(MAX) to text, that would be a type change
            return false;
        }

        if (oldColumn.SqlType == SqlDbTypeExt.NVarChar && newColumn.SqlType == SqlDbTypeExt.NVarChar)
        {
            // Changing nvarchar to nvarchar(MAX) - this is just a length change, no rebuild needed
            return false;
        }

        // For now, length/precision changes don't require rebuild unless they involve text/ntext/image
        return false;
    }

    /// <summary>
    /// Checks if an identity property change requires rebuild.
    /// </summary>
    private static bool RequiresRebuildForIdentityChange(SqlTableColumn oldColumn, SqlTableColumn newColumn)
    {
        // Identity property changes always require rebuild
        if (oldColumn.IsIdentity != newColumn.IsIdentity)
        {
            return true;
        }

        // Identity seed/increment changes also require rebuild
        if (oldColumn.IsIdentity && newColumn.IsIdentity)
        {
            if (oldColumn.IdentitySeed != newColumn.IdentitySeed ||
                oldColumn.IdentityIncrement != newColumn.IdentityIncrement)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Checks if a computed column change requires rebuild.
    /// </summary>
    private static bool RequiresRebuildForComputedChange(SqlTableColumn oldColumn, SqlTableColumn newColumn)
    {
        // Computed property changes require rebuild
        if (oldColumn.IsComputed != newColumn.IsComputed)
        {
            return true;
        }

        // Computed formula changes require rebuild
        if (oldColumn.IsComputed && newColumn.IsComputed)
        {
            string oldFormula = NormalizeComputedSql(oldColumn.ComputedSql);
            string newFormula = NormalizeComputedSql(newColumn.ComputedSql);
            if (oldFormula != newFormula)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Checks if a position change requires rebuild (when column has dependencies).
    /// </summary>
    private static bool RequiresRebuildForPositionChange(SqlTableColumn oldColumn, SqlTableColumn newColumn, SqlTable table)
    {
        // If position hasn't changed, no rebuild needed for position
        if (oldColumn.OrdinalPosition == newColumn.OrdinalPosition)
        {
            return false;
        }

        // Check if column has index dependencies
        bool hasIndexDependencies = HasIndexDependencies(oldColumn, table);

        // Check if column has computed dependencies (other computed columns depend on this one)
        bool hasComputedDependencies = HasComputedDependencies(oldColumn, table);

        // Position changes require rebuild if:
        // 1. Column is computed itself
        // 2. Column has computed dependencies
        // 3. Column has index dependencies
        if (oldColumn.IsComputed || hasComputedDependencies || hasIndexDependencies)
        {
            return true;
        }

        return false;
    }

    /// <summary>
    /// Checks if a column has index dependencies.
    /// </summary>
    private static bool HasIndexDependencies(SqlTableColumn column, SqlTable table)
    {
        if (table.Indexes == null)
        {
            return false;
        }

        foreach (SqlIndex index in table.Indexes)
        {
            if (index.Columns != null && index.Columns.Any(c => 
                c.Equals(column.Name, StringComparison.OrdinalIgnoreCase)))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Checks if a column has computed dependencies (other computed columns reference it).
    /// </summary>
    private static bool HasComputedDependencies(SqlTableColumn column, SqlTable table)
    {
        if (table.Columns == null)
        {
            return false;
        }

        foreach (SqlTableColumn otherColumn in table.Columns.Values)
        {
            if (otherColumn.IsComputed && !string.IsNullOrWhiteSpace(otherColumn.ComputedSql))
            {
                // Simple check: if computed SQL contains the column name
                // This is a basic check - a more sophisticated parser would be needed for 100% accuracy
                string computedSql = otherColumn.ComputedSql.ToUpperInvariant();
                string columnName = $"[{column.Name.ToUpperInvariant()}]";
                string columnNameNoBrackets = column.Name.ToUpperInvariant();
                
                if (computedSql.Contains(columnName, StringComparison.OrdinalIgnoreCase) ||
                    computedSql.Contains(columnNameNoBrackets, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>
    /// Checks if a SQL type is a text type (text, ntext, varchar, nvarchar, etc.).
    /// </summary>
    private static bool IsTextType(SqlDbTypeExt sqlType)
    {
        return sqlType == SqlDbTypeExt.VarChar || 
               sqlType == SqlDbTypeExt.NVarChar || 
               sqlType == SqlDbTypeExt.NText || 
               sqlType == SqlDbTypeExt.Text || 
               sqlType == SqlDbTypeExt.Xml || 
               sqlType == SqlDbTypeExt.Char || 
               sqlType == SqlDbTypeExt.NChar;
    }

    /// <summary>
    /// Checks if a SQL type is an image type.
    /// </summary>
    private static bool IsImageType(SqlDbTypeExt sqlType)
    {
        return sqlType == SqlDbTypeExt.Image;
    }

    /// <summary>
    /// Checks if a SQL type is a binary type (binary, varbinary, image).
    /// </summary>
    private static bool IsBinaryType(SqlDbTypeExt sqlType)
    {
        return sqlType == SqlDbTypeExt.VarBinary || 
               sqlType == SqlDbTypeExt.Image || 
               sqlType == SqlDbTypeExt.Binary;
    }

    /// <summary>
    /// Checks if a SQL type is timestamp.
    /// </summary>
    private static bool IsTimestampType(SqlDbTypeExt sqlType)
    {
        return sqlType == SqlDbTypeExt.Timestamp;
    }

    /// <summary>
    /// Normalizes computed SQL for comparison (removes whitespace).
    /// </summary>
    private static string NormalizeComputedSql(string? computedSql)
    {
        if (string.IsNullOrWhiteSpace(computedSql))
        {
            return string.Empty;
        }

        // Normalize whitespace: replace multiple spaces/newlines with single space, trim
        return System.Text.RegularExpressions.Regex.Replace(computedSql, @"\s+", " ").Trim();
    }
}

