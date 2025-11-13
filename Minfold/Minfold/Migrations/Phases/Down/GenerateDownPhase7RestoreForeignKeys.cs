using System.Collections.Concurrent;
using System.Text;

namespace Minfold.Migrations.Phases.Down;

public static class GenerateDownPhase7RestoreForeignKeys
{
    public static string Generate(
        SchemaDiff diff,
        ConcurrentDictionary<string, SqlTable> currentSchema,
        ConcurrentDictionary<string, SqlTable> targetSchema)
    {
        StringBuilder content = new StringBuilder();
        
        // Collect FK changes
        List<ForeignKeyChange> allFkChanges = DownForeignKeyHelper.CollectForeignKeyChanges(diff, currentSchema);
        
        // Collect all FKs to be restored (from Modify and Drop changes)
        // Use NOCHECK → CHECK pattern to handle circular dependencies and preserve NotEnforced state
        // IMPORTANT: Get the original FK state from targetSchema (before migration), not from OldForeignKey (after migration)
        // IMPORTANT: Only restore FKs that existed before the migration (Modify/Drop), NOT FKs that were added (Add)
        List<(List<SqlForeignKey> FkGroup, bool WasNoCheck)> fksToRestore = new List<(List<SqlForeignKey>, bool)>();
        HashSet<string> processedFksDown = new HashSet<string>();
        
        // Track FKs that were added (should NOT be restored, only dropped)
        HashSet<string> addedFkNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Add))
        {
            if (fkChange.NewForeignKey != null)
            {
                addedFkNames.Add(fkChange.NewForeignKey.Name);
            }
        }
        
        // Collect FKs from Modify changes (restore old FK)
        foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Modify))
        {
            if (fkChange.OldForeignKey != null && 
                !processedFksDown.Contains(fkChange.OldForeignKey.Name) &&
                !addedFkNames.Contains(fkChange.OldForeignKey.Name))
            {
                SqlForeignKey? fkToUse = GetOriginalForeignKey(fkChange.OldForeignKey, targetSchema);
                if (fkToUse == null) continue;
                
                List<SqlForeignKey> fkGroup = BuildForeignKeyGroup(fkToUse, fkChange.OldForeignKey, allFkChanges, targetSchema);
                bool wasNoCheck = fkGroup[0].NotEnforced;
                fksToRestore.Add((fkGroup, wasNoCheck));
                processedFksDown.Add(fkChange.OldForeignKey.Name);
            }
        }
        
        // Collect FKs from Drop changes (restore dropped FK)
        foreach (ForeignKeyChange fkChange in allFkChanges.Where(c => c.ChangeType == ForeignKeyChangeType.Drop))
        {
            if (fkChange.OldForeignKey != null && 
                !processedFksDown.Contains(fkChange.OldForeignKey.Name) &&
                !addedFkNames.Contains(fkChange.OldForeignKey.Name))
            {
                SqlForeignKey? fkToUse = GetOriginalForeignKey(fkChange.OldForeignKey, targetSchema);
                if (fkToUse == null) continue;
                
                List<SqlForeignKey> fkGroup = BuildForeignKeyGroup(fkToUse, fkChange.OldForeignKey, allFkChanges, targetSchema);
                bool wasNoCheck = fkGroup[0].NotEnforced;
                fksToRestore.Add((fkGroup, wasNoCheck));
                processedFksDown.Add(fkChange.OldForeignKey.Name);
            }
        }
        
        // Create all FKs with NOCHECK first (avoids circular dependency issues and reduces lock time)
        Dictionary<string, SqlTable> tablesDictDown = new Dictionary<string, SqlTable>(targetSchema);
        foreach (var (fkGroup, wasNoCheck) in fksToRestore.OrderBy(g => g.FkGroup[0].Table).ThenBy(g => g.FkGroup[0].Name))
        {
            // Force NOCHECK during creation to avoid circular dependency issues
            string fkSql = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDictDown, forceNoCheck: true);
            content.Append(fkSql);
            content.AppendLine();
        }
        
        // Restore CHECK state for FKs that weren't originally NOCHECK
        // IMPORTANT: CHECK CONSTRAINT doesn't always restore is_not_trusted correctly after WITH NOCHECK
        // So we need to drop and recreate the FK with WITH CHECK to ensure correct NotEnforced state
        foreach (var (fkGroup, wasNoCheck) in fksToRestore.OrderBy(g => g.FkGroup[0].Table).ThenBy(g => g.FkGroup[0].Name))
        {
            if (!wasNoCheck)
            {
                SqlForeignKey firstFk = fkGroup[0];
                // Drop the FK that was created with NOCHECK
                content.AppendLine($"ALTER TABLE [{firstFk.Schema}].[{firstFk.Table}] DROP CONSTRAINT [{firstFk.Name}];");
                
                // Recreate it with WITH CHECK to ensure correct NotEnforced state
                string fkSqlWithCheck = GenerateForeignKeys.GenerateForeignKeyStatement(fkGroup, tablesDictDown, forceNoCheck: false);
                if (!string.IsNullOrEmpty(fkSqlWithCheck))
                {
                    content.Append(fkSqlWithCheck);
                    content.AppendLine();
                }
            }
        }
        
        return content.ToString().Trim();
    }
    
    private static SqlForeignKey? GetOriginalForeignKey(SqlForeignKey fk, ConcurrentDictionary<string, SqlTable> targetSchema)
    {
        if (targetSchema.TryGetValue(fk.Table.ToLowerInvariant(), out SqlTable? targetTable))
        {
            foreach (SqlTableColumn column in targetTable.Columns.Values)
            {
                foreach (SqlForeignKey targetFk in column.ForeignKeys)
                {
                    if (targetFk.Name.Equals(fk.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        return targetFk;
                    }
                }
            }
        }
        return null;
    }
    
    private static List<SqlForeignKey> BuildForeignKeyGroup(
        SqlForeignKey firstFk,
        SqlForeignKey originalFk,
        List<ForeignKeyChange> allFkChanges,
        ConcurrentDictionary<string, SqlTable> targetSchema)
    {
        List<SqlForeignKey> fkGroup = new List<SqlForeignKey> { firstFk };
        HashSet<string> addedColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { firstFk.Column };
        
        foreach (ForeignKeyChange otherFkChange in allFkChanges)
        {
            if (otherFkChange.OldForeignKey != null &&
                otherFkChange.OldForeignKey.Name == originalFk.Name &&
                otherFkChange.OldForeignKey.Table == originalFk.Table &&
                !addedColumns.Contains(otherFkChange.OldForeignKey.Column))
            {
                SqlForeignKey? otherOriginalFk = GetOriginalForeignKey(otherFkChange.OldForeignKey, targetSchema);
                if (otherOriginalFk != null)
                {
                    fkGroup.Add(otherOriginalFk);
                    addedColumns.Add(otherOriginalFk.Column);
                }
            }
        }
        
        return fkGroup;
    }
}

