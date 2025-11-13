using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace Minfold;

public static class MigrationSqlGenerator
{
    public static string GenerateSectionHeader(int phaseNumber, string phaseDescription)
    {
        StringBuilder sb = new StringBuilder();
        sb.AppendLine("-- =============================================");
        sb.AppendLine($"-- Phase {phaseNumber}: {phaseDescription}");
        sb.AppendLine("-- =============================================");
        sb.AppendLine();
        return sb.ToString();
    }
}
