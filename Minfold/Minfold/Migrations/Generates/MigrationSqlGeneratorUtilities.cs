using System.Security.Cryptography;
using System.Text;

namespace Minfold;

/// <summary>
/// Shared utilities for SQL generation across all generator classes.
/// </summary>
public static class MigrationSqlGeneratorUtilities
{
    /// <summary>
    /// Generates a deterministic suffix from input strings using SHA256 hashing.
    /// Same inputs will always produce the same suffix, ensuring idempotent migration generation.
    /// </summary>
    /// <param name="inputs">Input strings to hash (e.g., table name, column name, default value, operation context)</param>
    /// <returns>8-character hexadecimal suffix derived from the hash</returns>
    public static string GenerateDeterministicSuffix(params string[] inputs)
    {
        if (inputs == null || inputs.Length == 0)
        {
            throw new ArgumentException("At least one input is required", nameof(inputs));
        }

        // Normalize inputs: convert to lowercase and join with a delimiter
        string normalizedInput = string.Join("|", inputs.Select(s => (s ?? string.Empty).ToLowerInvariant()));
        
        // Compute SHA256 hash
        byte[] hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(normalizedInput));
        
        // Take first 8 hex characters (32 bits) for readability
        return Convert.ToHexString(hashBytes).Substring(0, 8).ToLowerInvariant();
    }
}

