using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using InitializeNL.PgPartitionSmith.Core.Models;

namespace InitializeNL.PgPartitionSmith.Core.Naming;

public class PartitionNamingService
{
  private readonly string _baseName;
  private readonly string _partitionedName;
  private readonly bool _hashedNames;
  private readonly string _schema;

  public PartitionNamingService(
    string baseName,
    string partitionedName,
    bool hashedNames,
    string schema)
  {
    _baseName = baseName;
    _partitionedName = partitionedName;
    _hashedNames = hashedNames;
    _schema = schema;
  }

  public string GetPartitionTableName(string parentTableName, string itemName, string path)
  {
    if (!_hashedNames)
    {
      return $"{parentTableName}_{itemName}";
    }

    string hashInput = $"{_baseName}/{path}";
    string shortHash = ComputeShortHash(hashInput);
    return $"{_baseName}_{_partitionedName}_{shortHash}";
  }

  public string? GetCommentSql(
    string tableName,
    PartitionItem item,
    string partitionType,
    string path,
    bool isRemainder)
  {
    if (!_hashedNames)
    {
      return null;
    }

    Dictionary<string, object> commentData = new Dictionary<string, object>
    {
      ["path"] = path,
      ["type"] = partitionType,
      ["field"] = item.Field,
      ["is_remainder"] = isRemainder
    };

    if (!isRemainder)
    {
      if (item.Values != null)
      {
        commentData["values"] = item.Values;
      }

      if (item.From != null)
      {
        commentData["from"] = item.From;
      }

      if (item.To != null)
      {
        commentData["to"] = item.To;
      }
    }

    string json = JsonSerializer.Serialize(commentData);
    string qualifiedName = $"{_schema}.{tableName}";
    string escapedComment = $"PartitionSmith:{json}".Replace("'", "''");
    return $"COMMENT ON TABLE {qualifiedName} IS '{escapedComment}'";
  }

  private static string ComputeShortHash(string input)
  {
    byte[] hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(input));
    return Convert.ToHexStringLower(hashBytes)[..8];
  }
}
