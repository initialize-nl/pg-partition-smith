using System.Text.Json.Serialization;

namespace InitializeNL.PgPartitionSmith.Core.Models;

public class PartitionItem
{
  [JsonPropertyName("field")]
  public string Field { get; set; } = null!;

  [JsonPropertyName("values")]
  public List<string>? Values { get; set; }

  [JsonPropertyName("name")]
  public string Name { get; set; } = null!;

  [JsonPropertyName("from")]
  public string? From { get; set; }

  [JsonPropertyName("to")]
  public string? To { get; set; }

  [JsonPropertyName("partitions")]
  public PartitionDefinition? Partitions { get; set; }

  public bool IsRemainder { get; set; }

  public void Validate(string path, string parentType)
  {
    if (string.IsNullOrWhiteSpace(Field))
    {
      throw new InvalidOperationException(
        $"Partition item at {path} is missing 'field'.");
    }

    if (string.IsNullOrWhiteSpace(Name))
    {
      throw new InvalidOperationException(
        $"Partition item at {path} is missing 'name'.");
    }

    if (!IsRemainder)
    {
      if (parentType == "list" && (Values == null || Values.Count == 0))
      {
        throw new InvalidOperationException(
          $"List partition item at {path} is missing 'values'.");
      }

      if (parentType == "range" && (string.IsNullOrWhiteSpace(From) || string.IsNullOrWhiteSpace(To)))
      {
        throw new InvalidOperationException(
          $"Range partition item at {path} is missing 'from' or 'to'.");
      }
    }

    Partitions?.Validate(path);
  }
}
