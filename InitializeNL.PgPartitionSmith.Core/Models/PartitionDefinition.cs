using System.Text.Json.Serialization;

namespace InitializeNL.PgPartitionSmith.Core.Models;

public class PartitionDefinition
{
  [JsonPropertyName("type")]
  public string Type { get; set; } = null!;

  [JsonPropertyName("hasRemainder")]
  public bool HasRemainder { get; set; }

  [JsonPropertyName("items")]
  public List<PartitionItem> Items { get; set; } = [];

  public void Validate(string path)
  {
    if (Type != "list" && Type != "range")
    {
      throw new InvalidOperationException(
        $"Invalid partition type '{Type}' at {path}. Must be 'list' or 'range'.");
    }

    if (Items.Count == 0)
    {
      throw new InvalidOperationException(
        $"Partition definition at {path} has no items.");
    }

    for (int i = 0; i < Items.Count; i++)
    {
      Items[i].Validate($"{path}.items[{i}]", Type);
    }
  }
}
