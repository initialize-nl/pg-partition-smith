using System.Text.Json.Serialization;

namespace InitializeNL.PgPartitionSmith.Core.Models;

public class PartitionSchema
{
  [JsonPropertyName("partitions")]
  public PartitionDefinition Partitions { get; set; } = null!;

  public void Validate()
  {
    Partitions.Validate("root");
  }
}
