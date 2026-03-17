namespace InitializeNL.PgPartitionSmith.Auto.Configuration;

public class ListPartitionItemConfig
{
  public string Name { get; set; } = null!;
  public List<string> Values { get; set; } = [];
  public TablePartitionConfig? ChildPartition { get; set; }
}
