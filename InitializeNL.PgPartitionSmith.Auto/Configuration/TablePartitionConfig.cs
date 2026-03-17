using System.Reflection;

namespace InitializeNL.PgPartitionSmith.Auto.Configuration;

public class TablePartitionConfig
{
  public string TableName { get; set; } = null!;
  public string Schema { get; set; } = "public";
  public bool SchemaExplicitlySet { get; set; }
  public string PartitionType { get; set; } = null!;
  public string PartitionColumn { get; set; } = null!;
  public PropertyInfo? MappedProperty { get; set; }
  public RangeInterval? Interval { get; set; }
  public List<string>? KnownValues { get; set; }
  public List<ListPartitionItemConfig> ListPartitions { get; set; } = [];
  public bool AutoCreateForNewValues { get; set; }
  public bool HasRemainderPartition { get; set; }
  public string? CreateIfNotExistsColumns { get; set; }
  public TablePartitionConfig? SubPartition { get; set; }
  public TimeSpan? PreCreateAhead { get; set; }
}
