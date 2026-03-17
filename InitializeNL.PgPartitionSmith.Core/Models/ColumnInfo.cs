namespace InitializeNL.PgPartitionSmith.Core.Models;

public class ColumnInfo
{
  public string ColumnName { get; set; } = null!;
  public string FullType { get; set; } = null!;
  public bool IsNotNull { get; set; }
  public string? ColumnDefault { get; set; }
  public int OrdinalPosition { get; set; }
}
