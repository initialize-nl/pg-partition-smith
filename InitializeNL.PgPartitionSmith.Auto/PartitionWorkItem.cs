using InitializeNL.PgPartitionSmith.Auto.Configuration;

namespace InitializeNL.PgPartitionSmith.Auto;

internal record PartitionWorkItem(
  string TableName,
  TablePartitionConfig Config,
  string PartitionName,
  string Ddl,
  int Level);
