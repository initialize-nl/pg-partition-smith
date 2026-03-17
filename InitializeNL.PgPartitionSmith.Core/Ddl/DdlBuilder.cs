namespace InitializeNL.PgPartitionSmith.Core.Ddl;

public static class DdlBuilder
{
  public static string Qualified(string schema, string tableName) => $"{schema}.{tableName}";

  public static string CreateRangePartition(
    string schema, string partitionName, string parentTableName,
    string from, string to, string? subPartitionBy = null)
  {
    string subClause = subPartitionBy != null ? $" PARTITION BY {subPartitionBy}" : "";
    return $"CREATE TABLE {Qualified(schema, partitionName)} PARTITION OF {Qualified(schema, parentTableName)} FOR VALUES FROM ('{from}') TO ('{to}'){subClause}";
  }

  public static string CreateListPartition(
    string schema, string partitionName, string parentTableName,
    IEnumerable<string> values, string? subPartitionBy = null)
  {
    string valuesList = string.Join(", ", values.Select(v => $"'{v}'"));
    string subClause = subPartitionBy != null ? $" PARTITION BY {subPartitionBy}" : "";
    return $"CREATE TABLE {Qualified(schema, partitionName)} PARTITION OF {Qualified(schema, parentTableName)} FOR VALUES IN ({valuesList}){subClause}";
  }

  public static string CreateDefaultPartition(
    string schema, string partitionName, string parentTableName,
    string? subPartitionBy = null)
  {
    string subClause = subPartitionBy != null ? $" PARTITION BY {subPartitionBy}" : "";
    return $"CREATE TABLE {Qualified(schema, partitionName)} PARTITION OF {Qualified(schema, parentTableName)} DEFAULT{subClause}";
  }

  public static string CreatePartitionedTable(
    string schema, string tableName, string columns, string partitionType, string partitionColumn)
  {
    return $"CREATE TABLE IF NOT EXISTS {Qualified(schema, tableName)} ({columns}) PARTITION BY {partitionType.ToUpperInvariant()} ({partitionColumn})";
  }
}
