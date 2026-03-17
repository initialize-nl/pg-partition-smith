using InitializeNL.PgPartitionSmith.Core.Locking;

namespace InitializeNL.PgPartitionSmith.Auto.Configuration;

public class PartitionManagerBuilder
{
  private string? _connectionString;
  private IPartitionLock? _lock;
  private bool _hashedNames;
  private readonly Dictionary<string, TablePartitionConfig> _tableConfigs = new();

  public PartitionManagerBuilder UseConnectionString(string connectionString)
  {
    _connectionString = connectionString;
    return this;
  }

  public PartitionManagerBuilder UseAdvisoryLocks()
  {
    _lock = new AdvisoryPartitionLock();
    return this;
  }

  public PartitionManagerBuilder UseTableLock(string tableName = "__partition_lock", string schema = "public")
  {
    _lock = new TablePartitionLock(tableName, schema);
    return this;
  }

  public PartitionManagerBuilder UseLock(IPartitionLock partitionLock)
  {
    _lock = partitionLock;
    return this;
  }

  public PartitionManagerBuilder UseHashedNames()
  {
    _hashedNames = true;
    return this;
  }

  public PartitionManagerBuilder ForTable(string tableName, Action<TablePartitionConfigBuilder> configure)
  {
    TablePartitionConfigBuilder tableBuilder = new TablePartitionConfigBuilder();
    configure(tableBuilder);
    TablePartitionConfig config = tableBuilder.Build();
    config.TableName = tableName;
    _tableConfigs[tableName] = config;
    return this;
  }

  // Exposed for testing
  public Dictionary<string, TablePartitionConfig> GetTableConfigs() => _tableConfigs;

  public IPartitionManager Build()
  {
    if (string.IsNullOrWhiteSpace(_connectionString))
      throw new InvalidOperationException("Connection string is required. Call UseConnectionString().");
    if (_tableConfigs.Count == 0)
      throw new InvalidOperationException("At least one table must be configured. Call ForTable().");

    _lock ??= new AdvisoryPartitionLock();

    return new PartitionManager(_connectionString, _lock, _hashedNames, _tableConfigs);
  }
}
