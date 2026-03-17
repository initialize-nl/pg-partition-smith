using System.Collections.Concurrent;
using System.Globalization;
using Npgsql;
using InitializeNL.PgPartitionSmith.Auto.Configuration;
using InitializeNL.PgPartitionSmith.Core.Catalog;
using InitializeNL.PgPartitionSmith.Core.Ddl;
using InitializeNL.PgPartitionSmith.Core.Locking;
using InitializeNL.PgPartitionSmith.Core.Naming;

namespace InitializeNL.PgPartitionSmith.Auto;

internal class PartitionManager : IPartitionManager
{
  private readonly string _connectionString;
  private readonly IPartitionLock _lock;
  private readonly bool _hashedNames;
  private readonly Dictionary<string, TablePartitionConfig> _tableConfigs;
  private readonly ConcurrentDictionary<string, PartitionCache> _caches = new();
  private readonly SemaphoreSlim _initLock = new SemaphoreSlim(1, 1);
  private bool _initialized;

  internal PartitionManager(
    string connectionString,
    IPartitionLock partitionLock,
    bool hashedNames,
    Dictionary<string, TablePartitionConfig> tableConfigs)
  {
    _connectionString = connectionString;
    _lock = partitionLock;
    _hashedNames = hashedNames;
    _tableConfigs = tableConfigs;
  }

  public async Task InitializeAsync(CancellationToken ct = default)
  {
    await _initLock.WaitAsync(ct);
    try
    {
      if (_initialized) return;

      await using NpgsqlConnection connection = new NpgsqlConnection(_connectionString);
      await connection.OpenAsync(ct);

      if (_lock is TablePartitionLock tableLock)
      {
        await tableLock.EnsureLockTableExistsAsync(connection);
      }

      foreach ((string tableName, TablePartitionConfig config) in _tableConfigs)
      {
        await SchemaValidator.ValidateAndInitializeAsync(connection, config, ct);

        await InitializeStaticPartitionsAsync(connection, tableName, config, ct);

        PartitionCache cache = GetOrCreateCache(tableName);
        IEnumerable<string> existingPartitions = await CatalogQueries.GetExistingPartitionNamesAsync(
          connection, config.TableName, config.Schema);
        cache.WarmUp(existingPartitions);
      }

      _initialized = true;
    }
    finally
    {
      _initLock.Release();
    }
  }

  public async Task EnsurePartitionAsync<T>(T entity, CancellationToken ct = default)
  {
    ArgumentNullException.ThrowIfNull(entity);

    if (!_initialized)
    {
      await InitializeAsync(ct);
    }

    (string tableName, TablePartitionConfig config) = FindConfigForType<T>();
    await EnsurePartitionTreeAsync(
      tableName,
      config,
      nodeConfig => nodeConfig.MappedProperty?.GetValue(entity),
      ct);
  }

  public async Task EnsurePartitionAsync(string tableName,
    IDictionary<string, object> values,
    CancellationToken ct = default)
  {
    if (!_initialized)
    {
      await InitializeAsync(ct);
    }

    if (!_tableConfigs.TryGetValue(tableName, out TablePartitionConfig? config))
      throw new InvalidOperationException($"Table '{tableName}' is not configured.");

    await EnsurePartitionTreeAsync(
      tableName,
      config,
      nodeConfig => values.TryGetValue(nodeConfig.PartitionColumn, out object? mapValue) ? mapValue : null,
      ct);
  }

  public async Task EnsurePartitionsAsync(
    string tableName,
    IEnumerable<IDictionary<string, object>> values,
    CancellationToken ct = default)
  {
    ArgumentNullException.ThrowIfNull(values);

    if (!_initialized)
    {
      await InitializeAsync(ct);
    }

    if (!_tableConfigs.TryGetValue(tableName, out TablePartitionConfig? config))
    {
      throw new InvalidOperationException($"Table '{tableName}' is not configured.");
    }

    List<PartitionWorkItem> workItems = [];
    foreach (IDictionary<string, object> entityValues in values)
    {
      ResolvePartitionTree(
        tableName,
        config,
        nodeConfig => entityValues.TryGetValue(nodeConfig.PartitionColumn, out object? mapValue) ? mapValue : null,
        0,
        workItems);
    }

    await EnsureWorkItemsAsync(workItems, ct);
  }

  public async Task EnsurePartitionsAsync<T>(IEnumerable<T> entities, CancellationToken ct = default)
  {
    ArgumentNullException.ThrowIfNull(entities);

    if (!_initialized)
    {
      await InitializeAsync(ct);
    }

    (string tableName, TablePartitionConfig config) = FindConfigForType<T>();

    List<PartitionWorkItem> workItems = [];
    foreach (T entity in entities)
    {
      ArgumentNullException.ThrowIfNull(entity);
      ResolvePartitionTree(
        tableName,
        config,
        nodeConfig => nodeConfig.MappedProperty?.GetValue(entity),
        0,
        workItems);
    }

    await EnsureWorkItemsAsync(workItems, ct);
  }

  public async Task PreCreatePartitionsAsync(string tableName,
    DateTime from, DateTime to,
    CancellationToken ct = default)
  {
    if (!_tableConfigs.TryGetValue(tableName, out TablePartitionConfig? config))
      throw new InvalidOperationException($"Table '{tableName}' is not configured.");

    if (config.PartitionType != "range")
      throw new InvalidOperationException("PreCreatePartitions only works with range partitions.");

    if (!_initialized)
    {
      await InitializeAsync(ct);
    }

    DateTime current = from;
    while (current < to)
    {
      await EnsureRangePartitionAsync(tableName, config, current, ct);
      current = config.Interval switch
      {
        RangeInterval.Daily => current.AddDays(1),
        RangeInterval.Weekly => current.AddDays(7),
        RangeInterval.Monthly => current.AddMonths(1),
        RangeInterval.Quarterly => current.AddMonths(3),
        RangeInterval.Yearly => current.AddYears(1),
        _ => throw new InvalidOperationException($"Unsupported interval: {config.Interval}")
      };
    }
  }

  private async Task EnsurePartitionTreeAsync(
    string tableName,
    TablePartitionConfig config,
    Func<TablePartitionConfig, object?> valueResolver,
    CancellationToken ct)
  {
    object? value = valueResolver(config);
    if (value == null)
    {
      throw new InvalidOperationException(
        $"Value for partition column '{config.PartitionColumn}' not provided.");
    }

    if (config.PartitionType == "range")
    {
      string partitionName = await EnsureRangePartitionAsync(tableName, config, value, ct);

      if (config.SubPartition != null)
      {
        TablePartitionConfig childConfig = CloneConfigWithTableName(config.SubPartition, partitionName);
        await EnsurePartitionTreeAsync(partitionName, childConfig, valueResolver, ct);
      }

      return;
    }

    string listValue = FormatListValue(value);
    ListPartitionItemConfig? staticItem = PartitionResolver.ResolveConfiguredListPartition(config, listValue);
    if (staticItem != null)
    {
      string partitionName = PartitionResolver.ResolveListPartitionName(config, staticItem.Name);
      TablePartitionConfig? childTemplate = staticItem.ChildPartition ?? config.SubPartition;
      string? subPartitionBy = BuildSubPartitionByClause(childTemplate);
      string ddl = DdlBuilder.CreateListPartition(
        config.Schema,
        partitionName,
        config.TableName,
        staticItem.Values,
        subPartitionBy);

      await EnsurePartitionExistsAsync(tableName, config, partitionName, ddl, ct);

      if (childTemplate != null)
      {
        TablePartitionConfig childConfig = CloneConfigWithTableName(childTemplate, partitionName);
        await EnsurePartitionTreeAsync(partitionName, childConfig, valueResolver, ct);
      }

      return;
    }

    IReadOnlyList<ListPartitionItemConfig> configuredListPartitions =
      PartitionResolver.GetConfiguredListPartitions(config);

    if (config.AutoCreateForNewValues || configuredListPartitions.Count == 0)
    {
      ListPartitionInfo dynamicPartition = PartitionResolver.ResolveList(config, listValue);
      string? subPartitionBy = BuildSubPartitionByClause(config.SubPartition);
      string ddl = DdlBuilder.CreateListPartition(
        config.Schema,
        dynamicPartition.PartitionName,
        config.TableName,
        [listValue],
        subPartitionBy);

      await EnsurePartitionExistsAsync(tableName, config, dynamicPartition.PartitionName, ddl, ct);

      if (config.SubPartition != null)
      {
        TablePartitionConfig childConfig = CloneConfigWithTableName(
          config.SubPartition,
          dynamicPartition.PartitionName);
        await EnsurePartitionTreeAsync(dynamicPartition.PartitionName, childConfig, valueResolver, ct);
      }

      return;
    }

    if (config.HasRemainderPartition)
    {
      string partitionName = $"{config.TableName}_remainder";
      string? subPartitionBy = BuildSubPartitionByClause(config.SubPartition);
      string ddl = DdlBuilder.CreateDefaultPartition(
        config.Schema,
        partitionName,
        config.TableName,
        subPartitionBy);

      await EnsurePartitionExistsAsync(tableName, config, partitionName, ddl, ct);

      if (config.SubPartition != null)
      {
        TablePartitionConfig childConfig = CloneConfigWithTableName(config.SubPartition, partitionName);
        await EnsurePartitionTreeAsync(partitionName, childConfig, valueResolver, ct);
      }

      return;
    }

    throw new InvalidOperationException(
      $"No static list partition configured for value '{listValue}' on column '{config.PartitionColumn}'.");
  }

  private void ResolvePartitionTree(
    string tableName,
    TablePartitionConfig config,
    Func<TablePartitionConfig, object?> valueResolver,
    int level,
    List<PartitionWorkItem> workItems)
  {
    object? value = valueResolver(config);
    if (value == null)
    {
      throw new InvalidOperationException(
        $"Value for partition column '{config.PartitionColumn}' not provided.");
    }

    if (config.PartitionType == "range")
    {
      DateTime dateValue = NormalizeRangeValue(value);
      RangePartitionInfo resolved = PartitionResolver.ResolveRange(config, dateValue);
      string? subPartitionBy = BuildSubPartitionByClause(config.SubPartition);
      string ddl = DdlBuilder.CreateRangePartition(
        config.Schema,
        resolved.PartitionName,
        config.TableName,
        resolved.From,
        resolved.To,
        subPartitionBy);

      workItems.Add(new PartitionWorkItem(tableName, config, resolved.PartitionName, ddl, level));

      if (config.SubPartition != null)
      {
        TablePartitionConfig childConfig = CloneConfigWithTableName(config.SubPartition, resolved.PartitionName);
        ResolvePartitionTree(resolved.PartitionName, childConfig, valueResolver, level + 1, workItems);
      }

      return;
    }

    string listValue = FormatListValue(value);
    ListPartitionItemConfig? staticItem = PartitionResolver.ResolveConfiguredListPartition(config, listValue);
    if (staticItem != null)
    {
      string partitionName = PartitionResolver.ResolveListPartitionName(config, staticItem.Name);
      TablePartitionConfig? childTemplate = staticItem.ChildPartition ?? config.SubPartition;
      string? subPartitionBy = BuildSubPartitionByClause(childTemplate);
      string ddl = DdlBuilder.CreateListPartition(
        config.Schema,
        partitionName,
        config.TableName,
        staticItem.Values,
        subPartitionBy);

      workItems.Add(new PartitionWorkItem(tableName, config, partitionName, ddl, level));

      if (childTemplate != null)
      {
        TablePartitionConfig childConfig = CloneConfigWithTableName(childTemplate, partitionName);
        ResolvePartitionTree(partitionName, childConfig, valueResolver, level + 1, workItems);
      }

      return;
    }

    IReadOnlyList<ListPartitionItemConfig> configuredListPartitions =
      PartitionResolver.GetConfiguredListPartitions(config);

    if (config.AutoCreateForNewValues || configuredListPartitions.Count == 0)
    {
      ListPartitionInfo dynamicPartition = PartitionResolver.ResolveList(config, listValue);
      string? subPartitionBy = BuildSubPartitionByClause(config.SubPartition);
      string ddl = DdlBuilder.CreateListPartition(
        config.Schema,
        dynamicPartition.PartitionName,
        config.TableName,
        [listValue],
        subPartitionBy);

      workItems.Add(new PartitionWorkItem(tableName, config, dynamicPartition.PartitionName, ddl, level));

      if (config.SubPartition != null)
      {
        TablePartitionConfig childConfig = CloneConfigWithTableName(
          config.SubPartition,
          dynamicPartition.PartitionName);
        ResolvePartitionTree(dynamicPartition.PartitionName, childConfig, valueResolver, level + 1, workItems);
      }

      return;
    }

    if (config.HasRemainderPartition)
    {
      string partitionName = $"{config.TableName}_remainder";
      string? subPartitionBy = BuildSubPartitionByClause(config.SubPartition);
      string ddl = DdlBuilder.CreateDefaultPartition(
        config.Schema,
        partitionName,
        config.TableName,
        subPartitionBy);

      workItems.Add(new PartitionWorkItem(tableName, config, partitionName, ddl, level));

      if (config.SubPartition != null)
      {
        TablePartitionConfig childConfig = CloneConfigWithTableName(config.SubPartition, partitionName);
        ResolvePartitionTree(partitionName, childConfig, valueResolver, level + 1, workItems);
      }

      return;
    }

    throw new InvalidOperationException(
      $"No static list partition configured for value '{listValue}' on column '{config.PartitionColumn}'.");
  }

  private async Task InitializeStaticPartitionsAsync(
    NpgsqlConnection connection,
    string tableName,
    TablePartitionConfig config,
    CancellationToken ct)
  {
    if (config.PartitionType != "list")
    {
      return;
    }

    IReadOnlyList<ListPartitionItemConfig> configuredListPartitions =
      PartitionResolver.GetConfiguredListPartitions(config);

    foreach (ListPartitionItemConfig item in configuredListPartitions)
    {
      string partitionName = PartitionResolver.ResolveListPartitionName(config, item.Name);
      TablePartitionConfig? childTemplate = item.ChildPartition ?? config.SubPartition;
      string? subPartitionBy = BuildSubPartitionByClause(childTemplate);
      string ddl = DdlBuilder.CreateListPartition(
        config.Schema,
        partitionName,
        config.TableName,
        item.Values,
        subPartitionBy);

      await EnsurePartitionExistsAsync(tableName, config, partitionName, ddl, connection, ct);

      if (childTemplate != null)
      {
        TablePartitionConfig childConfig = CloneConfigWithTableName(childTemplate, partitionName);
        await InitializeStaticPartitionsAsync(connection, partitionName, childConfig, ct);
      }
    }

    if (!config.HasRemainderPartition)
    {
      return;
    }

    string remainderPartitionName = $"{config.TableName}_remainder";
    string? remainderSubPartitionBy = BuildSubPartitionByClause(config.SubPartition);
    string remainderDdl = DdlBuilder.CreateDefaultPartition(
      config.Schema,
      remainderPartitionName,
      config.TableName,
      remainderSubPartitionBy);

    await EnsurePartitionExistsAsync(tableName, config, remainderPartitionName, remainderDdl, connection, ct);

    if (config.SubPartition != null)
    {
      TablePartitionConfig childConfig = CloneConfigWithTableName(config.SubPartition, remainderPartitionName);
      await InitializeStaticPartitionsAsync(connection, remainderPartitionName, childConfig, ct);
    }
  }

  private async Task<string> EnsureRangePartitionAsync(
    string tableName,
    TablePartitionConfig config,
    object value,
    CancellationToken ct)
  {
    DateTime dateValue = NormalizeRangeValue(value);
    RangePartitionInfo resolved = PartitionResolver.ResolveRange(config, dateValue);
    string? subPartitionBy = BuildSubPartitionByClause(config.SubPartition);
    string ddl = DdlBuilder.CreateRangePartition(
      config.Schema,
      resolved.PartitionName,
      config.TableName,
      resolved.From,
      resolved.To,
      subPartitionBy);

    await EnsurePartitionExistsAsync(tableName, config, resolved.PartitionName, ddl, ct);
    return resolved.PartitionName;
  }

  private async Task EnsurePartitionExistsAsync(
    string tableName,
    TablePartitionConfig config,
    string partitionName,
    string ddl,
    CancellationToken ct)
  {
    await using NpgsqlConnection connection = new NpgsqlConnection(_connectionString);
    await connection.OpenAsync(ct);
    await EnsurePartitionExistsAsync(tableName, config, partitionName, ddl, connection, ct);
  }

  private async Task EnsurePartitionExistsAsync(
    string tableName,
    TablePartitionConfig config,
    string partitionName,
    string ddl,
    NpgsqlConnection connection,
    CancellationToken ct)
  {
    PartitionCache cache = GetOrCreateCache(tableName);

    if (cache.Exists(partitionName))
    {
      return;
    }

    if (await CatalogQueries.TableExistsAsync(connection, partitionName, config.Schema))
    {
      cache.MarkExists(partitionName);
      return;
    }

    bool lockAcquired = await _lock.AcquireAsync(connection, partitionName, ct);
    if (!lockAcquired)
      throw new InvalidOperationException($"Failed to acquire lock for partition '{partitionName}'.");

    try
    {
      if (await CatalogQueries.TableExistsAsync(connection, partitionName, config.Schema))
      {
        cache.MarkExists(partitionName);
        return;
      }

      await using NpgsqlCommand cmd = new NpgsqlCommand(ddl, connection);
      await cmd.ExecuteNonQueryAsync(ct);

      if (_hashedNames)
      {
        PartitionNamingService namingService = new PartitionNamingService(
          config.TableName, "part", _hashedNames, config.Schema);
        Core.Models.PartitionItem commentItem = new Core.Models.PartitionItem { Field = config.PartitionColumn, Name = partitionName };
        string? commentSql = namingService.GetCommentSql(
          partitionName, commentItem, config.PartitionType, partitionName, false);
        if (commentSql != null)
        {
          await using NpgsqlCommand commentCmd = new NpgsqlCommand(commentSql, connection);
          await commentCmd.ExecuteNonQueryAsync(ct);
        }
      }

      cache.MarkExists(partitionName);
    }
    finally
    {
      await _lock.ReleaseAsync(connection, partitionName, ct);
    }
  }

  private async Task EnsureWorkItemsAsync(
    List<PartitionWorkItem> workItems,
    CancellationToken ct)
  {
    if (workItems.Count == 0)
    {
      return;
    }

    List<PartitionWorkItem> uniqueItems = workItems
      .GroupBy(w => w.PartitionName)
      .Select(g => g.First())
      .OrderBy(w => w.Level)
      .ToList();

    uniqueItems.RemoveAll(w => GetOrCreateCache(w.TableName).Exists(w.PartitionName));

    if (uniqueItems.Count == 0)
    {
      return;
    }

    await using NpgsqlConnection connection = new NpgsqlConnection(_connectionString);
    await connection.OpenAsync(ct);

    foreach (PartitionWorkItem item in uniqueItems)
    {
      await EnsurePartitionExistsAsync(item.TableName, item.Config, item.PartitionName, item.Ddl, connection, ct);
    }
  }

  private static DateTime NormalizeRangeValue(object value)
  {
    if (value is DateTimeOffset dateTimeOffset)
    {
      return dateTimeOffset.UtcDateTime;
    }

    return Convert.ToDateTime(value, CultureInfo.InvariantCulture);
  }

  private static string FormatListValue(object value)
  {
    if (value is DateTimeOffset dateTimeOffset)
    {
      return dateTimeOffset.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);
    }

    if (value is DateTime dateTime)
    {
      return dateTime.ToString("O", CultureInfo.InvariantCulture);
    }

    if (value is IFormattable formattable)
    {
      return formattable.ToString(null, CultureInfo.InvariantCulture);
    }

    return value.ToString()!;
  }

  private static string? BuildSubPartitionByClause(TablePartitionConfig? childConfig)
  {
    if (childConfig == null)
    {
      return null;
    }

    return $"{childConfig.PartitionType.ToUpperInvariant()} ({childConfig.PartitionColumn})";
  }

  private static TablePartitionConfig CloneConfigWithTableName(TablePartitionConfig source, string newTableName)
  {
    return new TablePartitionConfig
    {
      TableName = newTableName,
      Schema = source.Schema,
      SchemaExplicitlySet = source.SchemaExplicitlySet,
      PartitionType = source.PartitionType,
      PartitionColumn = source.PartitionColumn,
      MappedProperty = source.MappedProperty,
      Interval = source.Interval,
      KnownValues = source.KnownValues,
      ListPartitions = source.ListPartitions,
      AutoCreateForNewValues = source.AutoCreateForNewValues,
      HasRemainderPartition = source.HasRemainderPartition,
      CreateIfNotExistsColumns = source.CreateIfNotExistsColumns,
      SubPartition = source.SubPartition,
      PreCreateAhead = source.PreCreateAhead
    };
  }

  private (string tableName, TablePartitionConfig config) FindConfigForType<T>()
  {
    Type entityType = typeof(T);
    foreach ((string tableName, TablePartitionConfig config) in _tableConfigs)
    {
      if (config.MappedProperty?.DeclaringType == entityType)
        return (tableName, config);
    }

    throw new InvalidOperationException(
      $"No table configuration found for entity type '{entityType.Name}'. " +
      $"Use .MappedFrom<{entityType.Name}>() in the table configuration.");
  }

  private PartitionCache GetOrCreateCache(string tableName)
  {
    return _caches.GetOrAdd(tableName, _ => new PartitionCache());
  }

  public ValueTask DisposeAsync()
  {
    _caches.Clear();
    _initLock.Dispose();
    return ValueTask.CompletedTask;
  }
}
