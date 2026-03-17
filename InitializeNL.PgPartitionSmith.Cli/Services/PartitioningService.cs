using System.Text;
using Dapper;
using Npgsql;
using InitializeNL.PgPartitionSmith.Core.Catalog;
using InitializeNL.PgPartitionSmith.Core.Ddl;
using InitializeNL.PgPartitionSmith.Core.Models;
using InitializeNL.PgPartitionSmith.Core.Naming;

namespace InitializeNL.PgPartitionSmith.Cli.Services;

public class PartitioningService
{
  private readonly string _connectionString;
  private readonly string _schema;
  private readonly bool _dryRun;
  private readonly string _partitionedName;
  private readonly string _unpartitionedName;
  private readonly PartitionNamingService _namingService;

  public PartitioningService(
    string connectionString,
    string schema,
    bool dryRun,
    string partitionedName,
    string unpartitionedName,
    PartitionNamingService namingService)
  {
    _connectionString = connectionString;
    _schema = schema;
    _dryRun = dryRun;
    _partitionedName = partitionedName;
    _unpartitionedName = unpartitionedName;
    _namingService = namingService;
  }

  public async Task PartitionTableAsync(string tableName, PartitionSchema schema)
  {
    using NpgsqlConnection connection = new NpgsqlConnection(_connectionString);
    await connection.OpenAsync();

    bool isRerun = await CatalogQueries.TableExistsAsync(connection, $"{tableName}_{_partitionedName}", _schema);

    if (isRerun)
    {
      await RepartitionRemainderAsync(connection, tableName, schema);
    }
    else
    {
      await PartitionFirstRunAsync(connection, tableName, schema);
    }
  }

  private async Task PartitionFirstRunAsync(
    NpgsqlConnection connection,
    string tableName,
    PartitionSchema schema)
  {
    using NpgsqlTransaction transaction = await connection.BeginTransactionAsync();
    try
    {
      string partitionField = schema.Partitions.Items[0].Field;

      // Step 1: Read columns before mutating schema
      List<ColumnInfo> originalColumns = await GetColumnsAsync(connection, tableName);

      // Step 2: Add __is_partitioned column to original table
      await ExecuteSqlAsync(
        connection,
        $"ALTER TABLE {Qualified(tableName)} ADD COLUMN __is_partitioned boolean DEFAULT false",
        transaction);

      // Step 3: Build a column list that includes __is_partitioned
      List<ColumnInfo> columns =
      [
        .. originalColumns,
        new ColumnInfo
        {
          ColumnName = "__is_partitioned",
          FullType = "boolean",
          ColumnDefault = "false",
          OrdinalPosition = originalColumns.Count + 1
        }
      ];

      // Step 4: Create _partitioned table with the full column set
      string partitionedDdl = BuildCreateTableDdl(
        $"{tableName}_{_partitionedName}",
        columns,
        partitionField,
        schema.Partitions.Type);

      await ExecuteSqlAsync(connection, partitionedDdl, transaction);

      // Step 5: Create nested partitions recursively
      await CreatePartitionsAsync(
        connection,
        $"{tableName}_{_partitionedName}",
        schema.Partitions,
        transaction,
        pathPrefix: "");

      // Step 6: Copy data from original table into _partitioned with __is_partitioned = true
      string sourceColumnNames = string.Join(", ", originalColumns.Select(c => c.ColumnName));
      await ExecuteSqlAsync(
        connection,
        $"INSERT INTO {Qualified($"{tableName}_{_partitionedName}")} (__is_partitioned, {sourceColumnNames}) SELECT true, {sourceColumnNames} FROM {Qualified(tableName)}",
        transaction);

      // Step 7: Rename original table to _unpartitioned
      await ExecuteSqlAsync(
        connection,
        $"ALTER TABLE {Qualified(tableName)} RENAME TO {tableName}_{_unpartitionedName}",
        transaction);

      // Step 8: Create parent table partitioned by __is_partitioned
      string columnList = BuildColumnList(columns);
      string parentDdl = $"CREATE TABLE {Qualified($"{tableName}_parent_temp")} ({columnList}) PARTITION BY LIST (__is_partitioned)";
      await ExecuteSqlAsync(connection, parentDdl, transaction);

      // Step 9: Attach _unpartitioned as the false partition
      await ExecuteSqlAsync(
        connection,
        $"ALTER TABLE {Qualified($"{tableName}_parent_temp")} ATTACH PARTITION {Qualified($"{tableName}_{_unpartitionedName}")} FOR VALUES IN (false)",
        transaction);

      // Step 10: Attach _partitioned as the true partition
      await ExecuteSqlAsync(
        connection,
        $"ALTER TABLE {Qualified($"{tableName}_parent_temp")} ATTACH PARTITION {Qualified($"{tableName}_{_partitionedName}")} FOR VALUES IN (true)",
        transaction);

      // Step 11: Rename parent to original table name
      await ExecuteSqlAsync(
        connection,
        $"ALTER TABLE {Qualified($"{tableName}_parent_temp")} RENAME TO {tableName}",
        transaction);

      // Step 12: Recreate indexes from original table
      await RecreateIndexesAsync(connection, $"{tableName}_{_unpartitionedName}", $"{tableName}_{_partitionedName}", transaction);

      await transaction.CommitAsync();
    }
    catch
    {
      await transaction.RollbackAsync();
      throw;
    }
  }

  private async Task RepartitionRemainderAsync(
    NpgsqlConnection connection,
    string tableName,
    PartitionSchema schema)
  {
    using NpgsqlTransaction transaction = await connection.BeginTransactionAsync();
    try
    {
      await RepartitionRemainderRecursiveAsync(
        connection,
        $"{tableName}_{_partitionedName}",
        schema.Partitions,
        transaction);

      await transaction.CommitAsync();
    }
    catch
    {
      await transaction.RollbackAsync();
      throw;
    }
  }

  private async Task RepartitionRemainderRecursiveAsync(
    NpgsqlConnection connection,
    string parentTableName,
    PartitionDefinition definition,
    NpgsqlTransaction transaction,
    string pathPrefix = "")
  {
    string remainderPath = pathPrefix == "" ? "remainder" : $"{pathPrefix}/remainder";
    string remainderName = _namingService.GetPartitionTableName(parentTableName, "remainder", remainderPath);

    if (!await CatalogQueries.TableExistsAsync(connection, remainderName, _schema))
    {
      return;
    }

    // Check if remainder has any data
    long remainderCount = await connection.QuerySingleAsync<long>(
      $"SELECT count(*) FROM {Qualified(remainderName)}",
      transaction: transaction);

    if (remainderCount == 0)
    {
      return;
    }

    // Detach the remainder partition
    await ExecuteSqlAsync(
      connection,
      $"ALTER TABLE {Qualified(parentTableName)} DETACH PARTITION {Qualified(remainderName)}",
      transaction);

    // Create new partitions (excluding remainder — it gets re-added)
    List<PartitionItem> nonRemainderItems = definition.Items
      .Where(i => !i.IsRemainder)
      .ToList();

    foreach (PartitionItem item in nonRemainderItems)
    {
      string path = pathPrefix == "" ? item.Name : $"{pathPrefix}/{item.Name}";
      string partitionName = _namingService.GetPartitionTableName(parentTableName, item.Name, path);

      if (await CatalogQueries.TableExistsAsync(connection, partitionName, _schema))
      {
        continue;
      }

      string createSql = BuildPartitionOfSql(parentTableName, partitionName, definition.Type, item, item.Partitions);
      await ExecuteSqlAsync(connection, createSql, transaction);

      string? commentSql = _namingService.GetCommentSql(partitionName, item, definition.Type, path, item.IsRemainder);
      if (commentSql != null)
      {
        await ExecuteSqlAsync(connection, commentSql, transaction);
      }

      if (item.Partitions != null)
      {
        await CreatePartitionsAsync(connection, partitionName, item.Partitions, transaction, path);
      }
    }

    // Move data from detached remainder into parent (which routes to new partitions)
    await ExecuteSqlAsync(
      connection,
      $"INSERT INTO {Qualified(parentTableName)} SELECT * FROM {Qualified(remainderName)}",
      transaction);

    // Drop old remainder
    await ExecuteSqlAsync(
      connection,
      $"DROP TABLE {Qualified(remainderName)}",
      transaction);

    // Re-create remainder if needed
    if (definition.HasRemainder)
    {
      PartitionItem? remainderItem = definition.Items.FirstOrDefault(i => i.IsRemainder);
      if (remainderItem != null)
      {
        string createSql = BuildPartitionOfSql(parentTableName, remainderName, definition.Type, remainderItem, remainderItem.Partitions);
        await ExecuteSqlAsync(connection, createSql, transaction);

        string? commentSql = _namingService.GetCommentSql(
          remainderName,
          remainderItem,
          definition.Type,
          remainderPath,
          remainderItem.IsRemainder);
        if (commentSql != null)
        {
          await ExecuteSqlAsync(connection, commentSql, transaction);
        }
      }
    }

    // Recurse into child partitions that have their own remainders
    foreach (PartitionItem item in nonRemainderItems)
    {
      if (item.Partitions != null)
      {
        string path = pathPrefix == "" ? item.Name : $"{pathPrefix}/{item.Name}";
        string partitionName = _namingService.GetPartitionTableName(parentTableName, item.Name, path);
        await RepartitionRemainderRecursiveAsync(connection, partitionName, item.Partitions, transaction, path);
      }
    }
  }

  private async Task CreatePartitionsAsync(
    NpgsqlConnection connection,
    string parentTableName,
    PartitionDefinition definition,
    NpgsqlTransaction transaction,
    string pathPrefix = "")
  {
    foreach (PartitionItem item in definition.Items)
    {
      string path = pathPrefix == "" ? item.Name : $"{pathPrefix}/{item.Name}";
      string partitionName = _namingService.GetPartitionTableName(parentTableName, item.Name, path);
      string createSql = BuildPartitionOfSql(parentTableName, partitionName, definition.Type, item, item.Partitions);
      await ExecuteSqlAsync(connection, createSql, transaction);

      string? commentSql = _namingService.GetCommentSql(partitionName, item, definition.Type, path, item.IsRemainder);
      if (commentSql != null)
      {
        await ExecuteSqlAsync(connection, commentSql, transaction);
      }

      if (item.Partitions != null)
      {
        await CreatePartitionsAsync(connection, partitionName, item.Partitions, transaction, path);
      }
    }
  }

  private string BuildPartitionOfSql(
    string parentTableName,
    string partitionName,
    string partitionType,
    PartitionItem item,
    PartitionDefinition? subPartitions = null)
  {
    string? subPartitionByClause = BuildSubPartitionByClause(subPartitions);

    if (item.IsRemainder)
    {
      return DdlBuilder.CreateDefaultPartition(_schema, partitionName, parentTableName, subPartitionByClause);
    }

    if (partitionType == "list")
    {
      return DdlBuilder.CreateListPartition(_schema, partitionName, parentTableName, item.Values!, subPartitionByClause);
    }

    if (partitionType == "range")
    {
      return DdlBuilder.CreateRangePartition(_schema, partitionName, parentTableName, item.From!, item.To!, subPartitionByClause);
    }

    throw new InvalidOperationException($"Unknown partition type: {partitionType}");
  }

  private string? BuildSubPartitionByClause(PartitionDefinition? subPartitions)
  {
    if (subPartitions == null)
    {
      return null;
    }

    string partitionField = subPartitions.Items[0].Field;
    return $"{subPartitions.Type.ToUpperInvariant()} ({partitionField})";
  }

  private async Task<List<ColumnInfo>> GetColumnsAsync(NpgsqlConnection connection, string tableName)
  {
    IEnumerable<ColumnInfo> columns = await CatalogQueries.GetColumnInfoAsync(connection, tableName, _schema);
    return columns.ToList();
  }

  private string BuildCreateTableDdl(
    string tableName,
    List<ColumnInfo> columns,
    string partitionField,
    string partitionType)
  {
    StringBuilder ddl = new StringBuilder();
    ddl.Append($"CREATE TABLE {Qualified(tableName)} (");

    List<string> columnDefs = [];
    foreach (ColumnInfo column in columns)
    {
      StringBuilder colDef = new StringBuilder();
      colDef.Append($"{column.ColumnName} {column.FullType}");

      if (column.IsNotNull)
      {
        colDef.Append(" NOT NULL");
      }

      if (column.ColumnDefault != null)
      {
        colDef.Append($" DEFAULT {column.ColumnDefault}");
      }

      columnDefs.Add(colDef.ToString());
    }

    ddl.Append(string.Join(", ", columnDefs));
    ddl.Append($") PARTITION BY {partitionType.ToUpperInvariant()} ({partitionField})");

    return ddl.ToString();
  }

  private string BuildColumnList(List<ColumnInfo> columns)
  {
    List<string> columnDefs = [];
    foreach (ColumnInfo column in columns)
    {
      StringBuilder colDef = new StringBuilder();
      colDef.Append($"{column.ColumnName} {column.FullType}");

      if (column.IsNotNull)
      {
        colDef.Append(" NOT NULL");
      }

      if (column.ColumnDefault != null)
      {
        colDef.Append($" DEFAULT {column.ColumnDefault}");
      }

      columnDefs.Add(colDef.ToString());
    }

    return string.Join(", ", columnDefs);
  }

  private async Task RecreateIndexesAsync(
    NpgsqlConnection connection,
    string sourceTableName,
    string targetTableName,
    NpgsqlTransaction transaction)
  {
    IEnumerable<string> indexDefs = await connection.QueryAsync<string>(
      @"SELECT pg_get_indexdef(i.indexrelid)
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = @tableName
          AND n.nspname = @schema
          AND NOT i.indisprimary",
      new { tableName = sourceTableName, schema = _schema });

    foreach (string indexDef in indexDefs)
    {
      string newIndexDef = indexDef
        .Replace(Qualified(sourceTableName), Qualified(targetTableName))
        .Replace($"ON {sourceTableName}", $"ON {targetTableName}");

      string newDef = newIndexDef.Replace(
        $"INDEX {sourceTableName}",
        $"INDEX {targetTableName}");

      await ExecuteSqlAsync(connection, newDef, transaction);
    }
  }

  private string Qualified(string tableName) =>
    DdlBuilder.Qualified(_schema, tableName);

  private async Task ExecuteSqlAsync(
    NpgsqlConnection connection,
    string sql,
    NpgsqlTransaction? transaction = null)
  {
    // Note: dry-run still reads metadata from the database, it only skips DDL/DML mutations.
    if (_dryRun)
    {
      Console.WriteLine($"{sql};");
      Console.WriteLine();
      return;
    }

    await connection.ExecuteAsync(sql, transaction: transaction);
  }
}
