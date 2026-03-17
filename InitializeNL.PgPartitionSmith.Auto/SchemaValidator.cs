using Npgsql;
using InitializeNL.PgPartitionSmith.Auto.Configuration;
using InitializeNL.PgPartitionSmith.Core.Catalog;
using InitializeNL.PgPartitionSmith.Core.Ddl;

namespace InitializeNL.PgPartitionSmith.Auto;

public static class SchemaValidator
{
  public static async Task ValidateAndInitializeAsync(
    NpgsqlConnection connection,
    TablePartitionConfig config,
    CancellationToken ct = default)
  {
    bool tableExists = await CatalogQueries.TableExistsAsync(
      connection, config.TableName, config.Schema);

    if (!tableExists)
    {
      if (config.CreateIfNotExistsColumns != null)
      {
        string ddl = DdlBuilder.CreatePartitionedTable(
          config.Schema,
          config.TableName,
          config.CreateIfNotExistsColumns,
          config.PartitionType,
          config.PartitionColumn);
        await using NpgsqlCommand cmd = new NpgsqlCommand(ddl, connection);
        await cmd.ExecuteNonQueryAsync(ct);
      }
      else
      {
        throw new InvalidOperationException(
          $"Table '{config.Schema}.{config.TableName}' does not exist. " +
          $"Create it manually with PARTITION BY {config.PartitionType.ToUpperInvariant()} ({config.PartitionColumn}), " +
          $"or use .CreateIfNotExists() in the configuration.");
      }
    }
    else
    {
      bool isPartitioned = await CatalogQueries.IsPartitionedTableAsync(
        connection, config.TableName, config.Schema);

      if (!isPartitioned)
      {
        throw new InvalidOperationException(
          $"Table '{config.Schema}.{config.TableName}' exists but is not partitioned. " +
          $"Use pg-partition-smith CLI to migrate it, or recreate it with PARTITION BY.");
      }
    }
  }
}
