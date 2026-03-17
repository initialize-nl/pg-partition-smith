using System.Text.Json;
using Dapper;
using Npgsql;
using InitializeNL.PgPartitionSmith.Core.Models;

namespace InitializeNL.PgPartitionSmith.Cli.Services;

public class PartitionSchemaService
{
  private readonly string _connectionString;

  public PartitionSchemaService(string connectionString)
  {
    _connectionString = connectionString;
  }

  public async Task<PartitionSchema> GenerateSchemaFromQueryAsync(string query)
  {
    using NpgsqlConnection connection = new NpgsqlConnection(_connectionString);
    await connection.OpenAsync();

    string jsonResult = await connection.QuerySingleAsync<string>(query);
    PartitionSchema schema = JsonSerializer.Deserialize<PartitionSchema>(jsonResult)
      ?? throw new InvalidOperationException("Failed to deserialize partition schema.");

    AddRemainderPartitions(schema.Partitions);
    schema.Validate();

    return schema;
  }

  private void AddRemainderPartitions(PartitionDefinition definition)
  {
    foreach (PartitionItem item in definition.Items)
    {
      if (item.Partitions != null)
      {
        AddRemainderPartitions(item.Partitions);
      }
    }

    if (!definition.HasRemainder || definition.Items.Count == 0)
    {
      return;
    }

    PartitionItem firstItem = definition.Items[0];
    definition.Items.Add(new PartitionItem
    {
      Field = firstItem.Field,
      Name = "remainder",
      IsRemainder = true
    });
  }
}
