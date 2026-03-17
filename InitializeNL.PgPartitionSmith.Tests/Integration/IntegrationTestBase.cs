using Dapper;
using Npgsql;

namespace InitializeNL.PgPartitionSmith.Tests.Integration;

public abstract class IntegrationTestBase : IAsyncLifetime
{
  protected string ConnectionString { get; private set; } = null!;
  protected string TestSchema { get; } = $"test_{Guid.NewGuid():N}"[..20];

  public async Task InitializeAsync()
  {
    ConnectionString = Environment.GetEnvironmentVariable("PGPARTITION_TEST_CONNSTRING")
      ?? "Host=localhost;Database=pgpartition_test;Username=postgres;Password=postgres";

    try
    {
      await using NpgsqlConnection conn = new NpgsqlConnection(ConnectionString);
      await conn.OpenAsync();
      await conn.ExecuteAsync($"CREATE SCHEMA IF NOT EXISTS {TestSchema}");
    }
    catch (Exception ex)
    {
      throw Xunit.Sdk.SkipException.ForSkip($"PostgreSQL not available: {ex.Message}");
    }
  }

  public async Task DisposeAsync()
  {
    try
    {
      await using NpgsqlConnection conn = new NpgsqlConnection(ConnectionString);
      await conn.OpenAsync();
      await conn.ExecuteAsync($"DROP SCHEMA IF EXISTS {TestSchema} CASCADE");
    }
    catch
    {
      // Ignore cleanup errors
    }
  }
}
