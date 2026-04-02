using Testcontainers.PostgreSql;

namespace InitializeNL.PgPartitionSmith.Tests.Integration;

public class PostgresFixture : IAsyncLifetime
{
  private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17-alpine")
    .Build();

  public string ConnectionString => _container.GetConnectionString();

  public async Task InitializeAsync()
  {
    await _container.StartAsync();
  }

  public async Task DisposeAsync()
  {
    await _container.DisposeAsync().AsTask();
  }
}

[CollectionDefinition("Postgres")]
public class PostgresCollection : ICollectionFixture<PostgresFixture>;
