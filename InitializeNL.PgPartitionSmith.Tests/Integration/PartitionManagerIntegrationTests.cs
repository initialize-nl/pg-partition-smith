using Dapper;
using FluentAssertions;
using Npgsql;
using InitializeNL.PgPartitionSmith.Auto;
using InitializeNL.PgPartitionSmith.Auto.Configuration;

namespace InitializeNL.PgPartitionSmith.Tests.Integration;

[Trait("Category", "Integration")]
public class PartitionManagerIntegrationTests : IntegrationTestBase
{
  [Fact]
  public async Task EnsurePartition_Range_CreatesMonthlyPartitionAsync()
  {
    // Arrange: create partitioned table
    await using NpgsqlConnection conn = new NpgsqlConnection(ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {TestSchema}.situations (
        id BIGSERIAL,
        period_start TIMESTAMPTZ NOT NULL,
        title TEXT,
        PRIMARY KEY (id, period_start)
      ) PARTITION BY RANGE (period_start)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("situations", t => t
        .RangeBy("period_start")
        .Monthly()
        .WithSchema(TestSchema)
      )
      .Build();

    // Act
    await manager.EnsurePartitionAsync("situations",
      new Dictionary<string, object> { ["period_start"] = new DateTime(2026, 4, 15) });

    // Assert
    bool exists = await conn.QuerySingleAsync<bool>(
      $"SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'situations_2026_04' AND n.nspname = @schema)",
      new { schema = TestSchema });
    exists.Should().BeTrue();

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task EnsurePartition_Range_WithDateTimeOffset_UsesUtcBoundaryAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {TestSchema}.utc_events (
        id BIGSERIAL,
        period_start TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (id, period_start)
      ) PARTITION BY RANGE (period_start)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("utc_events", t => t
        .RangeBy("period_start")
        .Monthly()
        .WithSchema(TestSchema)
      )
      .Build();

    DateTimeOffset localBoundary = new DateTimeOffset(2026, 5, 1, 0, 30, 0, TimeSpan.FromHours(2));

    await manager.EnsurePartitionAsync("utc_events",
      new Dictionary<string, object> { ["period_start"] = localBoundary });

    bool aprilExists = await conn.QuerySingleAsync<bool>(
      "SELECT EXISTS(" +
      "SELECT 1 FROM pg_class c " +
      "JOIN pg_namespace n ON n.oid = c.relnamespace " +
      "WHERE c.relname = 'utc_events_2026_04' AND n.nspname = @schema)",
      new { schema = TestSchema });

    aprilExists.Should().BeTrue();

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task EnsurePartition_CalledTwice_OnlyCreatesOnceAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {TestSchema}.logs (
        id BIGSERIAL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (id, created_at)
      ) PARTITION BY RANGE (created_at)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("logs", t => t
        .RangeBy("created_at")
        .Monthly()
        .WithSchema(TestSchema)
      )
      .Build();

    // Act: call twice
    Dictionary<string, object> values = new Dictionary<string, object> { ["created_at"] = new DateTime(2026, 6, 1) };
    await manager.EnsurePartitionAsync("logs", values);
    await manager.EnsurePartitionAsync("logs", values);  // should hit cache

    // Assert: still only one partition
    int count = await conn.QuerySingleAsync<int>(
      $"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'logs_2026_06' AND n.nspname = @schema",
      new { schema = TestSchema });
    count.Should().Be(1);

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task PreCreatePartitions_CreatesMultipleMonthsAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {TestSchema}.metrics (
        id BIGSERIAL,
        recorded_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (id, recorded_at)
      ) PARTITION BY RANGE (recorded_at)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("metrics", t => t
        .RangeBy("recorded_at")
        .Monthly()
        .WithSchema(TestSchema)
      )
      .Build();

    // Act
    await manager.PreCreatePartitionsAsync("metrics",
      new DateTime(2026, 1, 1), new DateTime(2026, 4, 1));

    // Assert: 3 partitions (jan, feb, mar)
    int count = await conn.QuerySingleAsync<int>(
      $"SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent JOIN pg_namespace n ON n.oid = p.relnamespace WHERE p.relname = 'metrics' AND n.nspname = @schema",
      new { schema = TestSchema });
    count.Should().Be(3);

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task EnsurePartition_List_CreatesListPartitionAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {TestSchema}.events (
        id BIGSERIAL,
        region TEXT NOT NULL,
        title TEXT,
        PRIMARY KEY (id, region)
      ) PARTITION BY LIST (region)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("events", t => t
        .ListBy("region")
        .AutoCreateForNewValues()
        .WithSchema(TestSchema)
      )
      .Build();

    // Act
    await manager.EnsurePartitionAsync("events",
      new Dictionary<string, object> { ["region"] = "europe" });

    // Assert
    bool exists = await conn.QuerySingleAsync<bool>(
      $"SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'events_europe' AND n.nspname = @schema)",
      new { schema = TestSchema });
    exists.Should().BeTrue();

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task CreateIfNotExists_CreatesPartitionedTableAsync()
  {
    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("auto_table", t => t
        .RangeBy("ts")
        .Monthly()
        .WithSchema(TestSchema)
        .CreateIfNotExists("id BIGSERIAL, ts TIMESTAMPTZ NOT NULL, PRIMARY KEY (id, ts)")
      )
      .Build();

    // Act: initialize creates the table
    await manager.InitializeAsync();

    // Assert: table exists and is partitioned
    await using NpgsqlConnection conn = new NpgsqlConnection(ConnectionString);
    await conn.OpenAsync();
    bool exists = await conn.QuerySingleAsync<bool>(
      $"SELECT EXISTS(SELECT 1 FROM pg_partitioned_table pt JOIN pg_class c ON c.oid = pt.partrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'auto_table' AND n.nspname = @schema)",
      new { schema = TestSchema });
    exists.Should().BeTrue();

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task EnsurePartition_WithTableLock_CreatesPartitionAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {TestSchema}.locked_table (
        id BIGSERIAL,
        period_start TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (id, period_start)
      ) PARTITION BY RANGE (period_start)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(ConnectionString)
      .UseTableLock()
      .ForTable("locked_table", t => t
        .RangeBy("period_start")
        .Monthly()
        .WithSchema(TestSchema)
      )
      .Build();

    // Act
    await manager.EnsurePartitionAsync("locked_table",
      new Dictionary<string, object> { ["period_start"] = new DateTime(2026, 7, 1) });

    // Assert
    bool exists = await conn.QuerySingleAsync<bool>(
      $"SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'locked_table_2026_07' AND n.nspname = @schema)",
      new { schema = TestSchema });
    exists.Should().BeTrue();

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task EnsurePartition_ListToRange_CreatesChildRangePartitionAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {TestSchema}.situations (
        id BIGSERIAL,
        status TEXT NOT NULL,
        period_start TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (id, status, period_start)
      ) PARTITION BY LIST (status)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("situations", t => t
        .ListBy("status")
        .WithPartition("active", ["active"], child => child
          .RangeBy("period_start")
          .Monthly())
        .WithSchema(TestSchema)
      )
      .Build();

    await manager.EnsurePartitionAsync("situations",
      new Dictionary<string, object>
      {
        ["status"] = "active",
        ["period_start"] = new DateTime(2026, 4, 15)
      });

    bool listPartitionExists = await conn.QuerySingleAsync<bool>(
      "SELECT EXISTS(" +
      "SELECT 1 FROM pg_class c " +
      "JOIN pg_namespace n ON n.oid = c.relnamespace " +
      "WHERE c.relname = 'situations_active' AND n.nspname = @schema)",
      new { schema = TestSchema });
    bool rangePartitionExists = await conn.QuerySingleAsync<bool>(
      "SELECT EXISTS(" +
      "SELECT 1 FROM pg_class c " +
      "JOIN pg_namespace n ON n.oid = c.relnamespace " +
      "WHERE c.relname = 'situations_active_2026_04' AND n.nspname = @schema)",
      new { schema = TestSchema });

    listPartitionExists.Should().BeTrue();
    rangePartitionExists.Should().BeTrue();

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task EnsurePartitions_Bulk_CreatesAllNeededPartitionsAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync(
      $@"
      CREATE TABLE {TestSchema}.bulk_events (
        id BIGSERIAL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (id, created_at)
      ) PARTITION BY RANGE (created_at)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(ConnectionString)
      .UseAdvisoryLocks()
      .ForTable(
        "bulk_events",
        t => t
          .RangeBy("created_at")
          .Monthly()
          .WithSchema(TestSchema)
      )
      .Build();

    List<IDictionary<string, object>> batch =
    [
      new Dictionary<string, object> { ["created_at"] = new DateTime(2026, 1, 15) },
      new Dictionary<string, object> { ["created_at"] = new DateTime(2026, 1, 20) },
      new Dictionary<string, object> { ["created_at"] = new DateTime(2026, 2, 10) },
      new Dictionary<string, object> { ["created_at"] = new DateTime(2026, 3, 5) },
      new Dictionary<string, object> { ["created_at"] = new DateTime(2026, 3, 25) },
    ];

    await manager.EnsurePartitionsAsync("bulk_events", batch);

    int count = await conn.QuerySingleAsync<int>(
      $"SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent JOIN pg_namespace n ON n.oid = p.relnamespace WHERE p.relname = 'bulk_events' AND n.nspname = @schema",
      new { schema = TestSchema });
    count.Should().Be(3);

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task EnsurePartitions_Bulk_ListToRange_CreatesFullTreeAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync(
      $@"
      CREATE TABLE {TestSchema}.bulk_situations (
        id BIGSERIAL,
        status TEXT NOT NULL,
        period_start TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (id, status, period_start)
      ) PARTITION BY LIST (status)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(ConnectionString)
      .UseAdvisoryLocks()
      .ForTable(
        "bulk_situations",
        t => t
          .ListBy("status")
          .WithPartition(
            "active",
            ["active"],
            child => child
              .RangeBy("period_start")
              .Monthly())
          .WithSchema(TestSchema)
      )
      .Build();

    List<IDictionary<string, object>> batch =
    [
      new Dictionary<string, object> { ["status"] = "active", ["period_start"] = new DateTime(2026, 4, 15) },
      new Dictionary<string, object> { ["status"] = "active", ["period_start"] = new DateTime(2026, 5, 10) },
      new Dictionary<string, object> { ["status"] = "active", ["period_start"] = new DateTime(2026, 4, 20) },
    ];

    await manager.EnsurePartitionsAsync("bulk_situations", batch);

    bool listExists = await conn.QuerySingleAsync<bool>(
      "SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'bulk_situations_active' AND n.nspname = @schema)",
      new { schema = TestSchema });
    bool aprilExists = await conn.QuerySingleAsync<bool>(
      "SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'bulk_situations_active_2026_04' AND n.nspname = @schema)",
      new { schema = TestSchema });
    bool mayExists = await conn.QuerySingleAsync<bool>(
      "SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'bulk_situations_active_2026_05' AND n.nspname = @schema)",
      new { schema = TestSchema });

    listExists.Should().BeTrue();
    aprilExists.Should().BeTrue();
    mayExists.Should().BeTrue();

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task EnsurePartitions_Bulk_CalledTwice_IdempotentAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync(
      $@"
      CREATE TABLE {TestSchema}.bulk_logs (
        id BIGSERIAL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (id, created_at)
      ) PARTITION BY RANGE (created_at)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(ConnectionString)
      .UseAdvisoryLocks()
      .ForTable(
        "bulk_logs",
        t => t
          .RangeBy("created_at")
          .Monthly()
          .WithSchema(TestSchema)
      )
      .Build();

    List<IDictionary<string, object>> batch =
    [
      new Dictionary<string, object> { ["created_at"] = new DateTime(2026, 6, 1) },
      new Dictionary<string, object> { ["created_at"] = new DateTime(2026, 7, 1) },
    ];

    await manager.EnsurePartitionsAsync("bulk_logs", batch);
    await manager.EnsurePartitionsAsync("bulk_logs", batch);

    int count = await conn.QuerySingleAsync<int>(
      $"SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent JOIN pg_namespace n ON n.oid = p.relnamespace WHERE p.relname = 'bulk_logs' AND n.nspname = @schema",
      new { schema = TestSchema });
    count.Should().Be(2);

    await manager.DisposeAsync();
  }
}
