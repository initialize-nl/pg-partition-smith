using Dapper;
using FluentAssertions;
using Npgsql;
using InitializeNL.PgPartitionSmith.Auto;
using InitializeNL.PgPartitionSmith.Auto.Configuration;

namespace InitializeNL.PgPartitionSmith.Tests.Integration;

[Collection("Postgres")]
[Trait("Category", "Integration")]
public class TreePartitionIntegrationTests : IAsyncLifetime
{
  private readonly PostgresFixture _postgres;
  private string _schema = null!;

  public TreePartitionIntegrationTests(PostgresFixture postgres)
  {
    _postgres = postgres;
  }

  public async Task InitializeAsync()
  {
    _schema = $"test_{Guid.NewGuid():N}"[..20];
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($"CREATE SCHEMA {_schema}");
  }

  public async Task DisposeAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($"DROP SCHEMA IF EXISTS {_schema} CASCADE");
  }

  [Fact]
  public async Task ListToRange_CreatesTreeAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.situations (
        id BIGSERIAL,
        status TEXT NOT NULL,
        period_start TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (id, status, period_start)
      ) PARTITION BY LIST (status)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("situations", t => t
        .ListBy("status")
        .WithPartition("active", ["active"], child => child
          .RangeBy("period_start")
          .Monthly())
        .WithPartition("closed", ["closed"], child => child
          .RangeBy("period_start")
          .Monthly())
        .WithSchema(_schema))
      .Build();

    await manager.EnsurePartitionAsync("situations",
      new Dictionary<string, object>
      {
        ["status"] = "active",
        ["period_start"] = new DateTime(2026, 4, 15)
      });
    await manager.EnsurePartitionAsync("situations",
      new Dictionary<string, object>
      {
        ["status"] = "closed",
        ["period_start"] = new DateTime(2026, 5, 10)
      });

    bool activeExists = await PartitionExistsAsync(conn, "situations_active");
    bool activeApril = await PartitionExistsAsync(conn, "situations_active_2026_04");
    bool closedExists = await PartitionExistsAsync(conn, "situations_closed");
    bool closedMay = await PartitionExistsAsync(conn, "situations_closed_2026_05");

    activeExists.Should().BeTrue();
    activeApril.Should().BeTrue();
    closedExists.Should().BeTrue();
    closedMay.Should().BeTrue();

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task RangeToList_CreatesTreeAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.events (
        id BIGSERIAL,
        created_at TIMESTAMPTZ NOT NULL,
        region TEXT NOT NULL,
        PRIMARY KEY (id, created_at, region)
      ) PARTITION BY RANGE (created_at)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("events", t => t
        .RangeBy("created_at")
        .Monthly()
        .SubPartition(sub => sub
          .ListBy("region")
          .WithPartition("eu", ["eu"])
          .WithPartition("us", ["us"]))
        .WithSchema(_schema))
      .Build();

    await manager.EnsurePartitionAsync("events",
      new Dictionary<string, object>
      {
        ["created_at"] = new DateTime(2026, 6, 1),
        ["region"] = "eu"
      });

    bool monthExists = await PartitionExistsAsync(conn, "events_2026_06");
    bool euExists = await PartitionExistsAsync(conn, "events_2026_06_eu");

    monthExists.Should().BeTrue();
    euExists.Should().BeTrue();

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task TreePartition_DataCanBeInsertedAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.orders (
        id BIGSERIAL,
        status TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        amount NUMERIC,
        PRIMARY KEY (id, status, created_at)
      ) PARTITION BY LIST (status)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("orders", t => t
        .ListBy("status")
        .WithPartition("pending", ["pending"], child => child
          .RangeBy("created_at")
          .Monthly())
        .WithPartition("shipped", ["shipped"], child => child
          .RangeBy("created_at")
          .Monthly())
        .WithSchema(_schema))
      .Build();

    await manager.EnsurePartitionAsync("orders",
      new Dictionary<string, object>
      {
        ["status"] = "pending",
        ["created_at"] = new DateTime(2026, 4, 15)
      });
    await manager.EnsurePartitionAsync("orders",
      new Dictionary<string, object>
      {
        ["status"] = "shipped",
        ["created_at"] = new DateTime(2026, 4, 20)
      });

    await conn.ExecuteAsync(
      $"INSERT INTO {_schema}.orders (status, created_at, amount) VALUES (@status, @created_at, @amount)",
      new { status = "pending", created_at = new DateTime(2026, 4, 15), amount = 99.99m });
    await conn.ExecuteAsync(
      $"INSERT INTO {_schema}.orders (status, created_at, amount) VALUES (@status, @created_at, @amount)",
      new { status = "shipped", created_at = new DateTime(2026, 4, 20), amount = 49.50m });

    int parentCount = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.orders");
    int pendingCount = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.orders_pending_2026_04");
    int shippedCount = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.orders_shipped_2026_04");

    parentCount.Should().Be(2);
    pendingCount.Should().Be(1);
    shippedCount.Should().Be(1);

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task BulkEnsure_TreePartition_CreatesAllLevelsAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.metrics (
        id BIGSERIAL,
        category TEXT NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL,
        value NUMERIC,
        PRIMARY KEY (id, category, recorded_at)
      ) PARTITION BY LIST (category)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("metrics", t => t
        .ListBy("category")
        .WithPartition("cpu", ["cpu"], child => child
          .RangeBy("recorded_at")
          .Monthly())
        .WithPartition("memory", ["memory"], child => child
          .RangeBy("recorded_at")
          .Monthly())
        .WithSchema(_schema))
      .Build();

    List<IDictionary<string, object>> batch =
    [
      new Dictionary<string, object> { ["category"] = "cpu", ["recorded_at"] = new DateTime(2026, 1, 10) },
      new Dictionary<string, object> { ["category"] = "cpu", ["recorded_at"] = new DateTime(2026, 2, 15) },
      new Dictionary<string, object> { ["category"] = "memory", ["recorded_at"] = new DateTime(2026, 1, 20) },
      new Dictionary<string, object> { ["category"] = "memory", ["recorded_at"] = new DateTime(2026, 3, 5) },
    ];

    await manager.EnsurePartitionsAsync("metrics", batch);

    bool cpuExists = await PartitionExistsAsync(conn, "metrics_cpu");
    bool cpuJan = await PartitionExistsAsync(conn, "metrics_cpu_2026_01");
    bool cpuFeb = await PartitionExistsAsync(conn, "metrics_cpu_2026_02");
    bool memExists = await PartitionExistsAsync(conn, "metrics_memory");
    bool memJan = await PartitionExistsAsync(conn, "metrics_memory_2026_01");
    bool memMar = await PartitionExistsAsync(conn, "metrics_memory_2026_03");

    cpuExists.Should().BeTrue();
    cpuJan.Should().BeTrue();
    cpuFeb.Should().BeTrue();
    memExists.Should().BeTrue();
    memJan.Should().BeTrue();
    memMar.Should().BeTrue();

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task TreePartition_WithRemainderOnLeaf_CatchesUnmatchedDataAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.logs (
        id BIGSERIAL,
        level TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        message TEXT,
        PRIMARY KEY (id, level, created_at)
      ) PARTITION BY LIST (level)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("logs", t => t
        .ListBy("level")
        .WithPartition("error", ["error"], child => child
          .RangeBy("created_at")
          .Monthly())
        .WithPartition("info", ["info"], child => child
          .RangeBy("created_at")
          .Monthly())
        .WithRemainderPartition()
        .WithSchema(_schema))
      .Build();

    await manager.InitializeAsync();

    await manager.EnsurePartitionAsync("logs",
      new Dictionary<string, object>
      {
        ["level"] = "error",
        ["created_at"] = new DateTime(2026, 4, 1)
      });

    bool errorExists = await PartitionExistsAsync(conn, "logs_error");
    bool errorApril = await PartitionExistsAsync(conn, "logs_error_2026_04");
    bool defaultExists = await PartitionExistsAsync(conn, "logs_remainder");

    errorExists.Should().BeTrue();
    errorApril.Should().BeTrue();
    defaultExists.Should().BeTrue();

    await conn.ExecuteAsync(
      $"INSERT INTO {_schema}.logs (level, created_at, message) VALUES (@level, @created_at, @message)",
      new { level = "debug", created_at = new DateTime(2026, 4, 1), message = "caught by remainder" });

    int remainderCount = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.logs_remainder");
    remainderCount.Should().Be(1);

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task TreePartition_IdempotentCreationAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.idempotent (
        id BIGSERIAL,
        status TEXT NOT NULL,
        ts TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (id, status, ts)
      ) PARTITION BY LIST (status)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("idempotent", t => t
        .ListBy("status")
        .WithPartition("a", ["a"], child => child
          .RangeBy("ts")
          .Monthly())
        .WithSchema(_schema))
      .Build();

    Dictionary<string, object> values = new Dictionary<string, object>
    {
      ["status"] = "a",
      ["ts"] = new DateTime(2026, 7, 1)
    };

    await manager.EnsurePartitionAsync("idempotent", values);
    await manager.EnsurePartitionAsync("idempotent", values);
    await manager.EnsurePartitionAsync("idempotent", values);

    int listCount = await conn.QuerySingleAsync<int>(
      "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
      "WHERE c.relname = 'idempotent_a' AND n.nspname = @schema",
      new { schema = _schema });
    int rangeCount = await conn.QuerySingleAsync<int>(
      "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
      "WHERE c.relname = 'idempotent_a_2026_07' AND n.nspname = @schema",
      new { schema = _schema });

    listCount.Should().Be(1);
    rangeCount.Should().Be(1);

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task RandomData_ListToMonthlyRange_AllRowsRoutedCorrectlyAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.sales (
        id BIGSERIAL,
        channel TEXT NOT NULL,
        sold_at TIMESTAMPTZ NOT NULL,
        amount NUMERIC,
        PRIMARY KEY (id, channel, sold_at)
      ) PARTITION BY LIST (channel)");

    string[] channels = ["online", "retail", "wholesale"];

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("sales", t =>
      {
        t.ListBy("channel").WithSchema(_schema);
        foreach (string ch in channels)
        {
          t.WithPartition(ch, [ch], child => child.RangeBy("sold_at").Monthly());
        }
      })
      .Build();

    Random rng = new Random(42);
    int rowCount = 200;
    List<(string Channel, DateTime SoldAt, decimal Amount)> rows = [];

    for (int i = 0; i < rowCount; i++)
    {
      string channel = channels[rng.Next(channels.Length)];
      DateTime soldAt = new DateTime(2024, 1, 1).AddDays(rng.Next(365 * 3));
      decimal amount = Math.Round((decimal)(rng.NextDouble() * 1000), 2);
      rows.Add((channel, soldAt, amount));
    }

    List<IDictionary<string, object>> batch = rows
      .Select(r => (IDictionary<string, object>)new Dictionary<string, object>
      {
        ["channel"] = r.Channel,
        ["sold_at"] = r.SoldAt
      })
      .ToList();
    await manager.EnsurePartitionsAsync("sales", batch);

    foreach ((string channel, DateTime soldAt, decimal amount) in rows)
    {
      await conn.ExecuteAsync(
        $"INSERT INTO {_schema}.sales (channel, sold_at, amount) VALUES (@channel, @sold_at, @amount)",
        new { channel, sold_at = soldAt, amount });
    }

    int totalCount = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.sales");
    totalCount.Should().Be(rowCount);

    foreach (string ch in channels)
    {
      int expected = rows.Count(r => r.Channel == ch);
      int actual = await conn.QuerySingleAsync<int>(
        $"SELECT count(*) FROM {_schema}.sales_{ch}",
        new { schema = _schema });
      actual.Should().Be(expected, $"channel '{ch}' should have {expected} rows");
    }

    HashSet<string> expectedPartitions = rows
      .Select(r => $"sales_{r.Channel}_{r.SoldAt:yyyy_MM}")
      .ToHashSet();
    int partitionCount = await conn.QuerySingleAsync<int>(
      "SELECT count(*) FROM pg_inherits i " +
      "JOIN pg_class c ON c.oid = i.inhrelid " +
      "JOIN pg_namespace n ON n.oid = c.relnamespace " +
      "WHERE n.nspname = @schema AND c.relkind IN ('r', 'p') " +
      "AND c.relname != 'sales_online' AND c.relname != 'sales_retail' AND c.relname != 'sales_wholesale'",
      new { schema = _schema });
    partitionCount.Should().Be(expectedPartitions.Count);

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task RandomData_WeeklyRange_CreatesCorrectPartitionsAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.weekly_logs (
        id BIGSERIAL,
        created_at TIMESTAMPTZ NOT NULL,
        payload TEXT,
        PRIMARY KEY (id, created_at)
      ) PARTITION BY RANGE (created_at)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("weekly_logs", t => t
        .RangeBy("created_at")
        .Weekly()
        .WithSchema(_schema))
      .Build();

    Random rng = new Random(123);
    int rowCount = 150;
    List<DateTime> dates = [];
    for (int i = 0; i < rowCount; i++)
    {
      dates.Add(new DateTime(2026, 1, 1).AddDays(rng.Next(90)));
    }

    List<IDictionary<string, object>> batch = dates
      .Select(d => (IDictionary<string, object>)new Dictionary<string, object> { ["created_at"] = d })
      .ToList();
    await manager.EnsurePartitionsAsync("weekly_logs", batch);

    foreach (DateTime date in dates)
    {
      await conn.ExecuteAsync(
        $"INSERT INTO {_schema}.weekly_logs (created_at, payload) VALUES (@created_at, @payload)",
        new { created_at = date, payload = $"log-{date:yyyyMMdd}" });
    }

    int totalCount = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.weekly_logs");
    totalCount.Should().Be(rowCount);

    HashSet<string> expectedWeeks = dates
      .Select(d =>
      {
        int week = System.Globalization.ISOWeek.GetWeekOfYear(d);
        int isoYear = System.Globalization.ISOWeek.GetYear(d);
        return $"weekly_logs_{isoYear}_w{week:D2}";
      })
      .ToHashSet();

    foreach (string partitionName in expectedWeeks)
    {
      bool exists = await PartitionExistsAsync(conn, partitionName);
      exists.Should().BeTrue($"partition '{partitionName}' should exist");
    }

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task RandomData_DailyRange_CreatesCorrectPartitionsAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.daily_events (
        id BIGSERIAL,
        event_time TIMESTAMPTZ NOT NULL,
        data TEXT,
        PRIMARY KEY (id, event_time)
      ) PARTITION BY RANGE (event_time)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("daily_events", t => t
        .RangeBy("event_time")
        .Daily()
        .WithSchema(_schema))
      .Build();

    Random rng = new Random(77);
    int rowCount = 100;
    List<DateTime> dates = [];
    for (int i = 0; i < rowCount; i++)
    {
      DateTime day = new DateTime(2026, 3, 1).AddDays(rng.Next(31));
      dates.Add(day.AddHours(rng.Next(24)).AddMinutes(rng.Next(60)));
    }

    List<IDictionary<string, object>> batch = dates
      .Select(d => (IDictionary<string, object>)new Dictionary<string, object> { ["event_time"] = d })
      .ToList();
    await manager.EnsurePartitionsAsync("daily_events", batch);

    foreach (DateTime date in dates)
    {
      await conn.ExecuteAsync(
        $"INSERT INTO {_schema}.daily_events (event_time, data) VALUES (@event_time, @data)",
        new { event_time = date, data = $"evt-{date:HHmmss}" });
    }

    int totalCount = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.daily_events");
    totalCount.Should().Be(rowCount);

    HashSet<string> expectedDays = dates
      .Select(d => $"daily_events_{d:yyyy_MM_dd}")
      .ToHashSet();

    int partitionCount = await conn.QuerySingleAsync<int>(
      "SELECT count(*) FROM pg_inherits i " +
      "JOIN pg_class c ON c.oid = i.inhrelid " +
      "JOIN pg_namespace n ON n.oid = c.relnamespace " +
      "WHERE n.nspname = @schema AND c.relkind IN ('r', 'p')",
      new { schema = _schema });
    partitionCount.Should().Be(expectedDays.Count);

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task RandomData_QuarterlyRange_CreatesCorrectPartitionsAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.quarterly_data (
        id BIGSERIAL,
        recorded_at TIMESTAMPTZ NOT NULL,
        value NUMERIC,
        PRIMARY KEY (id, recorded_at)
      ) PARTITION BY RANGE (recorded_at)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("quarterly_data", t => t
        .RangeBy("recorded_at")
        .Quarterly()
        .WithSchema(_schema))
      .Build();

    Random rng = new Random(999);
    int rowCount = 80;
    List<DateTime> dates = [];
    for (int i = 0; i < rowCount; i++)
    {
      dates.Add(new DateTime(2024, 1, 1).AddDays(rng.Next(365 * 3)));
    }

    List<IDictionary<string, object>> batch = dates
      .Select(d => (IDictionary<string, object>)new Dictionary<string, object> { ["recorded_at"] = d })
      .ToList();
    await manager.EnsurePartitionsAsync("quarterly_data", batch);

    foreach (DateTime date in dates)
    {
      await conn.ExecuteAsync(
        $"INSERT INTO {_schema}.quarterly_data (recorded_at, value) VALUES (@recorded_at, @value)",
        new { recorded_at = date, value = Math.Round((decimal)(rng.NextDouble() * 500), 2) });
    }

    int totalCount = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.quarterly_data");
    totalCount.Should().Be(rowCount);

    HashSet<string> expectedQuarters = dates
      .Select(d =>
      {
        int quarter = (d.Month - 1) / 3 + 1;
        return $"quarterly_data_{d.Year}_q{quarter}";
      })
      .ToHashSet();

    foreach (string partitionName in expectedQuarters)
    {
      bool exists = await PartitionExistsAsync(conn, partitionName);
      exists.Should().BeTrue($"partition '{partitionName}' should exist");
    }

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task RandomData_YearlyRange_CreatesCorrectPartitionsAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.yearly_archive (
        id BIGSERIAL,
        archived_at TIMESTAMPTZ NOT NULL,
        label TEXT,
        PRIMARY KEY (id, archived_at)
      ) PARTITION BY RANGE (archived_at)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("yearly_archive", t => t
        .RangeBy("archived_at")
        .Yearly()
        .WithSchema(_schema))
      .Build();

    Random rng = new Random(555);
    int rowCount = 60;
    List<DateTime> dates = [];
    for (int i = 0; i < rowCount; i++)
    {
      dates.Add(new DateTime(2020, 1, 1).AddDays(rng.Next(365 * 7)));
    }

    List<IDictionary<string, object>> batch = dates
      .Select(d => (IDictionary<string, object>)new Dictionary<string, object> { ["archived_at"] = d })
      .ToList();
    await manager.EnsurePartitionsAsync("yearly_archive", batch);

    foreach (DateTime date in dates)
    {
      await conn.ExecuteAsync(
        $"INSERT INTO {_schema}.yearly_archive (archived_at, label) VALUES (@archived_at, @label)",
        new { archived_at = date, label = $"item-{date.Year}" });
    }

    int totalCount = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.yearly_archive");
    totalCount.Should().Be(rowCount);

    HashSet<string> expectedYears = dates.Select(d => $"yearly_archive_{d.Year}").ToHashSet();

    int partitionCount = await conn.QuerySingleAsync<int>(
      "SELECT count(*) FROM pg_inherits i " +
      "JOIN pg_class c ON c.oid = i.inhrelid " +
      "JOIN pg_namespace n ON n.oid = c.relnamespace " +
      "WHERE n.nspname = @schema AND c.relkind IN ('r', 'p')",
      new { schema = _schema });
    partitionCount.Should().Be(expectedYears.Count);

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task RandomData_ListToWeeklyRange_TreeWithBulkInsertsAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.tickets (
        id BIGSERIAL,
        priority TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        description TEXT,
        PRIMARY KEY (id, priority, created_at)
      ) PARTITION BY LIST (priority)");

    string[] priorities = ["critical", "high", "medium", "low"];

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("tickets", t =>
      {
        t.ListBy("priority").WithSchema(_schema);
        foreach (string p in priorities)
        {
          t.WithPartition(p, [p], child => child.RangeBy("created_at").Weekly());
        }
      })
      .Build();

    Random rng = new Random(314);
    int rowCount = 300;
    List<(string Priority, DateTime CreatedAt)> rows = [];
    for (int i = 0; i < rowCount; i++)
    {
      string priority = priorities[rng.Next(priorities.Length)];
      DateTime createdAt = new DateTime(2026, 1, 1).AddDays(rng.Next(180));
      rows.Add((priority, createdAt));
    }

    List<IDictionary<string, object>> batch = rows
      .Select(r => (IDictionary<string, object>)new Dictionary<string, object>
      {
        ["priority"] = r.Priority,
        ["created_at"] = r.CreatedAt
      })
      .ToList();
    await manager.EnsurePartitionsAsync("tickets", batch);

    foreach ((string priority, DateTime createdAt) in rows)
    {
      await conn.ExecuteAsync(
        $"INSERT INTO {_schema}.tickets (priority, created_at, description) VALUES (@priority, @created_at, @description)",
        new { priority, created_at = createdAt, description = $"ticket-{priority}" });
    }

    int totalCount = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.tickets");
    totalCount.Should().Be(rowCount);

    foreach (string p in priorities)
    {
      int expected = rows.Count(r => r.Priority == p);
      int actual = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.tickets_{p}");
      actual.Should().Be(expected, $"priority '{p}' should have {expected} rows");
    }

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task RandomData_RangeToList_YearlyWithRegionsAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.invoices (
        id BIGSERIAL,
        issued_at TIMESTAMPTZ NOT NULL,
        region TEXT NOT NULL,
        total NUMERIC,
        PRIMARY KEY (id, issued_at, region)
      ) PARTITION BY RANGE (issued_at)");

    string[] regions = ["emea", "apac", "americas"];

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("invoices", t => t
        .RangeBy("issued_at")
        .Yearly()
        .SubPartition(sub =>
        {
          sub.ListBy("region");
          foreach (string r in regions)
          {
            sub.WithPartition(r, [r]);
          }
        })
        .WithSchema(_schema))
      .Build();

    Random rng = new Random(2026);
    int rowCount = 120;
    List<(DateTime IssuedAt, string Region, decimal Total)> rows = [];
    for (int i = 0; i < rowCount; i++)
    {
      DateTime issuedAt = new DateTime(2023, 1, 1).AddDays(rng.Next(365 * 4));
      string region = regions[rng.Next(regions.Length)];
      decimal total = Math.Round((decimal)(rng.NextDouble() * 10000), 2);
      rows.Add((issuedAt, region, total));
    }

    List<IDictionary<string, object>> batch = rows
      .Select(r => (IDictionary<string, object>)new Dictionary<string, object>
      {
        ["issued_at"] = r.IssuedAt,
        ["region"] = r.Region
      })
      .ToList();
    await manager.EnsurePartitionsAsync("invoices", batch);

    foreach ((DateTime issuedAt, string region, decimal total) in rows)
    {
      await conn.ExecuteAsync(
        $"INSERT INTO {_schema}.invoices (issued_at, region, total) VALUES (@issued_at, @region, @total)",
        new { issued_at = issuedAt, region, total });
    }

    int totalCount = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.invoices");
    totalCount.Should().Be(rowCount);

    HashSet<int> years = rows.Select(r => r.IssuedAt.Year).ToHashSet();
    foreach (int year in years)
    {
      foreach (string region in regions)
      {
        int expected = rows.Count(r => r.IssuedAt.Year == year && r.Region == region);
        if (expected > 0)
        {
          string leafPartition = $"invoices_{year}_{region}";
          bool exists = await PartitionExistsAsync(conn, leafPartition);
          exists.Should().BeTrue($"partition '{leafPartition}' should exist");

          int actual = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.{leafPartition}");
          actual.Should().Be(expected, $"partition '{leafPartition}' should have {expected} rows");
        }
      }
    }

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task ThreeLevelTree_ListToRangeToList_CreatesFullHierarchyAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.shipments (
        id BIGSERIAL,
        status TEXT NOT NULL,
        shipped_at TIMESTAMPTZ NOT NULL,
        warehouse TEXT NOT NULL,
        weight NUMERIC,
        PRIMARY KEY (id, status, shipped_at, warehouse)
      ) PARTITION BY LIST (status)");

    string[] statuses = ["pending", "dispatched"];
    string[] warehouses = ["ams", "fra", "lon"];

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("shipments", t =>
      {
        t.ListBy("status").WithSchema(_schema);
        foreach (string s in statuses)
        {
          t.WithPartition(s, [s], child => child
            .RangeBy("shipped_at")
            .Monthly()
            .SubPartition(sub =>
            {
              sub.ListBy("warehouse");
              foreach (string w in warehouses)
              {
                sub.WithPartition(w, [w]);
              }
            }));
        }
      })
      .Build();

    Random rng = new Random(7777);
    int rowCount = 250;
    List<(string Status, DateTime ShippedAt, string Warehouse, decimal Weight)> rows = [];
    for (int i = 0; i < rowCount; i++)
    {
      string status = statuses[rng.Next(statuses.Length)];
      DateTime shippedAt = new DateTime(2025, 6, 1).AddDays(rng.Next(365));
      string warehouse = warehouses[rng.Next(warehouses.Length)];
      decimal weight = Math.Round((decimal)(rng.NextDouble() * 200), 2);
      rows.Add((status, shippedAt, warehouse, weight));
    }

    List<IDictionary<string, object>> batch = rows
      .Select(r => (IDictionary<string, object>)new Dictionary<string, object>
      {
        ["status"] = r.Status,
        ["shipped_at"] = r.ShippedAt,
        ["warehouse"] = r.Warehouse
      })
      .ToList();
    await manager.EnsurePartitionsAsync("shipments", batch);

    foreach ((string status, DateTime shippedAt, string warehouse, decimal weight) in rows)
    {
      await conn.ExecuteAsync(
        $"INSERT INTO {_schema}.shipments (status, shipped_at, warehouse, weight) " +
        "VALUES (@status, @shipped_at, @warehouse, @weight)",
        new { status, shipped_at = shippedAt, warehouse, weight });
    }

    int totalCount = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.shipments");
    totalCount.Should().Be(rowCount);

    foreach (string status in statuses)
    {
      string l1 = $"shipments_{status}";
      bool l1Exists = await PartitionExistsAsync(conn, l1);
      l1Exists.Should().BeTrue($"L1 partition '{l1}' should exist");

      int statusCount = rows.Count(r => r.Status == status);
      int actualStatusCount = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.{l1}");
      actualStatusCount.Should().Be(statusCount);
    }

    HashSet<(string Status, string Month)> expectedL2 = rows
      .Select(r => (r.Status, Month: $"{r.ShippedAt:yyyy_MM}"))
      .ToHashSet();
    foreach ((string status, string month) in expectedL2)
    {
      string l2 = $"shipments_{status}_{month}";
      bool l2Exists = await PartitionExistsAsync(conn, l2);
      l2Exists.Should().BeTrue($"L2 partition '{l2}' should exist");
    }

    HashSet<(string Status, string Month, string Warehouse)> expectedL3 = rows
      .Select(r => (r.Status, Month: $"{r.ShippedAt:yyyy_MM}", r.Warehouse))
      .ToHashSet();
    foreach ((string status, string month, string warehouse) in expectedL3)
    {
      string l3 = $"shipments_{status}_{month}_{warehouse}";
      bool l3Exists = await PartitionExistsAsync(conn, l3);
      l3Exists.Should().BeTrue($"L3 leaf partition '{l3}' should exist");

      int expected = rows.Count(r =>
        r.Status == status &&
        $"{r.ShippedAt:yyyy_MM}" == month &&
        r.Warehouse == warehouse);
      int actual = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.{l3}");
      actual.Should().Be(expected, $"leaf '{l3}' should have {expected} rows");
    }

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task ThreeLevelTree_RangeToListToRange_CreatesFullHierarchyAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.sensor_data (
        id BIGSERIAL,
        recorded_at TIMESTAMPTZ NOT NULL,
        sensor_type TEXT NOT NULL,
        sample_time TIMESTAMPTZ NOT NULL,
        reading NUMERIC,
        PRIMARY KEY (id, recorded_at, sensor_type, sample_time)
      ) PARTITION BY RANGE (recorded_at)");

    string[] sensorTypes = ["temperature", "humidity", "pressure"];

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .ForTable("sensor_data", t => t
        .RangeBy("recorded_at")
        .Quarterly()
        .SubPartition(sub =>
        {
          sub.ListBy("sensor_type");
          foreach (string st in sensorTypes)
          {
            sub.WithPartition(st, [st], leaf => leaf
              .RangeBy("sample_time")
              .Daily());
          }
        })
        .WithSchema(_schema))
      .Build();

    Random rng = new Random(4242);
    int rowCount = 150;
    List<(DateTime RecordedAt, string SensorType, DateTime SampleTime, decimal Reading)> rows = [];
    for (int i = 0; i < rowCount; i++)
    {
      DateTime recordedAt = new DateTime(2026, 1, 1).AddDays(rng.Next(180));
      string sensorType = sensorTypes[rng.Next(sensorTypes.Length)];
      DateTime sampleTime = recordedAt.Date.AddHours(rng.Next(24)).AddMinutes(rng.Next(60));
      decimal reading = Math.Round((decimal)(rng.NextDouble() * 100), 2);
      rows.Add((recordedAt, sensorType, sampleTime, reading));
    }

    List<IDictionary<string, object>> batch = rows
      .Select(r => (IDictionary<string, object>)new Dictionary<string, object>
      {
        ["recorded_at"] = r.RecordedAt,
        ["sensor_type"] = r.SensorType,
        ["sample_time"] = r.SampleTime
      })
      .ToList();
    await manager.EnsurePartitionsAsync("sensor_data", batch);

    foreach ((DateTime recordedAt, string sensorType, DateTime sampleTime, decimal reading) in rows)
    {
      await conn.ExecuteAsync(
        $"INSERT INTO {_schema}.sensor_data (recorded_at, sensor_type, sample_time, reading) " +
        "VALUES (@recorded_at, @sensor_type, @sample_time, @reading)",
        new { recorded_at = recordedAt, sensor_type = sensorType, sample_time = sampleTime, reading });
    }

    int totalCount = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.sensor_data");
    totalCount.Should().Be(rowCount);

    HashSet<string> expectedL1 = rows
      .Select(r =>
      {
        int quarter = (r.RecordedAt.Month - 1) / 3 + 1;
        return $"sensor_data_{r.RecordedAt.Year}_q{quarter}";
      })
      .ToHashSet();
    foreach (string l1 in expectedL1)
    {
      bool exists = await PartitionExistsAsync(conn, l1);
      exists.Should().BeTrue($"L1 partition '{l1}' should exist");
    }

    HashSet<(string Quarter, string SensorType)> expectedL2 = rows
      .Select(r =>
      {
        int quarter = (r.RecordedAt.Month - 1) / 3 + 1;
        return (Quarter: $"sensor_data_{r.RecordedAt.Year}_q{quarter}", r.SensorType);
      })
      .ToHashSet();
    foreach ((string quarter, string sensorType) in expectedL2)
    {
      string l2 = $"{quarter}_{sensorType}";
      bool exists = await PartitionExistsAsync(conn, l2);
      exists.Should().BeTrue($"L2 partition '{l2}' should exist");
    }

    HashSet<(string Quarter, string SensorType, string Day)> expectedL3 = rows
      .Select(r =>
      {
        int quarter = (r.RecordedAt.Month - 1) / 3 + 1;
        return (
          Quarter: $"sensor_data_{r.RecordedAt.Year}_q{quarter}",
          r.SensorType,
          Day: $"{r.SampleTime:yyyy_MM_dd}");
      })
      .ToHashSet();
    foreach ((string quarter, string sensorType, string day) in expectedL3)
    {
      string l3 = $"{quarter}_{sensorType}_{day}";
      bool exists = await PartitionExistsAsync(conn, l3);
      exists.Should().BeTrue($"L3 leaf partition '{l3}' should exist");
    }

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task HashedNames_ListToRange_CreatesPartitionsWithCommentsAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.hashed_orders (
        id BIGSERIAL,
        status TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (id, status, created_at)
      ) PARTITION BY LIST (status)");

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .UseHashedNames()
      .ForTable("hashed_orders", t => t
        .ListBy("status")
        .WithPartition("active", ["active"], child => child
          .RangeBy("created_at")
          .Monthly())
        .WithSchema(_schema))
      .Build();

    await manager.EnsurePartitionAsync("hashed_orders",
      new Dictionary<string, object>
      {
        ["status"] = "active",
        ["created_at"] = new DateTime(2026, 4, 15)
      });

    bool listExists = await PartitionExistsAsync(conn, "hashed_orders_active");
    bool rangeExists = await PartitionExistsAsync(conn, "hashed_orders_active_2026_04");
    listExists.Should().BeTrue();
    rangeExists.Should().BeTrue();

    List<string> comments = (await conn.QueryAsync<string>(
      "SELECT obj_description(c.oid) FROM pg_class c " +
      "JOIN pg_namespace n ON n.oid = c.relnamespace " +
      "WHERE n.nspname = @schema " +
      "AND obj_description(c.oid) IS NOT NULL " +
      "AND obj_description(c.oid) LIKE 'PartitionSmith:%'",
      new { schema = _schema })).ToList();

    comments.Should().NotBeEmpty("hashed names should produce PartitionSmith comments");
    comments.Should().AllSatisfy(c => c.Should().StartWith("PartitionSmith:"));

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task HashedNames_ThreeLevel_RandomData_AllRowsRoutedAsync()
  {
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.hashed_shipments (
        id BIGSERIAL,
        channel TEXT NOT NULL,
        shipped_at TIMESTAMPTZ NOT NULL,
        depot TEXT NOT NULL,
        value NUMERIC,
        PRIMARY KEY (id, channel, shipped_at, depot)
      ) PARTITION BY LIST (channel)");

    string[] channels = ["web", "store"];
    string[] depots = ["north", "south"];

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .UseHashedNames()
      .ForTable("hashed_shipments", t =>
      {
        t.ListBy("channel").WithSchema(_schema);
        foreach (string ch in channels)
        {
          t.WithPartition(ch, [ch], child => child
            .RangeBy("shipped_at")
            .Quarterly()
            .SubPartition(sub =>
            {
              sub.ListBy("depot");
              foreach (string d in depots)
              {
                sub.WithPartition(d, [d]);
              }
            }));
        }
      })
      .Build();

    Random rng = new Random(1234);
    int rowCount = 100;
    List<(string Channel, DateTime ShippedAt, string Depot, decimal Value)> rows = [];
    for (int i = 0; i < rowCount; i++)
    {
      string channel = channels[rng.Next(channels.Length)];
      DateTime shippedAt = new DateTime(2025, 1, 1).AddDays(rng.Next(365));
      string depot = depots[rng.Next(depots.Length)];
      decimal value = Math.Round((decimal)(rng.NextDouble() * 500), 2);
      rows.Add((channel, shippedAt, depot, value));
    }

    List<IDictionary<string, object>> batch = rows
      .Select(r => (IDictionary<string, object>)new Dictionary<string, object>
      {
        ["channel"] = r.Channel,
        ["shipped_at"] = r.ShippedAt,
        ["depot"] = r.Depot
      })
      .ToList();
    await manager.EnsurePartitionsAsync("hashed_shipments", batch);

    foreach ((string channel, DateTime shippedAt, string depot, decimal value) in rows)
    {
      await conn.ExecuteAsync(
        $"INSERT INTO {_schema}.hashed_shipments (channel, shipped_at, depot, value) " +
        "VALUES (@channel, @shipped_at, @depot, @value)",
        new { channel, shipped_at = shippedAt, depot, value });
    }

    int totalCount = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.hashed_shipments");
    totalCount.Should().Be(rowCount);

    foreach (string ch in channels)
    {
      int expected = rows.Count(r => r.Channel == ch);
      int actual = await conn.QuerySingleAsync<int>(
        $"SELECT count(*) FROM {_schema}.hashed_shipments_{ch}");
      actual.Should().Be(expected);
    }

    int commentCount = await conn.QuerySingleAsync<int>(
      "SELECT count(*) FROM pg_class c " +
      "JOIN pg_namespace n ON n.oid = c.relnamespace " +
      "WHERE n.nspname = @schema " +
      "AND obj_description(c.oid) LIKE 'PartitionSmith:%'",
      new { schema = _schema });
    commentCount.Should().BeGreaterThan(0, "hashed names should produce PartitionSmith metadata comments");

    await manager.DisposeAsync();
  }

  [Fact]
  public async Task HashedNames_FourLevel_RandomData_AllRowsRoutedAsync()
  {
    // L1: List(region) → L2: Range(quarterly) → L3: List(category) → L4: Range(daily)
    await using NpgsqlConnection conn = new NpgsqlConnection(_postgres.ConnectionString);
    await conn.OpenAsync();
    await conn.ExecuteAsync($@"
      CREATE TABLE {_schema}.transactions (
        id BIGSERIAL,
        region TEXT NOT NULL,
        tx_date TIMESTAMPTZ NOT NULL,
        category TEXT NOT NULL,
        entry_time TIMESTAMPTZ NOT NULL,
        amount NUMERIC,
        PRIMARY KEY (id, region, tx_date, category, entry_time)
      ) PARTITION BY LIST (region)");

    string[] regions = ["eu", "us"];
    string[] categories = ["debit", "credit"];

    IPartitionManager manager = new PartitionManagerBuilder()
      .UseConnectionString(_postgres.ConnectionString)
      .UseAdvisoryLocks()
      .UseHashedNames()
      .ForTable("transactions", t =>
      {
        t.ListBy("region").WithSchema(_schema);
        foreach (string r in regions)
        {
          t.WithPartition(r, [r], l2 => l2
            .RangeBy("tx_date")
            .Quarterly()
            .SubPartition(l3 =>
            {
              l3.ListBy("category");
              foreach (string c in categories)
              {
                l3.WithPartition(c, [c], l4 => l4
                  .RangeBy("entry_time")
                  .Daily());
              }
            }));
        }
      })
      .Build();

    Random rng = new Random(9999);
    int rowCount = 200;
    List<(string Region, DateTime TxDate, string Category, DateTime EntryTime, decimal Amount)> rows = [];
    for (int i = 0; i < rowCount; i++)
    {
      string region = regions[rng.Next(regions.Length)];
      DateTime txDate = new DateTime(2026, 1, 1).AddDays(rng.Next(180));
      string category = categories[rng.Next(categories.Length)];
      DateTime entryTime = txDate.Date.AddHours(rng.Next(24)).AddMinutes(rng.Next(60));
      decimal amount = Math.Round((decimal)(rng.NextDouble() * 5000), 2);
      rows.Add((region, txDate, category, entryTime, amount));
    }

    List<IDictionary<string, object>> batch = rows
      .Select(r => (IDictionary<string, object>)new Dictionary<string, object>
      {
        ["region"] = r.Region,
        ["tx_date"] = r.TxDate,
        ["category"] = r.Category,
        ["entry_time"] = r.EntryTime
      })
      .ToList();
    await manager.EnsurePartitionsAsync("transactions", batch);

    foreach ((string region, DateTime txDate, string category, DateTime entryTime, decimal amount) in rows)
    {
      await conn.ExecuteAsync(
        $"INSERT INTO {_schema}.transactions (region, tx_date, category, entry_time, amount) " +
        "VALUES (@region, @tx_date, @category, @entry_time, @amount)",
        new { region, tx_date = txDate, category, entry_time = entryTime, amount });
    }

    // Total via parent
    int totalCount = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.transactions");
    totalCount.Should().Be(rowCount);

    // L1: region partitions exist and row counts match
    foreach (string region in regions)
    {
      string l1 = $"transactions_{region}";
      bool l1Exists = await PartitionExistsAsync(conn, l1);
      l1Exists.Should().BeTrue($"L1 '{l1}' should exist");

      int expected = rows.Count(r => r.Region == region);
      int actual = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.{l1}");
      actual.Should().Be(expected, $"L1 '{l1}' row count");
    }

    // L2: region_quarter partitions exist
    HashSet<(string Region, string Quarter)> expectedL2 = rows
      .Select(r =>
      {
        int q = (r.TxDate.Month - 1) / 3 + 1;
        return (r.Region, Quarter: $"{r.TxDate.Year}_q{q}");
      })
      .ToHashSet();
    foreach ((string region, string quarter) in expectedL2)
    {
      string l2 = $"transactions_{region}_{quarter}";
      bool exists = await PartitionExistsAsync(conn, l2);
      exists.Should().BeTrue($"L2 '{l2}' should exist");
    }

    // L3: region_quarter_category partitions exist
    HashSet<(string Region, string Quarter, string Category)> expectedL3 = rows
      .Select(r =>
      {
        int q = (r.TxDate.Month - 1) / 3 + 1;
        return (r.Region, Quarter: $"{r.TxDate.Year}_q{q}", r.Category);
      })
      .ToHashSet();
    foreach ((string region, string quarter, string category) in expectedL3)
    {
      string l3 = $"transactions_{region}_{quarter}_{category}";
      bool exists = await PartitionExistsAsync(conn, l3);
      exists.Should().BeTrue($"L3 '{l3}' should exist");
    }

    // L4: leaf partitions exist and row counts match
    HashSet<(string Region, string Quarter, string Category, string Day)> expectedL4 = rows
      .Select(r =>
      {
        int q = (r.TxDate.Month - 1) / 3 + 1;
        return (r.Region, Quarter: $"{r.TxDate.Year}_q{q}", r.Category, Day: $"{r.EntryTime:yyyy_MM_dd}");
      })
      .ToHashSet();
    foreach ((string region, string quarter, string category, string day) in expectedL4)
    {
      string l4 = $"transactions_{region}_{quarter}_{category}_{day}";
      bool exists = await PartitionExistsAsync(conn, l4);
      exists.Should().BeTrue($"L4 leaf '{l4}' should exist");

      int expected = rows.Count(r =>
        r.Region == region &&
        $"{r.TxDate.Year}_q{(r.TxDate.Month - 1) / 3 + 1}" == quarter &&
        r.Category == category &&
        $"{r.EntryTime:yyyy_MM_dd}" == day);
      int actual = await conn.QuerySingleAsync<int>($"SELECT count(*) FROM {_schema}.{l4}");
      actual.Should().Be(expected, $"L4 leaf '{l4}' row count");
    }

    // Verify PartitionSmith comments exist (hashed names metadata)
    int commentCount = await conn.QuerySingleAsync<int>(
      "SELECT count(*) FROM pg_class c " +
      "JOIN pg_namespace n ON n.oid = c.relnamespace " +
      "WHERE n.nspname = @schema " +
      "AND obj_description(c.oid) LIKE 'PartitionSmith:%'",
      new { schema = _schema });
    commentCount.Should().BeGreaterThan(0, "hashed names should produce PartitionSmith metadata comments");

    // Verify partition count: L1 + L2 + L3 + L4
    int allPartitionCount = await conn.QuerySingleAsync<int>(
      "SELECT count(*) FROM pg_inherits i " +
      "JOIN pg_class c ON c.oid = i.inhrelid " +
      "JOIN pg_namespace n ON n.oid = c.relnamespace " +
      "WHERE n.nspname = @schema AND c.relkind IN ('r', 'p')",
      new { schema = _schema });
    int expectedTotal = regions.Length + expectedL2.Count + expectedL3.Count + expectedL4.Count;
    allPartitionCount.Should().Be(expectedTotal);

    await manager.DisposeAsync();
  }

  private async Task<bool> PartitionExistsAsync(NpgsqlConnection conn, string partitionName)
  {
    return await conn.QuerySingleAsync<bool>(
      "SELECT EXISTS(SELECT 1 FROM pg_class c " +
      "JOIN pg_namespace n ON n.oid = c.relnamespace " +
      "WHERE c.relname = @name AND n.nspname = @schema)",
      new { name = partitionName, schema = _schema });
  }
}
