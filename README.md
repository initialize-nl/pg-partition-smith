# InitializeNL.PgPartitionSmith

A PostgreSQL table partitioning toolkit for .NET 10+. Automatically creates and manages partitions at runtime.

## Features

- On-demand partition creation with concurrency-safe locking
- Range partitioning (daily, weekly, monthly, quarterly, yearly)
- List partitioning with static or dynamic auto-created partitions
- Nested sub-partitioning (e.g., list by status, then range by date)
- Optional default/remainder partitions
- CLI tool for migrating existing tables to partitioned tables
- In-memory partition cache for fast lookups
- ASP.NET hosted service integration via `Microsoft.Extensions.Hosting`
- Advisory lock or table-based distributed locking
- Hashed partition names with `COMMENT ON TABLE` metadata

## Packages

| Package | Description |
|---------|-------------|
| `InitializeNL.PgPartitionSmith.Core` | Core library: models, DDL generation, catalog queries, naming, and locking |
| `InitializeNL.PgPartitionSmith.Auto` | Runtime auto-partitioning for on-demand partition creation |
| `InitializeNL.PgPartitionSmith.Auto.Hosting` | Microsoft.Extensions.Hosting integration with dependency injection |
| `InitializeNL.PgPartitionSmith.Cli` | CLI tool for migrating unpartitioned tables |

## Quick Start

### As a library

```csharp
IPartitionManager manager = new PartitionManagerBuilder()
    .UseConnectionString("Host=localhost;Database=mydb")
    .UseAdvisoryLocks()
    .ForTable("events", t => t
        .RangeBy("created_at")
        .Monthly()
        .WithSchema("public"))
    .Build();

await manager.InitializeAsync();

await manager.EnsurePartitionAsync("events", new Dictionary<string, object>
{
    ["created_at"] = DateTime.UtcNow
});
```

### With ASP.NET hosting

```csharp
services.AddPgPartitionSmith(builder => builder
    .UseConnectionString(connectionString)
    .UseAdvisoryLocks()
    .ForTable("events", t => t
        .RangeBy("created_at")
        .Monthly()));
```

### With entity mapping

```csharp
var manager = new PartitionManagerBuilder()
    .UseConnectionString(connectionString)
    .ForTable("events", t => t
        .RangeBy("created_at")
        .Monthly()
        .MappedFrom<Event>(e => e.CreatedAt))
    .Build();

await manager.EnsurePartitionAsync(myEvent);
```

### List partitioning with sub-partitions

```csharp
var manager = new PartitionManagerBuilder()
    .UseConnectionString(connectionString)
    .ForTable("situations", t => t
        .ListBy("status")
        .WithPartition("active", ["active"], child => child
            .RangeBy("period_start").Monthly())
        .WithPartition("finished", ["finished"], child => child
            .RangeBy("period_start").Monthly()))
    .Build();
```

### Pre-creating partitions

```csharp
await manager.PreCreatePartitionsAsync("events",
    DateTime.UtcNow,
    DateTime.UtcNow.AddMonths(6));
```

### As a CLI

```bash
dotnet run --project InitializeNL.PgPartitionSmith.Cli -- \
    --connection-string "Host=localhost;Database=mydb;Username=postgres;Password=secret" \
    --table-name events \
    --query "SELECT json_build_object('partition', json_build_object('type', 'range', ...))::text" \
    --execute
```

## CLI Options

| Option | Description |
|--------|-------------|
| `--connection-string` | PostgreSQL connection string |
| `--table-name` | Name of the table to partition |
| `--query` | SQL query that returns a JSON partition schema |
| `--schema` | PostgreSQL schema name (default: `public`) |
| `--dry-run` | Print SQL statements without executing them |
| `--partitioned-name` | Name suffix for the partitioned table (default: `part`) |
| `--unpartitioned-name` | Name suffix for the unpartitioned table (default: `unpart`) |
| `--hashed-names` | Use short hashed names with `COMMENT ON TABLE` for context |

## Configuration Options

| Method | Description |
|--------|-------------|
| `UseConnectionString(string)` | PostgreSQL connection string |
| `UseAdvisoryLocks()` | Use `pg_advisory_lock` for concurrency (default) |
| `UseTableLock(name, schema)` | Use table-based locking |
| `UseHashedNames()` | Use SHA256-hashed partition names |
| `ForTable(name, config)` | Configure partitioning for a table |
| `RangeBy(column)` | Range partition by column |
| `ListBy(column)` | List partition by column |
| `Daily() / Weekly() / Monthly() / Quarterly() / Yearly()` | Range interval |
| `WithPartition(name, values, child?)` | Named list partition with optional sub-partition |
| `AutoCreateForNewValues()` | Auto-create list partitions for unknown values |
| `WithRemainderPartition()` | Add a DEFAULT catch-all partition |
| `SubPartition(config)` | Nested sub-partitioning |
| `PreCreate(TimeSpan)` | Pre-create partitions ahead of time on startup |
| `CreateIfNotExists(columns)` | Auto-create the partitioned table if it doesn't exist |
| `MappedFrom<T>(expression)` | Map entity property for `EnsurePartitionAsync<T>()` |
| `WithSchema(string)` | PostgreSQL schema (default: `public`) |

## License

[MIT](LICENSE)
