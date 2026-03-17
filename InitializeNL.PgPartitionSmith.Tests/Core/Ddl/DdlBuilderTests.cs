using FluentAssertions;
using InitializeNL.PgPartitionSmith.Core.Ddl;

namespace InitializeNL.PgPartitionSmith.Tests.Core.Ddl;

public class DdlBuilderTests
{
  [Fact]
  public void CreateRangePartition_GeneratesCorrectSql()
  {
    string sql = DdlBuilder.CreateRangePartition(
      schema: "public",
      partitionName: "situations_2026_04",
      parentTableName: "situations",
      from: "2026-04-01",
      to: "2026-05-01");

    sql.Should().Be(
      "CREATE TABLE public.situations_2026_04 PARTITION OF public.situations FOR VALUES FROM ('2026-04-01') TO ('2026-05-01')");
  }

  [Fact]
  public void CreateListPartition_GeneratesCorrectSql()
  {
    string sql = DdlBuilder.CreateListPartition(
      schema: "public",
      partitionName: "events_europe",
      parentTableName: "events",
      values: ["eu", "europe"]);

    sql.Should().Be(
      "CREATE TABLE public.events_europe PARTITION OF public.events FOR VALUES IN ('eu', 'europe')");
  }

  [Fact]
  public void CreateDefaultPartition_GeneratesCorrectSql()
  {
    string sql = DdlBuilder.CreateDefaultPartition(
      schema: "public",
      partitionName: "events_default",
      parentTableName: "events");

    sql.Should().Be(
      "CREATE TABLE public.events_default PARTITION OF public.events DEFAULT");
  }

  [Fact]
  public void CreateRangePartition_WithSubPartitionClause_IncludesPartitionBy()
  {
    string sql = DdlBuilder.CreateRangePartition(
      schema: "public",
      partitionName: "situations_2026_04",
      parentTableName: "situations",
      from: "2026-04-01",
      to: "2026-05-01",
      subPartitionBy: "LIST (region)");

    sql.Should().Contain("PARTITION BY LIST (region)");
  }

  [Fact]
  public void CreateListPartition_WithSubPartitionClause_IncludesPartitionBy()
  {
    string sql = DdlBuilder.CreateListPartition(
      schema: "public",
      partitionName: "situations_active",
      parentTableName: "situations",
      values: ["active"],
      subPartitionBy: "RANGE (period_start)");

    sql.Should().Contain("PARTITION BY RANGE (period_start)");
  }

  [Fact]
  public void CreatePartitionedTable_GeneratesCorrectSql()
  {
    string sql = DdlBuilder.CreatePartitionedTable(
      schema: "public",
      tableName: "situations",
      columns: "id BIGSERIAL, period_start TIMESTAMPTZ NOT NULL, PRIMARY KEY (id, period_start)",
      partitionType: "range",
      partitionColumn: "period_start");

    sql.Should().Be(
      "CREATE TABLE IF NOT EXISTS public.situations (id BIGSERIAL, period_start TIMESTAMPTZ NOT NULL, PRIMARY KEY (id, period_start)) PARTITION BY RANGE (period_start)");
  }
}
