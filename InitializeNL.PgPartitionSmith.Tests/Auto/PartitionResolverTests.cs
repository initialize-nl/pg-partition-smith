using FluentAssertions;
using InitializeNL.PgPartitionSmith.Auto;
using InitializeNL.PgPartitionSmith.Auto.Configuration;

namespace InitializeNL.PgPartitionSmith.Tests.Auto;

public class PartitionResolverTests
{
  [Theory]
  [InlineData("2026-04-15", "situations_2026_04", "2026-04-01", "2026-05-01")]
  [InlineData("2026-01-01", "situations_2026_01", "2026-01-01", "2026-02-01")]
  [InlineData("2026-12-31", "situations_2026_12", "2026-12-01", "2027-01-01")]
  public void ResolveRange_Monthly_CorrectNameAndBoundaries(
    string inputDate, string expectedName, string expectedFrom, string expectedTo)
  {
    TablePartitionConfig config = new TablePartitionConfig
    {
      TableName = "situations",
      PartitionType = "range",
      PartitionColumn = "period_start",
      Interval = RangeInterval.Monthly
    };

    RangePartitionInfo result = PartitionResolver.ResolveRange(config, DateTime.Parse(inputDate));

    result.PartitionName.Should().Be(expectedName);
    result.From.Should().Be(expectedFrom);
    result.To.Should().Be(expectedTo);
  }

  [Theory]
  [InlineData("2026-04-15", "situations_2026_q2", "2026-04-01", "2026-07-01")]
  [InlineData("2026-01-01", "situations_2026_q1", "2026-01-01", "2026-04-01")]
  [InlineData("2026-12-31", "situations_2026_q4", "2026-10-01", "2027-01-01")]
  public void ResolveRange_Quarterly_CorrectNameAndBoundaries(
    string inputDate, string expectedName, string expectedFrom, string expectedTo)
  {
    TablePartitionConfig config = new TablePartitionConfig
    {
      TableName = "situations",
      PartitionType = "range",
      PartitionColumn = "period_start",
      Interval = RangeInterval.Quarterly
    };

    RangePartitionInfo result = PartitionResolver.ResolveRange(config, DateTime.Parse(inputDate));

    result.PartitionName.Should().Be(expectedName);
    result.From.Should().Be(expectedFrom);
    result.To.Should().Be(expectedTo);
  }

  [Fact]
  public void ResolveRange_Yearly_CorrectNameAndBoundaries()
  {
    TablePartitionConfig config = new TablePartitionConfig
    {
      TableName = "situations",
      PartitionType = "range",
      PartitionColumn = "period_start",
      Interval = RangeInterval.Yearly
    };

    RangePartitionInfo result = PartitionResolver.ResolveRange(config, DateTime.Parse("2026-07-15"));

    result.PartitionName.Should().Be("situations_2026");
    result.From.Should().Be("2026-01-01");
    result.To.Should().Be("2027-01-01");
  }

  [Fact]
  public void ResolveRange_Daily_CorrectNameAndBoundaries()
  {
    TablePartitionConfig config = new TablePartitionConfig
    {
      TableName = "situations",
      PartitionType = "range",
      PartitionColumn = "period_start",
      Interval = RangeInterval.Daily
    };

    RangePartitionInfo result = PartitionResolver.ResolveRange(config, DateTime.Parse("2026-04-15"));

    result.PartitionName.Should().Be("situations_2026_04_15");
    result.From.Should().Be("2026-04-15");
    result.To.Should().Be("2026-04-16");
  }

  [Fact]
  public void ResolveList_ReturnsValueAsPartitionName()
  {
    TablePartitionConfig config = new TablePartitionConfig
    {
      TableName = "events",
      PartitionType = "list",
      PartitionColumn = "region"
    };

    ListPartitionInfo result = PartitionResolver.ResolveList(config, "europe");

    result.PartitionName.Should().Be("events_europe");
    result.Value.Should().Be("europe");
  }

  [Fact]
  public void ResolveList_WithSpaces_SanitizesName()
  {
    TablePartitionConfig config = new TablePartitionConfig
    {
      TableName = "events",
      PartitionType = "list",
      PartitionColumn = "region"
    };

    ListPartitionInfo result = PartitionResolver.ResolveList(config, "North America");

    result.PartitionName.Should().Be("events_north_america");
    result.Value.Should().Be("North America");
  }

  [Fact]
  public void ResolveConfiguredListPartition_WithMatchingValue_ReturnsConfiguredItem()
  {
    TablePartitionConfig config = new TablePartitionConfig
    {
      TableName = "events",
      PartitionType = "list",
      PartitionColumn = "region",
      ListPartitions =
      [
        new ListPartitionItemConfig
        {
          Name = "eu",
          Values = ["eu", "europe"]
        },
        new ListPartitionItemConfig
        {
          Name = "us",
          Values = ["us"]
        }
      ]
    };

    ListPartitionItemConfig? result = PartitionResolver.ResolveConfiguredListPartition(config, "europe");

    result.Should().NotBeNull();
    result!.Name.Should().Be("eu");
  }

  [Fact]
  public void ResolveListPartitionName_ForConfiguredItem_UsesItemName()
  {
    TablePartitionConfig config = new TablePartitionConfig
    {
      TableName = "events",
      PartitionType = "list",
      PartitionColumn = "region"
    };

    string partitionName = PartitionResolver.ResolveListPartitionName(config, "North America");

    partitionName.Should().Be("events_north_america");
  }

  [Fact]
  public void ResolveConfiguredListPartition_WithKnownValues_UsesLegacyStaticListConfig()
  {
    TablePartitionConfig config = new TablePartitionConfig
    {
      TableName = "events",
      PartitionType = "list",
      PartitionColumn = "region",
      KnownValues = ["eu", "us"]
    };

    ListPartitionItemConfig? result = PartitionResolver.ResolveConfiguredListPartition(config, "eu");

    result.Should().NotBeNull();
    result!.Name.Should().Be("eu");
    result.Values.Should().Equal("eu");
  }
}
