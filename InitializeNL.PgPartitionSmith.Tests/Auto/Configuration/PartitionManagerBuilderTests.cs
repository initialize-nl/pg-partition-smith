using FluentAssertions;
using InitializeNL.PgPartitionSmith.Auto;
using InitializeNL.PgPartitionSmith.Auto.Configuration;

namespace InitializeNL.PgPartitionSmith.Tests.Auto.Configuration;

public class PartitionManagerBuilderTests
{
  [Fact]
  public void Build_WithoutConnectionString_Throws()
  {
    PartitionManagerBuilder builder = new PartitionManagerBuilder();

    Action act = () => builder.Build();

    act.Should().Throw<InvalidOperationException>()
      .WithMessage("*connection string*");
  }

  [Fact]
  public void Build_WithoutTables_Throws()
  {
    PartitionManagerBuilder builder = new PartitionManagerBuilder()
      .UseConnectionString("Host=localhost")
      .UseAdvisoryLocks();

    Action act = () => builder.Build();

    act.Should().Throw<InvalidOperationException>()
      .WithMessage("*at least one table*");
  }

  [Fact]
  public void ForTable_RangeMonthly_StoresConfig()
  {
    PartitionManagerBuilder builder = new PartitionManagerBuilder()
      .UseConnectionString("Host=localhost")
      .UseAdvisoryLocks()
      .ForTable("situations", t => t
        .RangeBy("period_start")
        .Monthly()
      );

    Dictionary<string, TablePartitionConfig> config = builder.GetTableConfigs();

    config.Should().ContainKey("situations");
    config["situations"].PartitionType.Should().Be("range");
    config["situations"].PartitionColumn.Should().Be("period_start");
    config["situations"].Interval.Should().Be(RangeInterval.Monthly);
  }

  [Fact]
  public void ForTable_ListWithAutoCreate_StoresConfig()
  {
    PartitionManagerBuilder builder = new PartitionManagerBuilder()
      .UseConnectionString("Host=localhost")
      .UseAdvisoryLocks()
      .ForTable("events", t => t
        .ListBy("region")
        .AutoCreateForNewValues()
      );

    Dictionary<string, TablePartitionConfig> config = builder.GetTableConfigs();

    config.Should().ContainKey("events");
    config["events"].PartitionType.Should().Be("list");
    config["events"].PartitionColumn.Should().Be("region");
    config["events"].AutoCreateForNewValues.Should().BeTrue();
  }

  [Fact]
  public void ForTable_ListWithStaticPartitions_StoresNamedItems()
  {
    PartitionManagerBuilder builder = new PartitionManagerBuilder()
      .UseConnectionString("Host=localhost")
      .UseAdvisoryLocks()
      .ForTable("events", t => t
        .ListBy("region")
        .WithPartition("eu", ["eu", "europe"])
        .WithPartition("us", ["us", "united states"])
      );

    Dictionary<string, TablePartitionConfig> config = builder.GetTableConfigs();

    config["events"].ListPartitions.Should().HaveCount(2);
    config["events"].ListPartitions[0].Name.Should().Be("eu");
    config["events"].ListPartitions[0].Values.Should().Equal("eu", "europe");
    config["events"].ListPartitions[1].Name.Should().Be("us");
  }

  [Fact]
  public void ForTable_ListWithStaticPartitionChild_StoresPerItemChildConfig()
  {
    PartitionManagerBuilder builder = new PartitionManagerBuilder()
      .UseConnectionString("Host=localhost")
      .UseAdvisoryLocks()
      .ForTable("situations", t => t
        .ListBy("status")
        .WithPartition("active", ["active"], child => child
          .RangeBy("period_start")
          .Monthly())
      );

    Dictionary<string, TablePartitionConfig> config = builder.GetTableConfigs();
    TablePartitionConfig? childConfig = config["situations"].ListPartitions[0].ChildPartition;

    childConfig.Should().NotBeNull();
    childConfig!.PartitionType.Should().Be("range");
    childConfig.PartitionColumn.Should().Be("period_start");
    childConfig.Interval.Should().Be(RangeInterval.Monthly);
  }

  [Fact]
  public void ForTable_WithSchema_PropagatesToImplicitChildSchemaOnly()
  {
    PartitionManagerBuilder builder = new PartitionManagerBuilder()
      .UseConnectionString("Host=localhost")
      .UseAdvisoryLocks()
      .ForTable("situations", t => t
        .ListBy("status")
        .WithPartition("active", ["active"], child => child
          .RangeBy("period_start")
          .Monthly())
        .WithPartition("public_child", ["public"], child => child
          .RangeBy("period_start")
          .Monthly()
          .WithSchema("public"))
        .WithSchema("custom_schema")
      );

    Dictionary<string, TablePartitionConfig> config = builder.GetTableConfigs();

    config["situations"].ListPartitions[0].ChildPartition!.Schema.Should().Be("custom_schema");
    config["situations"].ListPartitions[1].ChildPartition!.Schema.Should().Be("public");
  }

  [Fact]
  public void ForTable_AutoCreateWithRemainder_Throws()
  {
    PartitionManagerBuilder builder = new PartitionManagerBuilder()
      .UseConnectionString("Host=localhost")
      .UseAdvisoryLocks();

    Action act = () => builder.ForTable("events", t => t
      .ListBy("region")
      .AutoCreateForNewValues()
      .WithRemainderPartition()
    );

    act.Should().Throw<InvalidOperationException>()
      .WithMessage("*Cannot combine*auto-create*remainder*");
  }

  [Fact]
  public void ForTable_WithRemainderPartition_StoresConfig()
  {
    PartitionManagerBuilder builder = new PartitionManagerBuilder()
      .UseConnectionString("Host=localhost")
      .UseAdvisoryLocks()
      .ForTable("events", t => t
        .ListBy("region")
        .WithRemainderPartition()
      );

    Dictionary<string, TablePartitionConfig> config = builder.GetTableConfigs();

    config["events"].HasRemainderPartition.Should().BeTrue();
  }

  [Fact]
  public void ForTable_StaticListPartitionsWithAutoCreate_Throws()
  {
    PartitionManagerBuilder builder = new PartitionManagerBuilder()
      .UseConnectionString("Host=localhost")
      .UseAdvisoryLocks();

    Action act = () => builder.ForTable("events", t => t
      .ListBy("region")
      .WithPartition("eu", ["eu"])
      .AutoCreateForNewValues()
    );

    act.Should().Throw<InvalidOperationException>()
      .WithMessage("*Cannot combine auto-create with explicit list partitions*");
  }

  [Fact]
  public void Build_WithValidConfig_ReturnsPartitionManager()
  {
    PartitionManagerBuilder builder = new PartitionManagerBuilder()
      .UseConnectionString("Host=localhost")
      .UseAdvisoryLocks()
      .ForTable("situations", t => t
        .RangeBy("period_start")
        .Monthly()
      );

    IPartitionManager manager = builder.Build();

    manager.Should().NotBeNull();
    manager.Should().BeAssignableTo<IPartitionManager>();
  }
}
