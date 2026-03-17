using FluentAssertions;
using InitializeNL.PgPartitionSmith.Core.Naming;

namespace InitializeNL.PgPartitionSmith.Tests.Core.Naming;

public class PartitionNamingServiceTests
{
  [Fact]
  public void GetPartitionTableName_PlainMode_ReturnsParentUnderscoreItem()
  {
    PartitionNamingService svc = new PartitionNamingService("orders", "part", hashedNames: false, "public");

    string result = svc.GetPartitionTableName("orders_part", "europe", "root/europe");

    result.Should().Be("orders_part_europe");
  }

  [Fact]
  public void GetPartitionTableName_HashedMode_Returns8CharHash()
  {
    PartitionNamingService svc = new PartitionNamingService("orders", "part", hashedNames: true, "public");

    string result = svc.GetPartitionTableName("orders_part", "europe", "root/europe");

    result.Should().MatchRegex(@"^orders_part_[0-9a-f]{8}$");
  }

  [Fact]
  public void GetPartitionTableName_HashedMode_DeterministicForSameInput()
  {
    PartitionNamingService svc = new PartitionNamingService("orders", "part", hashedNames: true, "public");

    string result1 = svc.GetPartitionTableName("orders_part", "europe", "root/europe");
    string result2 = svc.GetPartitionTableName("orders_part", "europe", "root/europe");

    result1.Should().Be(result2);
  }

  [Fact]
  public void GetCommentSql_PlainMode_ReturnsNull()
  {
    PartitionNamingService svc = new PartitionNamingService("orders", "part", hashedNames: false, "public");
    InitializeNL.PgPartitionSmith.Core.Models.PartitionItem item = new InitializeNL.PgPartitionSmith.Core.Models.PartitionItem
    {
      Field = "region", Name = "europe", Values = ["eu"]
    };

    string? result = svc.GetCommentSql("orders_part_europe", item, "list", "root/europe", false);

    result.Should().BeNull();
  }

  [Fact]
  public void GetCommentSql_HashedMode_ReturnsCommentWithJson()
  {
    PartitionNamingService svc = new PartitionNamingService("orders", "part", hashedNames: true, "public");
    InitializeNL.PgPartitionSmith.Core.Models.PartitionItem item = new InitializeNL.PgPartitionSmith.Core.Models.PartitionItem
    {
      Field = "region", Name = "europe", Values = ["eu"]
    };

    string? result = svc.GetCommentSql("orders_part_abc12345", item, "list", "root/europe", false);

    result.Should().NotBeNull();
    result.Should().Contain("COMMENT ON TABLE");
    result.Should().Contain("PartitionSmith:");
    result.Should().Contain("\"field\":\"region\"");
  }
}
