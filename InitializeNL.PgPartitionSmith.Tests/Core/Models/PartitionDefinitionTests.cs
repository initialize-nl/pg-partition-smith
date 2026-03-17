using FluentAssertions;
using InitializeNL.PgPartitionSmith.Core.Models;

namespace InitializeNL.PgPartitionSmith.Tests.Core.Models;

public class PartitionDefinitionTests
{
  [Fact]
  public void Validate_InvalidType_Throws()
  {
    PartitionDefinition definition = new PartitionDefinition
    {
      Type = "hash",
      Items = [new PartitionItem { Field = "id", Name = "test", Values = ["1"] }]
    };

    Action act = () => definition.Validate("root");

    act.Should().Throw<InvalidOperationException>()
      .WithMessage("*Invalid partition type*hash*");
  }

  [Fact]
  public void Validate_EmptyItems_Throws()
  {
    PartitionDefinition definition = new PartitionDefinition
    {
      Type = "list",
      Items = []
    };

    Action act = () => definition.Validate("root");

    act.Should().Throw<InvalidOperationException>()
      .WithMessage("*has no items*");
  }

  [Fact]
  public void Validate_ValidListDefinition_DoesNotThrow()
  {
    PartitionDefinition definition = new PartitionDefinition
    {
      Type = "list",
      Items = [new PartitionItem { Field = "region", Name = "europe", Values = ["eu"] }]
    };

    Action act = () => definition.Validate("root");

    act.Should().NotThrow();
  }

  [Fact]
  public void Validate_ValidRangeDefinition_DoesNotThrow()
  {
    PartitionDefinition definition = new PartitionDefinition
    {
      Type = "range",
      Items = [new PartitionItem { Field = "date", Name = "jan", From = "2026-01-01", To = "2026-02-01" }]
    };

    Action act = () => definition.Validate("root");

    act.Should().NotThrow();
  }
}
