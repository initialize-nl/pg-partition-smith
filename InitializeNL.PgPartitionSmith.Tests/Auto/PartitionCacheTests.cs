using FluentAssertions;
using InitializeNL.PgPartitionSmith.Auto;

namespace InitializeNL.PgPartitionSmith.Tests.Auto;

public class PartitionCacheTests
{
  [Fact]
  public void Exists_UnknownPartition_ReturnsFalse()
  {
    PartitionCache cache = new PartitionCache();

    cache.Exists("situations_2026_04").Should().BeFalse();
  }

  [Fact]
  public void MarkExists_ThenExists_ReturnsTrue()
  {
    PartitionCache cache = new PartitionCache();

    cache.MarkExists("situations_2026_04");

    cache.Exists("situations_2026_04").Should().BeTrue();
  }

  [Fact]
  public void WarmUp_AddsAllPartitions()
  {
    PartitionCache cache = new PartitionCache();

    cache.WarmUp(["situations_2026_01", "situations_2026_02", "situations_2026_03"]);

    cache.Exists("situations_2026_01").Should().BeTrue();
    cache.Exists("situations_2026_02").Should().BeTrue();
    cache.Exists("situations_2026_03").Should().BeTrue();
    cache.Exists("situations_2026_04").Should().BeFalse();
  }

  [Fact]
  public void IsThreadSafe_ConcurrentAccess_DoesNotThrow()
  {
    PartitionCache cache = new PartitionCache();

    Action act = () => Parallel.For(0, 1000, i =>
    {
      string name = $"partition_{i % 50}";
      cache.MarkExists(name);
      _ = cache.Exists(name);
    });

    act.Should().NotThrow();
  }
}
