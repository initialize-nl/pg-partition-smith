using FluentAssertions;
using InitializeNL.PgPartitionSmith.Core.Locking;

namespace InitializeNL.PgPartitionSmith.Tests.Core.Locking;

public class AdvisoryPartitionLockTests
{
  [Fact]
  public void ComputeLockId_SameInput_ReturnsSameHash()
  {
    long hash1 = AdvisoryPartitionLock.ComputeLockId("situations_2026_04");
    long hash2 = AdvisoryPartitionLock.ComputeLockId("situations_2026_04");

    hash1.Should().Be(hash2);
  }

  [Fact]
  public void ComputeLockId_DifferentInput_ReturnsDifferentHash()
  {
    long hash1 = AdvisoryPartitionLock.ComputeLockId("situations_2026_04");
    long hash2 = AdvisoryPartitionLock.ComputeLockId("situations_2026_05");

    hash1.Should().NotBe(hash2);
  }
}
