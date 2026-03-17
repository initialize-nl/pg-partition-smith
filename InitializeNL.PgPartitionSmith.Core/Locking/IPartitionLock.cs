using Npgsql;

namespace InitializeNL.PgPartitionSmith.Core.Locking;

public interface IPartitionLock
{
  Task<bool> AcquireAsync(NpgsqlConnection connection, string partitionName, CancellationToken ct = default);
  Task<bool> ReleaseAsync(NpgsqlConnection connection, string partitionName, CancellationToken ct = default);
}
