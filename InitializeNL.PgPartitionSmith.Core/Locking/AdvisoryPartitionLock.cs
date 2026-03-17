using System.Security.Cryptography;
using System.Text;
using Dapper;
using Npgsql;

namespace InitializeNL.PgPartitionSmith.Core.Locking;

public class AdvisoryPartitionLock : IPartitionLock
{
  public async Task<bool> AcquireAsync(NpgsqlConnection connection, string partitionName, CancellationToken ct = default)
  {
    try
    {
      long lockId = ComputeLockId(partitionName);
      CommandDefinition command = new(
        "SELECT pg_advisory_lock(@lockId)",
        new { lockId },
        cancellationToken: ct);
      await connection.ExecuteAsync(command);
      return true;
    }
    catch (NpgsqlException)
    {
      return false;
    }
  }

  public async Task<bool> ReleaseAsync(NpgsqlConnection connection, string partitionName, CancellationToken ct = default)
  {
    try
    {
      long lockId = ComputeLockId(partitionName);
      CommandDefinition command = new(
        "SELECT pg_advisory_unlock(@lockId)",
        new { lockId },
        cancellationToken: ct);
      await connection.ExecuteAsync(command);
      return true;
    }
    catch (NpgsqlException)
    {
      return false;
    }
  }

  public static long ComputeLockId(string partitionName)
  {
    byte[] hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(partitionName));
    return BitConverter.ToInt64(hashBytes, 0);
  }
}
