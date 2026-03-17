using System.Data;
using Dapper;
using Npgsql;

namespace InitializeNL.PgPartitionSmith.Core.Locking;

public class TablePartitionLock : IPartitionLock
{
  private readonly string _schema;
  private readonly string _lockTableName;

  public TablePartitionLock(string lockTableName = "__partition_lock", string schema = "public")
  {
    _schema = schema;
    _lockTableName = lockTableName;
  }

  private string QualifiedLockTableName => $"\"{EscapeIdentifier(_schema)}\".\"{EscapeIdentifier(_lockTableName)}\"";

  private static string EscapeIdentifier(string identifier)
  {
    return identifier.Replace("\"", "\"\"");
  }

  public async Task EnsureLockTableExistsAsync(NpgsqlConnection connection)
  {
    await connection.ExecuteAsync(
      $@"CREATE TABLE IF NOT EXISTS {QualifiedLockTableName} (
        one INT PRIMARY KEY CHECK (one = 1),
        created_on TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'utc')
      )");
  }

  public async Task<bool> AcquireAsync(NpgsqlConnection connection, string partitionName, CancellationToken ct = default)
  {
    try
    {
      if (connection.State != ConnectionState.Open)
        await connection.OpenAsync(ct);

      await using NpgsqlTransaction trx = await connection.BeginTransactionAsync(
        IsolationLevel.RepeatableRead, ct);
      int rowsAffected = await connection.ExecuteAsync(
        $"INSERT INTO {QualifiedLockTableName}(one, created_on) VALUES (1, NOW()) ON CONFLICT DO NOTHING",
        transaction: trx);
      await trx.CommitAsync(ct);

      return rowsAffected > 0;
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
      await connection.ExecuteAsync($"DELETE FROM {QualifiedLockTableName} WHERE one = 1");
      return true;
    }
    catch (NpgsqlException)
    {
      return false;
    }
  }
}
