namespace InitializeNL.PgPartitionSmith.Auto;

public interface IPartitionManager : IAsyncDisposable
{
  Task EnsurePartitionAsync<T>(T entity, CancellationToken ct = default);

  Task EnsurePartitionAsync(string tableName,
    IDictionary<string, object> values,
    CancellationToken ct = default);

  Task EnsurePartitionsAsync<T>(IEnumerable<T> entities, CancellationToken ct = default);

  Task EnsurePartitionsAsync(
    string tableName,
    IEnumerable<IDictionary<string, object>> values,
    CancellationToken ct = default);

  Task PreCreatePartitionsAsync(string tableName,
    DateTime from, DateTime to,
    CancellationToken ct = default);

  Task InitializeAsync(CancellationToken ct = default);
}
