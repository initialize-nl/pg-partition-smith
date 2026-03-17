using System.Collections.Concurrent;

namespace InitializeNL.PgPartitionSmith.Auto;

public class PartitionCache
{
  private readonly ConcurrentDictionary<string, bool> _known = new();

  public bool Exists(string partitionName) => _known.ContainsKey(partitionName);

  public void MarkExists(string partitionName) => _known.TryAdd(partitionName, true);

  public void WarmUp(IEnumerable<string> partitionNames)
  {
    foreach (string name in partitionNames)
    {
      _known.TryAdd(name, true);
    }
  }
}
