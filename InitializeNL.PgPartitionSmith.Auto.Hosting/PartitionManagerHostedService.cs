using Microsoft.Extensions.Hosting;
using InitializeNL.PgPartitionSmith.Auto;

namespace InitializeNL.PgPartitionSmith.Auto.Hosting;

public class PartitionManagerHostedService : IHostedService
{
  private readonly IPartitionManager _manager;

  public PartitionManagerHostedService(IPartitionManager manager)
  {
    _manager = manager;
  }

  public async Task StartAsync(CancellationToken ct)
  {
    await _manager.InitializeAsync(ct);
  }

  public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}
