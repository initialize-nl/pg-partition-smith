using Microsoft.Extensions.DependencyInjection;
using InitializeNL.PgPartitionSmith.Auto;
using InitializeNL.PgPartitionSmith.Auto.Configuration;

namespace InitializeNL.PgPartitionSmith.Auto.Hosting;

public static class ServiceCollectionExtensions
{
  public static IServiceCollection AddPgPartitionSmith(
    this IServiceCollection services,
    Action<PartitionManagerBuilder> configure)
  {
    PartitionManagerBuilder builder = new PartitionManagerBuilder();
    configure(builder);
    IPartitionManager manager = builder.Build();

    services.AddSingleton(manager);
    services.AddHostedService(sp =>
      new PartitionManagerHostedService(sp.GetRequiredService<IPartitionManager>()));

    return services;
  }
}
