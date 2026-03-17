using System.CommandLine;
using System.Globalization;
using InitializeNL.PgPartitionSmith.Cli.Services;
using InitializeNL.PgPartitionSmith.Core.Models;
using InitializeNL.PgPartitionSmith.Core.Naming;

Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;
CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
CultureInfo.DefaultThreadCurrentUICulture = CultureInfo.InvariantCulture;

Option<string> connectionStringOption = new Option<string>(
  name: "--connection-string",
  description: "PostgreSQL connection string")
{ IsRequired = true };

Option<string> tableNameOption = new Option<string>(
  name: "--table-name",
  description: "Name of the table to partition")
{ IsRequired = true };

Option<string> queryOption = new Option<string>(
  name: "--query",
  description: "SQL query that returns a JSON partition schema")
{ IsRequired = true };

Option<string> schemaOption = new Option<string>(
  name: "--schema",
  description: "PostgreSQL schema name",
  getDefaultValue: () => "public");

Option<bool> dryRunOption = new Option<bool>(
  name: "--dry-run",
  description: "Print SQL statements without executing them",
  getDefaultValue: () => false);

Option<string> partitionedNameOption = new Option<string>(
  name: "--partitioned-name",
  description: "Name suffix for the partitioned table",
  getDefaultValue: () => "part");

Option<string> unpartitionedNameOption = new Option<string>(
  name: "--unpartitioned-name",
  description: "Name suffix for the unpartitioned table",
  getDefaultValue: () => "unpart");

Option<bool> hashedNamesOption = new Option<bool>(
  name: "--hashed-names",
  description: "Use short hashed names for partition tables with COMMENT ON TABLE for context",
  getDefaultValue: () => false);

RootCommand rootCommand = new RootCommand("PostgreSQL Table Partitioning Tool")
{
  connectionStringOption,
  tableNameOption,
  queryOption,
  schemaOption,
  dryRunOption,
  partitionedNameOption,
  unpartitionedNameOption,
  hashedNamesOption
};

rootCommand.SetHandler(async (string connectionString, string tableName, string query, string schema, bool dryRun, string partitionedName, string unpartitionedName, bool hashedNames) =>
{
  try
  {
    PartitionSchemaService schemaService = new PartitionSchemaService(connectionString);
    PartitionSchema partitionSchema = await schemaService.GenerateSchemaFromQueryAsync(query);

    PartitionNamingService namingService = new PartitionNamingService(
      tableName,
      partitionedName,
      hashedNames,
      schema);

    PartitioningService partitioningService = new PartitioningService(
      connectionString,
      schema,
      dryRun,
      partitionedName,
      unpartitionedName,
      namingService);
    await partitioningService.PartitionTableAsync(tableName, partitionSchema);

    if (!dryRun)
    {
      Console.WriteLine($"Successfully partitioned table {schema}.{tableName}");
    }
  }
  catch (Exception ex)
  {
    Console.Error.WriteLine($"Error: {ex.Message}");
    Environment.Exit(1);
  }
}, connectionStringOption, tableNameOption, queryOption, schemaOption, dryRunOption, partitionedNameOption, unpartitionedNameOption, hashedNamesOption);

return await rootCommand.InvokeAsync(args);
