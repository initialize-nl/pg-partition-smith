using System.Globalization;
using System.Linq;
using InitializeNL.PgPartitionSmith.Auto.Configuration;

namespace InitializeNL.PgPartitionSmith.Auto;

public static class PartitionResolver
{
  public static RangePartitionInfo ResolveRange(TablePartitionConfig config, DateTime value)
  {
    return config.Interval switch
    {
      RangeInterval.Daily => ResolveDaily(config.TableName, value),
      RangeInterval.Weekly => ResolveWeekly(config.TableName, value),
      RangeInterval.Monthly => ResolveMonthly(config.TableName, value),
      RangeInterval.Quarterly => ResolveQuarterly(config.TableName, value),
      RangeInterval.Yearly => ResolveYearly(config.TableName, value),
      _ => throw new InvalidOperationException($"Unsupported interval: {config.Interval}")
    };
  }

  public static ListPartitionInfo ResolveList(TablePartitionConfig config, string value)
  {
    return new ListPartitionInfo(ResolveListPartitionName(config, value), value);
  }

  public static ListPartitionItemConfig? ResolveConfiguredListPartition(TablePartitionConfig config, string value)
  {
    foreach (ListPartitionItemConfig item in GetConfiguredListPartitions(config))
    {
      if (item.Values.Any(v => string.Equals(v, value, StringComparison.Ordinal)))
      {
        return item;
      }
    }

    return null;
  }

  public static string ResolveListPartitionName(TablePartitionConfig config, string nameOrValue)
  {
    string sanitized = SanitizePartitionToken(nameOrValue);
    return $"{config.TableName}_{sanitized}";
  }

  public static IReadOnlyList<ListPartitionItemConfig> GetConfiguredListPartitions(TablePartitionConfig config)
  {
    if (config.ListPartitions.Count > 0)
    {
      return config.ListPartitions;
    }

    if (config.KnownValues == null || config.KnownValues.Count == 0)
    {
      return [];
    }

    return config.KnownValues
      .Select(value => new ListPartitionItemConfig
      {
        Name = SanitizePartitionToken(value),
        Values = [value]
      })
      .ToList();
  }

  private static RangePartitionInfo ResolveDaily(string tableName, DateTime value)
  {
    DateTime from = value.Date;
    DateTime to = from.AddDays(1);
    string name = $"{tableName}_{from:yyyy_MM_dd}";
    return new RangePartitionInfo(name, Format(from), Format(to));
  }

  private static RangePartitionInfo ResolveWeekly(string tableName, DateTime value)
  {
    int week = ISOWeek.GetWeekOfYear(value);
    int isoYear = ISOWeek.GetYear(value);
    DateTime from = ISOWeek.ToDateTime(isoYear, week, DayOfWeek.Monday);
    DateTime to = from.AddDays(7);
    string name = $"{tableName}_{isoYear}_w{week:D2}";
    return new RangePartitionInfo(name, Format(from), Format(to));
  }

  private static RangePartitionInfo ResolveMonthly(string tableName, DateTime value)
  {
    DateTime from = new DateTime(value.Year, value.Month, 1);
    DateTime to = from.AddMonths(1);
    string name = $"{tableName}_{from:yyyy_MM}";
    return new RangePartitionInfo(name, Format(from), Format(to));
  }

  private static RangePartitionInfo ResolveQuarterly(string tableName, DateTime value)
  {
    int quarter = (value.Month - 1) / 3 + 1;
    DateTime from = new DateTime(value.Year, (quarter - 1) * 3 + 1, 1);
    DateTime to = from.AddMonths(3);
    string name = $"{tableName}_{value.Year}_q{quarter}";
    return new RangePartitionInfo(name, Format(from), Format(to));
  }

  private static RangePartitionInfo ResolveYearly(string tableName, DateTime value)
  {
    DateTime from = new DateTime(value.Year, 1, 1);
    DateTime to = from.AddYears(1);
    string name = $"{tableName}_{value.Year}";
    return new RangePartitionInfo(name, Format(from), Format(to));
  }

  private static string Format(DateTime dt) =>
    dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

  private static string SanitizePartitionToken(string value) =>
    value.ToLowerInvariant().Replace(" ", "_");
}
