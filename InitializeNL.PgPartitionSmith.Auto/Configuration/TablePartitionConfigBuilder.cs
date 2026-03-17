using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace InitializeNL.PgPartitionSmith.Auto.Configuration;

public class TablePartitionConfigBuilder
{
  private readonly TablePartitionConfig _config = new();

  public TablePartitionConfigBuilder RangeBy(string columnName)
  {
    _config.PartitionType = "range";
    _config.PartitionColumn = columnName;
    return this;
  }

  public TablePartitionConfigBuilder ListBy(string columnName)
  {
    _config.PartitionType = "list";
    _config.PartitionColumn = columnName;
    return this;
  }

  public TablePartitionConfigBuilder MappedFrom<T>(Expression<Func<T, object?>> propertyExpression)
  {
    MemberExpression member = propertyExpression.Body is UnaryExpression unary
      ? (MemberExpression)unary.Operand
      : (MemberExpression)propertyExpression.Body;
    _config.MappedProperty = (PropertyInfo)member.Member;
    return this;
  }

  public TablePartitionConfigBuilder Daily() { _config.Interval = RangeInterval.Daily; return this; }
  public TablePartitionConfigBuilder Weekly() { _config.Interval = RangeInterval.Weekly; return this; }
  public TablePartitionConfigBuilder Monthly() { _config.Interval = RangeInterval.Monthly; return this; }
  public TablePartitionConfigBuilder Quarterly() { _config.Interval = RangeInterval.Quarterly; return this; }
  public TablePartitionConfigBuilder Yearly() { _config.Interval = RangeInterval.Yearly; return this; }

  public TablePartitionConfigBuilder WithValues(params string[] values)
  {
    _config.KnownValues = [.. values];
    return this;
  }

  public TablePartitionConfigBuilder WithPartition(string name, params object[] values)
  {
    return WithPartition(name, values, null);
  }

  public TablePartitionConfigBuilder WithPartition(
    string name,
    object[] values,
    Action<TablePartitionConfigBuilder>? childBuilder)
  {
    if (_config.PartitionType != "list")
    {
      throw new InvalidOperationException("WithPartition() is only valid for list partitioning.");
    }

    if (string.IsNullOrWhiteSpace(name))
    {
      throw new InvalidOperationException("List partition name is required.");
    }

    if (values.Length == 0)
    {
      throw new InvalidOperationException("List partition must contain at least one value.");
    }

    if (_config.AutoCreateForNewValues)
    {
      throw new InvalidOperationException(
        "Cannot combine auto-create with explicit list partitions. Choose one list strategy.");
    }

    TablePartitionConfig? childConfig = null;
    if (childBuilder != null)
    {
      TablePartitionConfigBuilder child = new TablePartitionConfigBuilder();
      childBuilder(child);
      childConfig = child.Build();
      ApplySchemaIfImplicit(childConfig, _config.Schema);
    }

    ListPartitionItemConfig item = new ListPartitionItemConfig
    {
      Name = name,
      Values = values.Select(FormatListValue).ToList(),
      ChildPartition = childConfig
    };

    _config.ListPartitions.Add(item);
    return this;
  }

  public TablePartitionConfigBuilder AutoCreateForNewValues()
  {
    if (_config.HasRemainderPartition)
      throw new InvalidOperationException(
        "Cannot combine auto-create with remainder partition. A DEFAULT partition would swallow new values.");
    if (_config.ListPartitions.Count > 0)
      throw new InvalidOperationException(
        "Cannot combine auto-create with explicit list partitions. Choose one list strategy.");
    _config.AutoCreateForNewValues = true;
    return this;
  }

  public TablePartitionConfigBuilder WithRemainderPartition()
  {
    if (_config.AutoCreateForNewValues)
      throw new InvalidOperationException(
        "Cannot combine auto-create with remainder partition. A DEFAULT partition would swallow new values.");
    _config.HasRemainderPartition = true;
    return this;
  }

  public TablePartitionConfigBuilder WithSchema(string schema)
  {
    _config.Schema = schema;
    _config.SchemaExplicitlySet = true;
    if (_config.SubPartition != null)
    {
      ApplySchemaIfImplicit(_config.SubPartition, schema);
    }

    foreach (ListPartitionItemConfig listPartition in _config.ListPartitions)
    {
      if (listPartition.ChildPartition != null)
      {
        ApplySchemaIfImplicit(listPartition.ChildPartition, schema);
      }
    }

    return this;
  }

  public TablePartitionConfigBuilder CreateIfNotExists(string columns)
  {
    _config.CreateIfNotExistsColumns = columns;
    return this;
  }

  public TablePartitionConfigBuilder SubPartition(Action<TablePartitionConfigBuilder> configure)
  {
    TablePartitionConfigBuilder subBuilder = new TablePartitionConfigBuilder();
    configure(subBuilder);
    _config.SubPartition = subBuilder.Build();
    ApplySchemaIfImplicit(_config.SubPartition, _config.Schema);
    return this;
  }

  public TablePartitionConfigBuilder PreCreate(TimeSpan ahead)
  {
    _config.PreCreateAhead = ahead;
    return this;
  }

  internal TablePartitionConfig Build()
  {
    return _config;
  }

  private static string FormatListValue(object value)
  {
    if (value is null)
    {
      throw new InvalidOperationException("List partition values cannot contain null.");
    }

    if (value is DateTimeOffset dateTimeOffset)
    {
      return dateTimeOffset.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);
    }

    if (value is DateTime dateTime)
    {
      return dateTime.ToString("O", CultureInfo.InvariantCulture);
    }

    if (value is IFormattable formattable)
    {
      return formattable.ToString(null, CultureInfo.InvariantCulture);
    }

    return value.ToString()!;
  }

  private static void ApplySchemaIfImplicit(TablePartitionConfig config, string schema)
  {
    if (!config.SchemaExplicitlySet)
    {
      config.Schema = schema;
    }

    if (config.SubPartition != null)
    {
      ApplySchemaIfImplicit(config.SubPartition, schema);
    }

    foreach (ListPartitionItemConfig listPartition in config.ListPartitions)
    {
      if (listPartition.ChildPartition != null)
      {
        ApplySchemaIfImplicit(listPartition.ChildPartition, schema);
      }
    }
  }
}
