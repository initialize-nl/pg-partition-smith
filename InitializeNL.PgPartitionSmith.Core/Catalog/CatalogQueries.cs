using Dapper;
using Npgsql;
using InitializeNL.PgPartitionSmith.Core.Models;

namespace InitializeNL.PgPartitionSmith.Core.Catalog;

public static class CatalogQueries
{
  public static async Task<bool> TableExistsAsync(
    NpgsqlConnection connection, string tableName, string schema)
  {
    int count = await connection.QuerySingleAsync<int>(
      @"SELECT count(*)
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = @tableName
        AND n.nspname = @schema",
      new { tableName, schema });
    return count > 0;
  }

  public static async Task<bool> IsPartitionedTableAsync(
    NpgsqlConnection connection, string tableName, string schema)
  {
    string? partStrategy = await connection.QuerySingleOrDefaultAsync<string>(
      @"SELECT CASE pt.partstrat
          WHEN 'r' THEN 'range'
          WHEN 'l' THEN 'list'
          WHEN 'h' THEN 'hash'
        END
        FROM pg_catalog.pg_partitioned_table pt
        JOIN pg_catalog.pg_class c ON c.oid = pt.partrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = @tableName
        AND n.nspname = @schema",
      new { tableName, schema });
    return partStrategy != null;
  }

  public static async Task<string?> GetPartitionStrategyAsync(
    NpgsqlConnection connection, string tableName, string schema)
  {
    return await connection.QuerySingleOrDefaultAsync<string>(
      @"SELECT CASE pt.partstrat
          WHEN 'r' THEN 'range'
          WHEN 'l' THEN 'list'
          WHEN 'h' THEN 'hash'
        END
        FROM pg_catalog.pg_partitioned_table pt
        JOIN pg_catalog.pg_class c ON c.oid = pt.partrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = @tableName
        AND n.nspname = @schema",
      new { tableName, schema });
  }

  public static async Task<IEnumerable<string>> GetExistingPartitionNamesAsync(
    NpgsqlConnection connection, string parentTableName, string schema)
  {
    return await connection.QueryAsync<string>(
      @"SELECT c.relname
        FROM pg_catalog.pg_inherits i
        JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid
        JOIN pg_catalog.pg_class p ON p.oid = i.inhparent
        JOIN pg_catalog.pg_namespace n ON n.oid = p.relnamespace
        WHERE p.relname = @parentTableName
        AND n.nspname = @schema",
      new { parentTableName, schema });
  }

  public static async Task<IEnumerable<ColumnInfo>> GetColumnInfoAsync(
    NpgsqlConnection connection, string tableName, string schema)
  {
    return await connection.QueryAsync<ColumnInfo>(
      @"SELECT a.attname ""ColumnName"",
           format_type(a.atttypid, a.atttypmod) ""FullType"",
           a.attnotnull ""IsNotNull"",
           pg_get_expr(d.adbin, d.adrelid) ""ColumnDefault"",
           a.attnum ""OrdinalPosition""
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE c.relname = @tableName
        AND n.nspname = @schema
        AND a.attnum > 0
        AND NOT a.attisdropped
        ORDER BY a.attnum",
      new { tableName, schema });
  }

  public static async Task<IEnumerable<string>> GetUniqueConstraintNamesAsync(
    NpgsqlConnection connection, string tableName, string schema)
  {
    return await connection.QueryAsync<string>(
      @"SELECT con.conname
        FROM pg_catalog.pg_constraint con
        JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = @tableName
        AND n.nspname = @schema
        AND con.contype IN ('p', 'u')",
      new { tableName, schema });
  }

  public static async Task<IEnumerable<string>> GetConstraintColumnNamesAsync(
    NpgsqlConnection connection, string constraintName, string schema)
  {
    return await connection.QueryAsync<string>(
      @"SELECT a.attname
        FROM pg_catalog.pg_constraint con
        JOIN pg_catalog.pg_namespace n ON n.oid = con.connamespace
        CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
        JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
        WHERE con.conname = @constraintName
        AND n.nspname = @schema
        ORDER BY k.ord",
      new { constraintName, schema });
  }
}
