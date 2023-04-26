import type { Expression } from '../../sequelize.js';
import { joinSQLFragments } from '../../utils/join-sql-fragments';
import { generateIndexName } from '../../utils/string';
import { AbstractQueryGenerator } from '../abstract/query-generator';
import type { EscapeOptions, RemoveIndexQueryOptions, TableNameOrModel } from '../abstract/query-generator-typescript';
import type { TableName } from '../abstract/query-interface.js';
import { ENUM } from './data-types';

/**
 * Temporary class to ease the TypeScript migration
 */
export class PostgresQueryGeneratorTypeScript extends AbstractQueryGenerator {
  describeTableQuery(tableName: TableNameOrModel) {
    const table = this.extractTableDetails(tableName);

    return joinSQLFragments([
      'SELECT',
      'pk.constraint_type as "Constraint",',
      'c.column_name as "Field",',
      'c.column_default as "Default",',
      'c.is_nullable as "Null",',
      `(CASE WHEN c.udt_name = 'hstore' THEN c.udt_name ELSE c.data_type END) || (CASE WHEN c.character_maximum_length IS NOT NULL THEN '(' || c.character_maximum_length || ')' ELSE '' END) as "Type",`,
      '(SELECT array_agg(e.enumlabel) FROM pg_catalog.pg_type t JOIN pg_catalog.pg_enum e ON t.oid=e.enumtypid WHERE t.typname=c.udt_name) AS "special",',
      '(SELECT pgd.description FROM pg_catalog.pg_statio_all_tables AS st INNER JOIN pg_catalog.pg_description pgd on (pgd.objoid=st.relid) WHERE c.ordinal_position=pgd.objsubid AND c.table_name=st.relname) AS "Comment"',
      'FROM information_schema.columns c',
      'LEFT JOIN (SELECT tc.table_schema, tc.table_name,',
      'cu.column_name, tc.constraint_type',
      'FROM information_schema.TABLE_CONSTRAINTS tc',
      'JOIN information_schema.KEY_COLUMN_USAGE  cu',
      'ON tc.table_schema=cu.table_schema and tc.table_name=cu.table_name',
      'and tc.constraint_name=cu.constraint_name',
      `and tc.constraint_type='PRIMARY KEY') pk`,
      'ON pk.table_schema=c.table_schema',
      'AND pk.table_name=c.table_name',
      'AND pk.column_name=c.column_name',
      `WHERE c.table_name = ${this.escape(table.tableName)}`,
      `AND c.table_schema = ${this.escape(table.schema)}`,
    ]);
  }

  showIndexesQuery(tableName: TableNameOrModel) {
    const table = this.extractTableDetails(tableName);

    // TODO [>=6]: refactor the query to use pg_indexes
    return joinSQLFragments([
      'SELECT i.relname AS name, ix.indisprimary AS primary, ix.indisunique AS unique, ix.indkey AS indkey,',
      'array_agg(a.attnum) as column_indexes, array_agg(a.attname) AS column_names, pg_get_indexdef(ix.indexrelid)',
      'AS definition FROM pg_class t, pg_class i, pg_index ix, pg_attribute a , pg_namespace s',
      'WHERE t.oid = ix.indrelid AND i.oid = ix.indexrelid AND a.attrelid = t.oid AND',
      `t.relkind = 'r' and t.relname = ${this.escape(table.tableName)}`,
      `AND s.oid = t.relnamespace AND s.nspname = ${this.escape(table.schema)}`,
      'GROUP BY i.relname, ix.indexrelid, ix.indisprimary, ix.indisunique, ix.indkey ORDER BY i.relname;',
    ]);
  }

  removeIndexQuery(
    tableName: TableNameOrModel,
    indexNameOrAttributes: string | string[],
    options?: RemoveIndexQueryOptions,
  ) {
    if (options?.cascade && options?.concurrently) {
      throw new Error(`Cannot specify both concurrently and cascade options in removeIndexQuery for ${this.dialect.name} dialect`);
    }

    let indexName;
    const table = this.extractTableDetails(tableName);
    if (Array.isArray(indexNameOrAttributes)) {
      indexName = generateIndexName(table, { fields: indexNameOrAttributes });
    } else {
      indexName = indexNameOrAttributes;
    }

    return joinSQLFragments([
      'DROP INDEX',
      options?.concurrently ? 'CONCURRENTLY' : '',
      options?.ifExists ? 'IF EXISTS' : '',
      `${this.quoteIdentifier(table.schema!)}.${this.quoteIdentifier(indexName)}`,
      options?.cascade ? 'CASCADE' : '',
    ]);
  }

  jsonPathExtractionQuery(sqlExpression: string, path: ReadonlyArray<number | string>, unquote: boolean): string {
    const operator = path.length === 1
      ? (unquote ? '->>' : '->')
      : (unquote ? '#>>' : '#>');

    const pathSql = path.length === 1
      // when accessing an array index with ->, the index must be a number
      // when accessing an object key with ->, the key must be a string
      ? this.escape(path[0])
      // when accessing with #>, the path is always an array of strings
      : this.escape(path.map(value => String(value)));

    return sqlExpression + operator + pathSql;
  }

  formatUnquoteJson(arg: Expression, options?: EscapeOptions) {
    return `${this.escape(arg, options)}#>>ARRAY[]::TEXT[]`;
  }

  pgEnumName(
    tableName: TableName,
    columnName?: string | undefined,
    customName?: string | undefined,
    options: PgEnumNameOptions = {},
  ) {
    const tableDetails = this.extractTableDetails(tableName, options);

    let prefixedEnumName;
    if (customName == null) {
      prefixedEnumName = `enum_${tableDetails.tableName}_${columnName}`;
    } else {
      prefixedEnumName = `enum_${customName}`;
    }

    if (options.noEscape) {
      return prefixedEnumName;
    }

    const escapedEnumName = this.quoteIdentifier(prefixedEnumName);

    if (Boolean(options.schema) !== false && tableDetails.schema) {
      return this.quoteIdentifier(tableDetails.schema) + (tableDetails.delimiter ?? '.') + escapedEnumName;
    }

    return escapedEnumName;
  }

  pgListEnums(tableName: TableName, attrName: string, customName: string | undefined, options?: PgEnumNameOptions): string;
  pgListEnums(tableName: TableName, attrName?: undefined, customName?: undefined, options?: PgEnumNameOptions): string;
  pgListEnums(
    tableName: TableName,
    attrName?: string | undefined,
    customName?: string | undefined,
    options?: PgEnumNameOptions,
  ) {
    let enumName = '';
    let schema: string | undefined;

    if (tableName != null) {
      const tableDetails = this.extractTableDetails(tableName, options);
      if (tableDetails.schema) {
        schema = tableDetails.schema;
      } else if (attrName) {
        // pgEnumName escapes as an identifier, we want to escape it as a string
        enumName = ` AND t.typname=${this.escape(this.pgEnumName(tableDetails.tableName, attrName, customName, { noEscape: true }))}`;
      }
    }

    if (!schema) {
      schema = this.options.schema || this.dialect.getDefaultSchema();
    }

    return 'SELECT t.typname enum_name, array_agg(e.enumlabel ORDER BY enumsortorder) enum_value FROM pg_type t '
      + 'JOIN pg_enum e ON t.oid = e.enumtypid '
      + 'JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace '
      + `WHERE n.nspname = ${this.escape(schema)}${enumName} GROUP BY 1`;
  }

  pgEnum(
    tableName: TableName,
    attr: string,
    dataType: ENUM<never>,
    options?: PgEnumNameOptions & { force?: boolean },
  ) {
    const enumName = this.pgEnumName(tableName, attr, dataType?.options?.customName, options);
    let values;

    if (dataType instanceof ENUM && dataType.options.values) {
      values = `ENUM(${dataType.options.values.map(value => this.escape(value)).join(', ')})`;
    } else {
      const match = dataType.toString().match(/^ENUM\(.+\)/);
      if (match) {
        values = match[0];
      } else {
        throw new Error(`Invalid ENUM type: ${dataType}`);
      }
    }

    let sql = `DO ${this.escape(`BEGIN CREATE TYPE ${enumName} AS ${values}; EXCEPTION WHEN duplicate_object THEN null; END`)};`;
    if (options && options.force === true) {
      sql = this.pgEnumDrop(tableName, attr, enumName) + sql;
    }

    return sql;
  }

  pgEnumAdd(
    tableName: TableName,
    attr: string,
    value: string,
    options: any,
    customName?: string | undefined,
  ) {
    const enumName = this.pgEnumName(tableName, attr, customName, {});
    let sql = `ALTER TYPE ${enumName} ADD VALUE IF NOT EXISTS `;

    sql += this.escape(value);

    if (options.before) {
      sql += ` BEFORE ${this.escape(options.before)}`;
    } else if (options.after) {
      sql += ` AFTER ${this.escape(options.after)}`;
    }

    return sql;
  }

  pgEnumDrop(tableName: TableName, attr?: string | undefined, enumName?: string | undefined) {
    enumName = enumName || this.pgEnumName(tableName, attr);

    return `DROP TYPE IF EXISTS ${enumName}; `;
  }

  dataTypeMapping(
    tableName: TableName,
    attr: string,
    dataType: string,
    options: { enumCustomName?: string | undefined } = {},
  ) {
    if (dataType.includes('PRIMARY KEY')) {
      dataType = dataType.replace('PRIMARY KEY', '');
    }

    if (dataType.includes('SERIAL')) {
      if (dataType.includes('BIGINT')) {
        dataType = dataType.replace('SERIAL', 'BIGSERIAL');
        dataType = dataType.replace('BIGINT', '');
      } else if (dataType.includes('SMALLINT')) {
        dataType = dataType.replace('SERIAL', 'SMALLSERIAL');
        dataType = dataType.replace('SMALLINT', '');
      } else {
        dataType = dataType.replace('INTEGER', '');
      }

      dataType = dataType.replace('NOT NULL', '');
    }

    if (dataType.startsWith('ENUM(')) {
      dataType = dataType.replace(/^ENUM\(.+\)/, this.pgEnumName(tableName, attr, options.enumCustomName)); // TODO fix pgEnumName
    }

    return dataType;
  }
}

type PgEnumNameOptions = {
  schema?: string,
  noEscape?: boolean,
  delimeter?: string,
};
