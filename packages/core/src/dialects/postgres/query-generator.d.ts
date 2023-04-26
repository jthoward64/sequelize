import type { TableName } from '../abstract/query-interface.js';
import { PostgresQueryGeneratorTypeScript } from './query-generator-typescript.js';

type PgEnumNameOptions = {
  schema?: boolean,
};

export class PostgresQueryGenerator extends PostgresQueryGeneratorTypeScript {
  pgEnumName(tableName: TableName, columnName: string, customName?: string | undefined, options?: PgEnumNameOptions): string;

  pgListEnums(tableName: TableName, attrName: string, customName: string | undefined, options?: PgEnumNameOptions): string;
  pgListEnums(tableName: TableName, attrName?: undefined, customName?: undefined, options?: PgEnumNameOptions): string;

  pgEnum(tableName: TableName, attr: string, dataType: unknown, options?: PgEnumNameOptions): string;
}
