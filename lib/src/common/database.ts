/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  QueryArrayConfig,
  QueryArrayResult,
  QueryConfig,
  QueryResult,
  QueryResultRow,
} from 'pg';

/**
 * This interface combines the used query functions from the 'pg' library
 * `Pool`, `Client`, `ClientBase`, and `PoolClient`.
 */
export interface DatabaseClient {
  query<R extends any[] = any[], I extends any[] = any[]>(
    queryConfig: QueryArrayConfig<I>,
    values?: I,
  ): Promise<QueryArrayResult<R>>;
  query<R extends QueryResultRow = any, I extends any[] = any[]>(
    queryConfig: QueryConfig<I>,
  ): Promise<QueryResult<R>>;
  query<R extends QueryResultRow = any, I extends any[] = any[]>(
    queryTextOrConfig: string | QueryConfig<I>,
    values?: I,
  ): Promise<QueryResult<R>>;
  query<R extends any[] = any[], I extends any[] = any[]>(
    queryConfig: QueryArrayConfig<I>,
    callback: (err: Error, result: QueryArrayResult<R>) => void,
  ): void;
  query<R extends QueryResultRow = any, I extends any[] = any[]>(
    queryTextOrConfig: string | QueryConfig<I>,
    callback: (err: Error, result: QueryResult<R>) => void,
  ): void;
  query<R extends QueryResultRow = any, I extends any[] = any[]>(
    queryText: string,
    values: any[],
    callback: (err: Error, result: QueryResult<R>) => void,
  ): void;

  /** Available from the PoolClient */
  release?(err?: Error | boolean): void;
}
