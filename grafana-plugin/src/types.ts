import { DataSourceJsonData } from '@grafana/data';
import { DataQuery } from '@grafana/schema';

export interface WeirQuery extends DataQuery {
  // A WeirQL expression, e.g. oldest({pipeline="orders"}).
  expr: string;
}

export const DEFAULT_QUERY: Partial<WeirQuery> = {
  expr: '',
};

export interface WeirOptions extends DataSourceJsonData {}
