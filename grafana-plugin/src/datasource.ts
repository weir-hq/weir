import { CoreApp, DataSourceInstanceSettings, MetricFindValue, ScopedVars } from '@grafana/data';
import { DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';

import { DEFAULT_QUERY, WeirOptions, WeirQuery } from './types';
import { formatValue, parseLabelValues } from './weirql';

export class DataSource extends DataSourceWithBackend<WeirQuery, WeirOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<WeirOptions>) {
    super(instanceSettings);
  }

  getDefaultQuery(_: CoreApp): Partial<WeirQuery> {
    return DEFAULT_QUERY;
  }

  filterQuery(query: WeirQuery): boolean {
    return !query.hide && !!query.expr?.trim();
  }

  applyTemplateVariables(query: WeirQuery, scopedVars: ScopedVars): WeirQuery {
    return { ...query, expr: interpolate(query.expr ?? '', scopedVars) };
  }

  // Dashboard variables: label_values(stage) or label_values(stage, pipeline="orders").
  async metricFindQuery(query: string, options?: { scopedVars?: ScopedVars }): Promise<MetricFindValue[]> {
    const parsed = parseLabelValues(interpolate(query, options?.scopedVars ?? {}));
    if (!parsed) {
      throw new Error('Use label_values(label) or label_values(label, other="value", ...)');
    }
    const values: string[] = await this.getResource(
      `labels/${encodeURIComponent(parsed.label)}/values`,
      parsed.filters
    );
    return values.map((text) => ({ text }));
  }

  // Query function names, for the query editor's help line.
  functions(): Promise<string[]> {
    return this.getResource('functions');
  }
}

export function interpolate(expr: string, scopedVars: ScopedVars): string {
  return getTemplateSrv().replace(expr, scopedVars, formatValue);
}
