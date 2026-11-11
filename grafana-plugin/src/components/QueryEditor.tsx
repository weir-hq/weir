import React, { ChangeEvent, KeyboardEvent, useEffect, useState } from 'react';
import { css } from '@emotion/css';
import { GrafanaTheme2, QueryEditorProps } from '@grafana/data';
import { Field, TextArea, useStyles2 } from '@grafana/ui';

import { DataSource } from '../datasource';
import { WeirOptions, WeirQuery } from '../types';

type Props = QueryEditorProps<DataSource, WeirQuery, WeirOptions>;

const EXAMPLES = [
  'oldest({pipeline="$pipeline"})',
  'graph({pipeline="$pipeline"}[5m])',
  'leak({pipeline="$pipeline"}[15m])',
];

export function QueryEditor({ datasource, query, onChange, onRunQuery }: Props) {
  const styles = useStyles2(getStyles);
  const [functions, setFunctions] = useState<string[]>([]);

  useEffect(() => {
    datasource.functions().then(setFunctions, () => setFunctions([]));
  }, [datasource]);

  const onExprChange = (event: ChangeEvent<HTMLTextAreaElement>) => {
    onChange({ ...query, expr: event.target.value });
  };

  // Shift+Enter or Ctrl/Cmd+Enter runs the query.
  const onKeyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === 'Enter' && (event.shiftKey || event.ctrlKey || event.metaKey)) {
      event.preventDefault();
      onRunQuery();
    }
  };

  const description = (
    <>
      WeirQL:{' '}
      <code>
        function({'{'}label=&quot;value&quot;{'}'}[range])
      </code>
      . Series functions follow the dashboard time range; table and graph functions use <code>[range]</code> when given.
      Shift+Enter runs.
      {functions.length > 0 && <> Functions: {functions.join(', ')}.</>}
    </>
  );

  return (
    <Field label="Query" description={description}>
      <TextArea
        aria-label="WeirQL query"
        className={styles.query}
        rows={Math.max(2, (query.expr ?? '').split('\n').length)}
        value={query.expr ?? ''}
        placeholder={EXAMPLES.join('   ')}
        onChange={onExprChange}
        onBlur={onRunQuery}
        onKeyDown={onKeyDown}
        spellCheck={false}
      />
    </Field>
  );
}

const getStyles = (theme: GrafanaTheme2) => ({
  query: css({
    fontFamily: theme.typography.fontFamilyMonospace,
    fontSize: theme.typography.bodySmall.fontSize,
  }),
});
