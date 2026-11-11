import React, { ChangeEvent } from 'react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { Field, Input } from '@grafana/ui';

import { WeirOptions } from '../types';

type Props = DataSourcePluginOptionsEditorProps<WeirOptions>;

export function ConfigEditor({ options, onOptionsChange }: Props) {
  const onURLChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({ ...options, url: event.target.value });
  };

  return (
    <Field
      label="Weir server URL"
      description="Where Grafana's server reaches the Weir server. Grafana makes the requests, not the browser."
      required
    >
      <Input
        id="config-editor-url"
        aria-label="Weir server URL"
        value={options.url}
        placeholder="http://localhost:4100"
        onChange={onURLChange}
        width={50}
      />
    </Field>
  );
}
