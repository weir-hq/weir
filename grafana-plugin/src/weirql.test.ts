import { formatValue, parseLabelValues } from './weirql';

describe('formatValue', () => {
  it('escapes quotes and backslashes for a WeirQL string', () => {
    expect(formatValue('a"b\\c')).toBe('a\\"b\\\\c');
  });

  it('turns several values into a regex alternation', () => {
    // a.b becomes a\.b as a regex, and a\\.b inside a WeirQL string.
    expect(formatValue(['a.b', 'c'])).toBe('(a\\\\.b|c)');
  });
});

describe('parseLabelValues', () => {
  it('reads a label alone', () => {
    expect(parseLabelValues('label_values(pipeline)')).toEqual({ label: 'pipeline', filters: {} });
  });

  it('reads filters', () => {
    expect(parseLabelValues('label_values(stage, pipeline="orders", x="a\\"b")')).toEqual({
      label: 'stage',
      filters: { pipeline: 'orders', x: 'a"b' },
    });
  });

  it('rejects anything else', () => {
    expect(parseLabelValues('stages()')).toBeUndefined();
    expect(parseLabelValues('label_values(stage, pipeline=orders)')).toBeUndefined();
  });
});
