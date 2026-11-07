# Weir

The plan is [ROADMAP.md](ROADMAP.md).

- `make ci` runs what CI runs. Run it before committing.
- A metric or attribute name lives in three places: `conventions/*.yaml`,
  `sdk/python/src/weir/semconv.py` and `server/semconv.go`. Tests fail until they match.
- Check behavior changes against the demo's ground truth (`make truth`), not only unit tests.
- Keep code comments and docs plain and short.
