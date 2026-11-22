The rules, fixtures, validation scripts, and dataset generator come from the public
Shopping Sessions buildathon track. The rules and generator were refreshed for
scorer v5 on November 21, 2026 (America/Chicago); the unchanged fixture archive was
also checked against the current download:
https://www.joinbuilders.org/buildathon/tracks/shopping-sessions

- `rules.md`: official rules.
- `tests/fixtures/`: official starter-polars fixtures, unmodified.
- `scripts/compare.py`, `scripts/run_fixtures.py`, `scripts/docker_run.sh`: official validation scripts and container runner, unmodified.
- `tools/generator/`: official generator source, unmodified.

The implementation in `src/` and build configuration are original to this project.
