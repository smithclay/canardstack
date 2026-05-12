# Repository Instructions

## Git Commit Messages

All commits in this repository must use Conventional Commits.

Use the form:

```text
type(optional-scope): imperative summary
```

Common types include `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, and `chore`.

Examples:

```text
feat(ingest): queue Arrow batches for metrics
perf(storage): append RecordBatches through DuckDB
docs: document commit message convention
```

Keep the subject concise, imperative, and lowercase after the type/scope unless it names a proper noun or API.
