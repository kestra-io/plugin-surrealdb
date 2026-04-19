# Kestra SurrealDB Plugin

## What

- Provides plugin components under `io.kestra.plugin.surrealdb`.
- Includes classes such as `Trigger`, `SurrealDBConnection`, `Query`.

## Why

- What user problem does this solve? Teams need to run SurrealQL queries against SurrealDB from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps SurrealDB steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on SurrealDB.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `surrealdb`

Infrastructure dependencies (Docker Compose services):

- `surrealdb`

### Key Plugin Classes

- `io.kestra.plugin.surrealdb.Query`
- `io.kestra.plugin.surrealdb.Trigger`

### Project Structure

```
plugin-surrealdb/
├── src/main/java/io/kestra/plugin/surrealdb/
├── src/test/java/io/kestra/plugin/surrealdb/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
