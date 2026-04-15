# ottyel

`ottyel` is a local OpenTelemetry workstation for the terminal. It ingests OTLP
over HTTP and gRPC, stores traces, logs, metrics, and LLM telemetry in SQLite,
and provides a keyboard-first TUI for investigation.

## Screenshot

![LLM Inspector](docs/screenshots/llm-inspector.png)

## Install

### Cargo

```bash
cargo install --locked --path .
```

### Local helper script

```bash
./scripts/install.sh
```

## Run

```bash
ottyel serve
```

Defaults:

- OTLP/HTTP: `127.0.0.1:4318`
- OTLP/gRPC: `127.0.0.1:4317`
- DB path: platform app-data directory, for example:
  - macOS: `~/Library/Application Support/ottyel/ottyel.db`

The install places `ottyel` in Cargo's bin directory, typically:

- `~/.cargo/bin`
