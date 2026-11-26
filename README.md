# NDB — Distributed NoSQL Search Database

A distributed document database with integrated full-text, fuzzy, semantic and
hybrid search, written in Rust. See `dbns_spec.md` for the full product and
engineering specification.

## Status

Phase 0 (foundations) — see `docs/progress.md` for tracked progress against
the spec.

## Build & test

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets
cargo fmt --all
```

## Run

```sh
cp ndb.example.yaml ndb.yaml
cargo run -p ndb-server -- --config ndb.yaml
```

The Prometheus scrape endpoint is served on `monitoring.metrics_port` (default
`9091`).

## Layout

- `crates/ndb-*` — subsystem crates (spec §19); only the Phase 0–4 crates are
  scaffolded so far (spec §29)
- `docs/adr/` — architecture decision records (spec §28)
- `docs/progress.md` — implementation progress tracker
- `ndb.example.yaml` — annotated server configuration (spec §25)
