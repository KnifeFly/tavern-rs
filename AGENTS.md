# Repository Guidelines

## Project Structure & Module Organization
This repository is the Rust rewrite of Tavern. Core code lives in `src/`:
- `src/main.rs` is the CLI entry point, `src/lib.rs` exposes shared modules.
- `src/server.rs` handles HTTP proxying and cache behavior.
- `src/cache.rs`, `src/upstream.rs`, and `src/http_range.rs` implement storage, upstream fetches, and range parsing.
- `src/config.rs`, `src/logging.rs`, `src/constants.rs`, and `src/runtime.rs` hold config, logging, headers, and build metadata.
Integration tests live in `tests/` (e.g., `tests/range.rs`, `tests/filechanged.rs`) with helpers in `tests/support/`.

## Build, Test, and Development Commands
- `cargo build` — compile the server and library.
- `cargo run -- -c config.yaml` — run with a config file (default is `config.yaml` in the working directory).
- `cargo test --tests` — run integration tests.
- `cargo test --test range` — run a single test file.
- `cargo fmt` and `cargo clippy --all-targets --all-features` — format and lint before submitting.

## Coding Style & Naming Conventions
- Use rustfmt defaults (4-space indentation).
- Naming: `snake_case` for functions/modules, `CamelCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- Keep modules small and focused; prefer explicit exports in `src/lib.rs`.

## Testing Guidelines
- Use Rust’s built-in test framework (tokio for async tests).
- Add new integration tests in `tests/` and name files after features (e.g., `cache_method.rs`).
- E2E-style tests should use the shared harness in `tests/support/` to spin up a mock upstream.

## Commit & Pull Request Guidelines
- This working copy has no git history, so no existing commit convention is available.
- Use clear, imperative commit messages (or Conventional Commits if you prefer).
- PRs should include a concise summary, tests run, and any config or behavior changes.

## Configuration Notes
- The server reads YAML via `-c/--config` and validates required fields (`server.addr`, `upstream.address`).
- Unknown fields are logged unless `strict: true` in the config.
