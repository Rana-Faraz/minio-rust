# Test Layout

This repo keeps a checked-in Rust snapshot of the upstream MinIO Go test surface.

Layout:
- `tests/cmd.rs` loads checked-in files under `tests/cmd/`
- `tests/internal.rs` loads checked-in files under `tests/internal/`
- `tests/support/mod.rs` holds shared test helpers

Important:
- `cargo test` does not read `.refrences/`
- the pushed repo can run this suite without the upstream Go checkout
- `.refrences/minio` is only a local source mirror used when regenerating the checked-in Rust files

To refresh the checked-in Rust test tree from the local reference mirror:

```bash
python3 scripts/materialize_go_tests.py
```
