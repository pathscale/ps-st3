# Local prepublish checklist

Before manually dispatching `Publish crate` from `main`, review the change
and run the applicable local checks below, including
`cargo publish --dry-run`. The workflow publishes the version declared in
`Cargo.toml`; it does not run the test suite remotely.

The existing manual CI requirements are:

- Check the default build on stable and Rust 1.60 with `cargo check`.
- Check OS parking without `std` using
  `cargo check --no-default-features --features atomic-host`.
- Run `cargo test --release`, then `cargo test --release --features host` for
  the `fanout` pool and host-backed tests.
- Run the no-`std` atomic host test with
  `cargo test --release --no-default-features --features atomic-host --test atomic-host`.
- Run Loom with `LOOM_MAX_PREEMPTIONS=3 RUSTFLAGS='--cfg st3_loom' cargo test --tests --release`.
- Run `cargo fmt --all -- --check`, `cargo clippy`, and
  `RUSTDOCFLAGS='-Dwarnings' cargo doc --no-deps --document-private-items`.

If a code change affects queue algorithms, unsafe transfer paths, or fanout
wake behavior, run the Loom checks against that change before publishing.
