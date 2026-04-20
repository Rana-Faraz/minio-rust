# minio-rust

`minio-rust` is an experimental Rust port of the MinIO object storage server. This repository is an independent community project and is not an official MinIO, Inc. repository.

## Status

- Early-stage and under active development.
- Intended to mirror MinIO server behavior where practical.
- Local and local-erasure style workflows are available today.
- Distributed URL pool deployments are not implemented yet.

## Quick start

Run the server directly:

```sh
cargo run -- server --address ":9000" --console-address ":9001" /data
```

Build and run the containerized variant:

```sh
docker compose up --build
```

The Docker image and entrypoint support the common standalone MinIO-style environment variables documented in [docs/docker.md](docs/docker.md).

## Development

Format and test locally before opening a pull request:

```sh
cargo fmt
cargo test
```

The repository includes a checked-in Rust test tree under `tests/` so contributors do not need a separate upstream Go checkout just to run the suite.

## Open source project files

- [LICENSE](LICENSE) contains the GNU Affero General Public License v3.0 text.
- [NOTICE](NOTICE) summarizes project copyright and attribution.
- [COMPLIANCE.md](COMPLIANCE.md) calls out AGPL compliance expectations.
- [CONTRIBUTING.md](CONTRIBUTING.md) explains the contribution workflow.
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) defines expected community behavior.
- [SECURITY.md](SECURITY.md) explains how to report vulnerabilities responsibly.

## License

This project is licensed under the GNU AGPL v3.0. If you modify and run this software for users over a network, the AGPL may require you to make the corresponding source available to those users. Review [COMPLIANCE.md](COMPLIANCE.md) and get qualified legal advice if you are unsure how the license applies to your use case.

This project is provided without warranty. See [LICENSE](LICENSE) for details.
