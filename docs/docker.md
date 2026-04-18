# Docker Compatibility

`minio-rust` now supports the main standalone/local-erasure container contract people use with MinIO:

```sh
docker run \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio-rust:minio server --address ":9000" --console-address ":9001" /data
```

It also supports:

- `MINIO_ACCESS_KEY_FILE`
- `MINIO_SECRET_KEY_FILE`
- `MINIO_ROOT_USER_FILE`
- `MINIO_ROOT_PASSWORD_FILE`
- `MINIO_CONFIG_ENV_FILE`
- `MINIO_VOLUMES`
- local ellipses expansion such as `/data{1...4}`
- hostless bind addresses such as `:9000` and `:9001`
- the deprecated Docker user-switch envs:
  - `MINIO_USERNAME`
  - `MINIO_GROUPNAME`
  - `MINIO_UID`
  - `MINIO_GID`

If the container is started with no explicit command, the entrypoint now defaults to:

```sh
minio server /data
```

If `MINIO_VOLUMES` is set, that value is used instead.
If `MINIO_OPTS` is set, it is injected into that default `server` command too.

The included [docker-compose.yml](../docker-compose.yml) uses the same command shape with local erasure-style disks:

```sh
docker compose up --build
```

Current limitation:

- distributed URL pools such as `http://minio{1...4}/data{1...2}` are rejected with a clear error because the distributed runtime is not implemented yet.
