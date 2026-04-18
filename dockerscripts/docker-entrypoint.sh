#!/bin/sh
#
set -eu

if [ -z "${1:-}" ] || { [ "${1:-}" = "minio" ] && [ "$#" -eq 1 ]; }; then
    if [ -n "${MINIO_VOLUMES:-}" ]; then
        if [ -n "${MINIO_OPTS:-}" ]; then
            # shellcheck disable=SC2086
            set -- minio server ${MINIO_OPTS} ${MINIO_VOLUMES}
        else
            # shellcheck disable=SC2086
            set -- minio server ${MINIO_VOLUMES}
        fi
    else
        if [ -n "${MINIO_OPTS:-}" ]; then
            # shellcheck disable=SC2086
            set -- minio server ${MINIO_OPTS} /data
        else
            set -- minio server /data
        fi
    fi
elif [ "${1:-}" != "minio" ]; then
    set -- minio "$@"
fi

docker_switch_user() {
    if [ -n "${MINIO_USERNAME:-}" ] && [ -n "${MINIO_GROUPNAME:-}" ]; then
        if [ -n "${MINIO_UID:-}" ] && [ -n "${MINIO_GID:-}" ]; then
            chroot --userspec="${MINIO_UID}:${MINIO_GID}" / "$@"
        else
            echo "${MINIO_USERNAME}:x:1000:1000:${MINIO_USERNAME}:/:/sbin/nologin" >>/etc/passwd
            echo "${MINIO_GROUPNAME}:x:1000" >>/etc/group
            chroot --userspec="${MINIO_USERNAME}:${MINIO_GROUPNAME}" / "$@"
        fi
    else
        exec "$@"
    fi
}

docker_switch_user "$@"
