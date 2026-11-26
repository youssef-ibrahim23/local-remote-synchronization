# local_remote_rest

[![pub.dev](https://img.shields.io/pub/v/local_remote_rest.svg)](https://pub.dev/packages/local_remote_rest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

**HTTP REST** implementation of `RemoteSyncAdapter` for the
[local_remote_synchronization](../local_remote_synchronization) ecosystem.

---

## Responsibility

This package bridges the Core contract `RemoteSyncAdapter` with any HTTP REST
backend via the `http` package. It translates `SyncOperation` payloads into
standard HTTP verbs (POST / PATCH / DELETE) and handles response mapping.

**It does not know which local database is being used.**

---

## Status

> 🗓 **Planned.** Not yet implemented.

---

## Implementation Checklist

- [ ] Implement `RemoteSyncAdapter` interface.
- [ ] Support configurable base URL and authentication headers.
- [ ] Map `SyncOperationType` to HTTP verbs.
- [ ] Handle HTTP error codes and map to `SyncError`.
- [ ] Support ETag / `If-Match` conflict detection headers.
- [ ] Write unit tests with mocked `http.Client`.

---

## License

MIT — see [LICENSE](../../LICENSE).
