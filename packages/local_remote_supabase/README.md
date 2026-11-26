# local_remote_supabase

[![pub.dev](https://img.shields.io/pub/v/local_remote_supabase.svg)](https://pub.dev/packages/local_remote_supabase)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

**Supabase** implementation of `RemoteSyncAdapter` for the
[local_remote_synchronization](../local_remote_synchronization) ecosystem.

---

## Responsibility

This package bridges the Core contract `RemoteSyncAdapter` with the
[supabase_flutter](https://pub.dev/packages/supabase_flutter) client.
It handles applying local mutations to a Supabase PostgreSQL backend
and fetching remote changes.

**It does not know which local database is being used.** The Core engine
supplies serialised `SyncOperation` payloads; this adapter submits them to
Supabase.

---

## Status

> 🗓 **Planned.** Not yet implemented.

---

## Implementation Checklist

- [ ] Implement `RemoteSyncAdapter` interface.
- [ ] Support insert / update / delete operations via Supabase PostgREST.
- [ ] Handle Supabase authentication and RLS token injection.
- [ ] Implement conflict-detection using `updated_at` timestamps or ETags.
- [ ] Write unit tests with mocked Supabase client.

---

## License

MIT — see [LICENSE](../../LICENSE).
