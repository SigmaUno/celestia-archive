# File archive durability

Successful `LocalObjectStore.Put` and `PutIfAbsent` calls sync the temporary
file before publishing its name, then sync the containing directory and every
ancestor through the filesystem root. This also persists newly created archive
roots and key directories. Existing ancestors are synced too, so a directory
left visible by an earlier failed sync is not assumed durable. This costs one
directory sync per path component per publication.

`Put` publishes by same-directory rename, preserving atomic manifest replacement
on filesystems that provide atomic rename. `PutIfAbsent` publishes by hard link,
preserving create-if-absent collision behavior. Neither method treats a sync
failure as success. A failed call can nevertheless leave the published object
visible. Before reporting an existing object, `Exists` syncs its containing
directory and ancestors, so automatic archive retries cannot reuse a segment
whose directory sync still fails. `PutIfAbsent` also completes these syncs before
returning `ErrObjectAlreadyExists`; sync failures return an error without that
sentinel, preventing callers from treating them as reusable collisions. These
paths assume objects were written by this backend, which syncs file contents
before publication, and the normal single-writer archive model. Cold reads
through `Get`, `Stat`, and `List` do not sync. Read-back alone does not establish
durability.
Temporary names can survive a crash without affecting the published object.

Live archiving and prune verification also check local manifest durability
before accepting its progress or issuing verified pruning evidence. This
includes archive runs with no new blocks, verification with previous evidence,
and pruning through the read-only local-store wrapper. A visible manifest left
by a failed replacement cannot bypass a persistent directory-sync failure.
Ordinary `LoadManifest`, verification for inspection, and cold serving still
only read; S3 receives no additional requests for these durability checks.

The implementation uses only Go's `os.File.Sync`, without ignoring unsupported
directory sync errors. Filesystems that reject directory synchronization cannot
successfully publish through this backend. On Darwin, Go attempts `F_FULLFSYNC`
and falls back to `fsync` for `ENOTSUP`; that fallback has weaker power-loss
guarantees. See [Go's Darwin implementation](https://go.dev/src/internal/poll/fd_fsync_darwin.go)
and [Linux fsync semantics](https://www.man7.org/linux/man-pages/man2/fsync.2.html).

Durability depends on the operating system, filesystem, mount configuration,
and storage hardware honoring synchronization requests. Network filesystems,
volatile devices, and hardware that misreports flush completion cannot be made
power-loss safe by these calls. Tests check synchronization order and error
propagation on the test filesystem; they do not simulate a power failure.
Concurrent external directory or symlink mutation is outside these guarantees.
