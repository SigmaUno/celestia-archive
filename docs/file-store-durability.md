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
visible; read-back alone does not establish durability, and a storage sync
failure must be resolved before permitting pruning or reusing those objects.
Temporary names can survive a crash without affecting the published object.

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
