package archive

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

var (
	ErrObjectNotFound      = errors.New("object not found")
	ErrObjectAlreadyExists = errors.New("object already exists")
)

type ObjectInfo struct {
	Key  string
	Size int64
}

type ObjectStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, data []byte) error
	Exists(ctx context.Context, key string) (bool, error)
	Stat(ctx context.Context, key string) (ObjectInfo, error)
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
}

type ImmutableObjectStore interface {
	PutIfAbsent(ctx context.Context, key string, data []byte) error
}

// ETagImmutableObjectStore is an optional capability for object stores
// (currently only S3) that can return an ETag from PutIfAbsent. Callers can
// compare the returned ETag against a locally computed MD5 and skip an extra
// round-trip only on an exact match. Other ETag forms are treated as opaque.
type ETagImmutableObjectStore interface {
	ImmutableObjectStore
	PutIfAbsentReturningETag(ctx context.Context, key string, data []byte) (etag string, err error)
}

// ETagPutter is the analogous capability for unconditional Put. Used when an
// object already-existed path needs revalidation but the store may also
// expose ETag verification.
type ETagPutter interface {
	PutReturningETag(ctx context.Context, key string, data []byte) (etag string, err error)
}

func OpenObjectStore(url string) (ObjectStore, error) {
	if err := ValidateObjectStoreURL(url); err != nil {
		return nil, err
	}
	if strings.HasPrefix(url, "file://") {
		return NewLocalObjectStore(strings.TrimPrefix(url, "file://"))
	}
	if strings.HasPrefix(url, "s3://") {
		return NewS3ObjectStore(context.Background(), url)
	}
	return nil, fmt.Errorf("unsupported object store URL %q: supported schemes are file:// and s3://", url)
}

func OpenObjectStoreReadOnly(url string) (ObjectStore, error) {
	if err := ValidateObjectStoreURL(url); err != nil {
		return nil, err
	}
	if strings.HasPrefix(url, "file://") {
		store, err := NewLocalObjectStoreReadOnly(strings.TrimPrefix(url, "file://"))
		if err != nil {
			return nil, err
		}
		return readOnlyObjectStore{store: store}, nil
	}
	if strings.HasPrefix(url, "s3://") {
		store, err := NewS3ObjectStore(context.Background(), url)
		if err != nil {
			return nil, err
		}
		return readOnlyObjectStore{store: store}, nil
	}
	return nil, fmt.Errorf("unsupported object store URL %q: supported schemes are file:// and s3://", url)
}

type readOnlyObjectStore struct {
	store ObjectStore
}

func (s readOnlyObjectStore) Get(ctx context.Context, key string) ([]byte, error) {
	return s.store.Get(ctx, key)
}

func (readOnlyObjectStore) Put(context.Context, string, []byte) error {
	return errors.New("object store is read-only")
}

func (s readOnlyObjectStore) Exists(ctx context.Context, key string) (bool, error) {
	return s.store.Exists(ctx, key)
}

func (s readOnlyObjectStore) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	return s.store.Stat(ctx, key)
}

func (s readOnlyObjectStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	return s.store.List(ctx, prefix)
}

func ValidateObjectStoreURL(rawURL string) error {
	if rawURL == "" {
		return errors.New("object store URL is required")
	}
	if strings.HasPrefix(rawURL, "file://") {
		if strings.TrimPrefix(rawURL, "file://") == "" {
			return errors.New("file object store URL requires a root path")
		}
		return nil
	}
	if strings.HasPrefix(rawURL, "s3://") {
		_, err := parseS3StoreURL(rawURL)
		return err
	}
	return fmt.Errorf("unsupported object store URL %q: supported schemes are file:// and s3://", rawURL)
}

func ValidateObjectKey(key string) error {
	if key == "" {
		return errors.New("object key is required")
	}
	return validateObjectPath("object key", key)
}

func ValidateObjectPrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	pathValue := strings.TrimRight(prefix, "/")
	if pathValue == "" {
		return fmt.Errorf("invalid object prefix %q", prefix)
	}
	return validateObjectPath("object prefix", pathValue)
}

func validateObjectPath(label string, value string) error {
	if strings.Contains(value, "\\") {
		return fmt.Errorf("invalid %s %q", label, value)
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("invalid %s %q", label, value)
		}
	}
	clean := filepath.Clean(filepath.FromSlash(value))
	if clean == "." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || clean == ".." || filepath.IsAbs(clean) {
		return fmt.Errorf("invalid %s %q", label, value)
	}
	return nil
}

type LocalObjectStore struct {
	root string
}

func NewLocalObjectStore(root string) (*LocalObjectStore, error) {
	if root == "" {
		return nil, errors.New("local object store root is required")
	}
	if err := rejectLocalObjectStoreRootSymlink(root); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &LocalObjectStore{root: root}, nil
}

func NewLocalObjectStoreReadOnly(root string) (*LocalObjectStore, error) {
	if root == "" {
		return nil, errors.New("local object store root is required")
	}
	if err := rejectLocalObjectStoreRootSymlink(root); err != nil {
		return nil, err
	}
	return &LocalObjectStore{root: root}, nil
}

func (s *LocalObjectStore) Get(_ context.Context, key string) ([]byte, error) {
	path, err := s.pathForKey(key)
	if err != nil {
		return nil, err
	}
	if symlinkErr := rejectLocalObjectSymlink(s.root, path); symlinkErr != nil {
		return nil, symlinkErr
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrObjectNotFound
	}
	return data, err
}

func (s *LocalObjectStore) Put(_ context.Context, key string, data []byte) error {
	path, err := s.pathForKey(key)
	if err != nil {
		return err
	}
	if symlinkErr := rejectLocalObjectSymlink(s.root, filepath.Dir(path)); symlinkErr != nil {
		return symlinkErr
	}
	mkdirErr := os.MkdirAll(filepath.Dir(path), 0o755)
	if mkdirErr != nil {
		return mkdirErr
	}
	if symlinkErr := rejectLocalObjectSymlink(s.root, filepath.Dir(path)); symlinkErr != nil {
		return symlinkErr
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".put-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := io.Copy(tmp, bytes.NewReader(data)); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return syncLocalObjectDirectories(filepath.Dir(path))
}

func (s *LocalObjectStore) PutIfAbsent(_ context.Context, key string, data []byte) error {
	path, err := s.pathForKey(key)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if symlinkErr := rejectLocalObjectSymlink(s.root, dir); symlinkErr != nil {
		return symlinkErr
	}
	if mkdirErr := os.MkdirAll(dir, 0o755); mkdirErr != nil {
		return mkdirErr
	}
	if symlinkErr := rejectLocalObjectSymlink(s.root, dir); symlinkErr != nil {
		return symlinkErr
	}
	tmp, err := os.CreateTemp(dir, ".put-if-absent-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()
	if _, err := io.Copy(tmp, bytes.NewReader(data)); err != nil {
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Link(tmpName, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrObjectAlreadyExists
		}
		return err
	}
	return syncLocalObjectDirectories(dir)
}

// Sync every ancestor, including existing ones: an earlier failed publication
// may have left directory entries visible but not durable.
func syncLocalObjectDirectories(dir string) error {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	for {
		if err := syncLocalDirectory(dir); err != nil {
			return fmt.Errorf("sync local object directory %q: %w", dir, err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil
		}
		dir = parent
	}
}

var syncLocalDirectory = func(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func (s *LocalObjectStore) Exists(_ context.Context, key string) (bool, error) {
	path, err := s.pathForKey(key)
	if err != nil {
		return false, err
	}
	if symlinkErr := rejectLocalObjectSymlink(s.root, path); symlinkErr != nil {
		return false, symlinkErr
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return false, fmt.Errorf("local object %q is a symlink", key)
	}
	return true, nil
}

func (s *LocalObjectStore) Stat(_ context.Context, key string) (ObjectInfo, error) {
	path, err := s.pathForKey(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	if symlinkErr := rejectLocalObjectSymlink(s.root, path); symlinkErr != nil {
		return ObjectInfo{}, symlinkErr
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return ObjectInfo{}, ErrObjectNotFound
	}
	if err != nil {
		return ObjectInfo{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return ObjectInfo{}, fmt.Errorf("local object %q is a symlink", key)
	}
	return ObjectInfo{Key: key, Size: info.Size()}, nil
}

func (s *LocalObjectStore) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	if err := ValidateObjectPrefix(prefix); err != nil {
		return nil, err
	}
	var infos []ObjectInfo
	if err := filepath.WalkDir(s.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		validKey := ValidateObjectKey(key) == nil
		if !validKey {
			return nil
		}
		if !objectKeyMatchesListPrefix(key, prefix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		infos = append(infos, ObjectInfo{Key: key, Size: info.Size()})
		return nil
	}); err != nil {
		return nil, err
	}
	slices.SortFunc(infos, func(a, b ObjectInfo) int {
		return cmp.Compare(a.Key, b.Key)
	})
	return infos, nil
}

func (s *LocalObjectStore) pathForKey(key string) (string, error) {
	if err := ValidateObjectKey(key); err != nil {
		return "", err
	}
	clean := filepath.Clean(filepath.FromSlash(key))
	return filepath.Join(s.root, clean), nil
}

func rejectLocalObjectSymlink(root, path string) error {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}
	current := root
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("local object path %q contains symlink %q", path, current)
		}
	}
	return nil
}

func rejectLocalObjectStoreRootSymlink(root string) error {
	info, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("local object store root %q is a symlink", root)
	}
	return nil
}

func objectKeyMatchesListPrefix(key, prefix string) bool {
	if prefix == "" {
		return true
	}
	if strings.HasSuffix(prefix, "/") {
		return strings.HasPrefix(key, prefix)
	}
	return key == prefix || strings.HasPrefix(key, prefix+"/")
}
