package archive

import (
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

const blockHashHexLen = 64

const (
	ManifestVersion = 1
	DefaultManifest = "manifest.json"
)

type Manifest struct {
	Version     int               `json:"version"`
	ChainID     string            `json:"chain_id"`
	FirstHeight int64             `json:"first_height"`
	LastHeight  int64             `json:"last_height"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	Segments    []SegmentManifest `json:"segments"`
}

type SegmentManifest struct {
	Key         string       `json:"key"`
	FirstHeight int64        `json:"first_height"`
	LastHeight  int64        `json:"last_height"`
	Compression string       `json:"compression"`
	SizeBytes   int64        `json:"size_bytes"`
	SHA256      string       `json:"sha256"`
	Blocks      []BlockIndex `json:"blocks"`
}

type BlockIndex struct {
	Height int64  `json:"height"`
	Hash   string `json:"hash"`
	Offset int64  `json:"offset,omitempty"`
}

func NewManifest(chainID string, segments []SegmentManifest) (Manifest, error) {
	now := time.Now().UTC()
	m := Manifest{
		Version:   ManifestVersion,
		ChainID:   chainID,
		CreatedAt: now,
		UpdatedAt: now,
		Segments:  append([]SegmentManifest(nil), segments...),
	}
	slices.SortFunc(m.Segments, func(a, b SegmentManifest) int {
		return cmp.Compare(a.FirstHeight, b.FirstHeight)
	})
	if len(m.Segments) > 0 {
		m.FirstHeight = m.Segments[0].FirstHeight
		m.LastHeight = m.Segments[len(m.Segments)-1].LastHeight
	}
	return m, m.Validate()
}

// AppendSegmentInPlace appends a new segment to the manifest without
// re-sorting or re-validating the existing segment list. It performs only
// the cheap, O(1) checks required to keep the manifest consistent: it
// validates the new segment in isolation, checks contiguity with the
// previous tail (FirstHeight == prior LastHeight + 1), and updates
// LastHeight / UpdatedAt while preserving FirstHeight / CreatedAt. The
// manifest is assumed to already be valid; callers building a manifest
// incrementally (e.g. live archiving) should use this helper instead of
// rebuilding the manifest from scratch after each segment upload.
func (m *Manifest) AppendSegmentInPlace(segment SegmentManifest, now time.Time) error {
	if m == nil {
		return errors.New("manifest is nil")
	}
	if m.Version == 0 {
		m.Version = ManifestVersion
	}
	if err := segment.Validate(); err != nil {
		return fmt.Errorf("segment: %w", err)
	}
	if len(m.Segments) == 0 {
		m.Segments = append(m.Segments, segment)
		m.FirstHeight = segment.FirstHeight
		m.LastHeight = segment.LastHeight
		if m.CreatedAt.IsZero() {
			m.CreatedAt = now
		}
		m.UpdatedAt = now
		return nil
	}
	prev := m.Segments[len(m.Segments)-1]
	if segment.FirstHeight != prev.LastHeight+1 {
		return fmt.Errorf("segment starts at %d, expected %d", segment.FirstHeight, prev.LastHeight+1)
	}
	if segment.Key == prev.Key {
		return fmt.Errorf("duplicate segment key %q", segment.Key)
	}
	m.Segments = append(m.Segments, segment)
	m.LastHeight = segment.LastHeight
	m.UpdatedAt = now
	return nil
}

func LoadManifest(ctx context.Context, store ObjectStore, key string) (Manifest, error) {
	if err := ValidateObjectKey(key); err != nil {
		return Manifest{}, fmt.Errorf("manifest key: %w", err)
	}
	if store == nil {
		return Manifest{}, errors.New("object store is required")
	}
	data, err := store.Get(ctx, key)
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, manifest.Validate()
}

// Archive progress and pruning authority require durable publication, even
// when a failed manifest write left a complete, readable file behind.
func loadDurableManifest(ctx context.Context, store ObjectStore, key string) (Manifest, error) {
	manifest, err := LoadManifest(ctx, store, key)
	if err != nil {
		return Manifest{}, err
	}
	if readOnly, ok := store.(readOnlyObjectStore); ok {
		store = readOnly.store
	}
	if local, ok := store.(*LocalObjectStore); ok {
		exists, err := local.Exists(ctx, key)
		if err != nil {
			return Manifest{}, err
		}
		if !exists {
			return Manifest{}, ErrObjectNotFound
		}
	}
	return manifest, nil
}

func SaveManifest(ctx context.Context, store ObjectStore, key string, manifest Manifest) error {
	if err := ValidateObjectKey(key); err != nil {
		return fmt.Errorf("manifest key: %w", err)
	}
	if store == nil {
		return errors.New("object store is required")
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return store.Put(ctx, key, data)
}

func (m Manifest) Validate() error {
	if m.Version != ManifestVersion {
		return fmt.Errorf("unsupported manifest version %d", m.Version)
	}
	if m.ChainID == "" {
		return errors.New("manifest chain_id is required")
	}
	if err := ValidateArchiveChainID(m.ChainID); err != nil {
		return err
	}
	if !m.CreatedAt.IsZero() && !m.UpdatedAt.IsZero() && m.UpdatedAt.Before(m.CreatedAt) {
		return errors.New("manifest updated_at must not be before created_at")
	}
	if len(m.Segments) == 0 {
		if m.FirstHeight != 0 || m.LastHeight != 0 {
			return errors.New("empty manifest must not define a height range")
		}
		return nil
	}
	if m.FirstHeight <= 0 || m.LastHeight < m.FirstHeight {
		return fmt.Errorf("invalid manifest height range %d-%d", m.FirstHeight, m.LastHeight)
	}
	expected := m.FirstHeight
	seen := map[string]struct{}{}
	for i, segment := range m.Segments {
		if err := segment.Validate(); err != nil {
			return fmt.Errorf("segment %d: %w", i, err)
		}
		if _, ok := seen[segment.Key]; ok {
			return fmt.Errorf("duplicate segment key %q", segment.Key)
		}
		seen[segment.Key] = struct{}{}
		if segment.FirstHeight != expected {
			return fmt.Errorf("segment %d starts at %d, expected %d", i, segment.FirstHeight, expected)
		}
		expected = segment.LastHeight + 1
	}
	if expected-1 != m.LastHeight {
		return fmt.Errorf("manifest last height %d does not match segments ending at %d", m.LastHeight, expected-1)
	}
	return nil
}

func ValidateArchiveChainID(chainID string) error {
	if chainID == "" {
		return errors.New("chain ID is required")
	}
	if strings.TrimSpace(chainID) != chainID {
		return fmt.Errorf("unsafe chain ID %q: leading or trailing whitespace", chainID)
	}
	if strings.ContainsAny(chainID, `/\`) {
		return fmt.Errorf("unsafe chain ID %q: path separators are not allowed", chainID)
	}
	if err := ValidateObjectKey(fmt.Sprintf("chains/%s/blocks", chainID)); err != nil {
		return fmt.Errorf("unsafe chain ID %q: %w", chainID, err)
	}
	return nil
}

func (s SegmentManifest) Validate() error {
	if err := ValidateObjectKey(s.Key); err != nil {
		return err
	}
	if s.FirstHeight <= 0 || s.LastHeight < s.FirstHeight {
		return fmt.Errorf("invalid height range %d-%d", s.FirstHeight, s.LastHeight)
	}
	if s.Compression == "" {
		return errors.New("compression is required")
	}
	if err := ValidateCompression(s.Compression); err != nil {
		return err
	}
	if s.SizeBytes <= 0 {
		return errors.New("size_bytes must be positive")
	}
	if len(s.SHA256) != blockHashHexLen {
		return errors.New("sha256 must be 64 hex characters")
	}
	if _, err := hex.DecodeString(s.SHA256); err != nil {
		return fmt.Errorf("sha256 is not valid hex: %w", err)
	}
	if got, want := len(s.Blocks), int(s.LastHeight-s.FirstHeight+1); got != want {
		return fmt.Errorf("block index length %d, expected %d", got, want)
	}
	for i, block := range s.Blocks {
		wantHeight := s.FirstHeight + int64(i)
		if block.Height != wantHeight {
			return fmt.Errorf("block index height %d, expected %d", block.Height, wantHeight)
		}
		if len(block.Hash) != blockHashHexLen {
			return fmt.Errorf("block index for height %d hash must be %d hex characters", block.Height, blockHashHexLen)
		}
		if _, err := hex.DecodeString(block.Hash); err != nil {
			return fmt.Errorf("block index for height %d hash is not valid hex: %w", block.Height, err)
		}
	}
	return nil
}
