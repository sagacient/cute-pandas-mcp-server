// SPDX-License-Identifier: MPL-2.0
// Copyright 2026 Sagacient <sagacient@gmail.com>
//
// See CONTRIBUTORS.md for full contributor list.

// Package storage provides file storage management with automatic TTL cleanup.
package storage

import (
	"crypto/rand"
	"github.com/sagacient/cute-pandas-mcp-server/scanner"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// FileInfo holds metadata about an uploaded file.
type FileInfo struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Path       string    `json:"-"` // Internal path, not exposed in JSON
	Size       int64     `json:"size"`
	UploadedAt time.Time `json:"uploaded_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	FileRef    string    `json:"file_ref"` // upload://id reference for tool calls
}

// FileStore manages uploaded files with automatic TTL-based cleanup.
type FileStore struct {
	baseDir string
	ttl     time.Duration
	maxSize int64
	scanner *scanner.Scanner
	files   map[string]*FileInfo
	mu      sync.RWMutex
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewFileStore creates a new FileStore with the given configuration.
// It starts a background cleanup goroutine that removes expired files.
// The scanner parameter can be nil to disable malware scanning.
func NewFileStore(baseDir string, ttl time.Duration, maxSize int64, sc *scanner.Scanner) (*FileStore, error) {
	// Expand ~ to home directory
	if strings.HasPrefix(baseDir, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to get home directory: %w", err)
		}
		baseDir = filepath.Join(home, baseDir[1:])
	}

	// Create base directory
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory %s: %w", baseDir, err)
	}

	fs := &FileStore{
		baseDir: baseDir,
		ttl:     ttl,
		maxSize: maxSize,
		scanner: sc,
		files:   make(map[string]*FileInfo),
		stopCh:  make(chan struct{}),
	}

	// Load existing files from disk (for restart recovery)
	fs.loadExistingFiles()

	// Start cleanup goroutine
	fs.wg.Add(1)
	go fs.cleanupLoop()

	scanStatus := "disabled"
	if sc != nil && sc.IsEnabled() {
		scanStatus = "enabled"
	}
	log.Printf("FileStore initialized: dir=%s, ttl=%v, maxSize=%d bytes, scanning=%s", baseDir, ttl, maxSize, scanStatus)
	return fs, nil
}

// loadExistingFiles scans the storage directory for existing files.
// Files without metadata are assigned a new TTL from now.
func (fs *FileStore) loadExistingFiles() {
	entries, err := os.ReadDir(fs.baseDir)
	if err != nil {
		log.Printf("Warning: could not read storage directory: %v", err)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Extract ID from filename (format: id_originalname)
		name := entry.Name()
		parts := strings.SplitN(name, "_", 2)
		if len(parts) < 2 {
			continue
		}

		id := parts[0]
		originalName := parts[1]

		fileInfo := &FileInfo{
			ID:         id,
			Name:       originalName,
			Path:       filepath.Join(fs.baseDir, name),
			Size:       info.Size(),
			UploadedAt: info.ModTime(),
			ExpiresAt:  time.Now().Add(fs.ttl), // Reset TTL on restart
			FileRef:    "upload://" + id,
		}

		fs.files[id] = fileInfo
		log.Printf("Loaded existing file: %s (expires at %v)", name, fileInfo.ExpiresAt)
	}
}

// cleanupLoop runs periodically to remove expired files.
func (fs *FileStore) cleanupLoop() {
	defer fs.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-fs.stopCh:
			return
		case <-ticker.C:
			fs.cleanup()
		}
	}
}

// cleanup removes expired files.
func (fs *FileStore) cleanup() {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	now := time.Now()
	for id, info := range fs.files {
		if now.After(info.ExpiresAt) {
			if err := os.Remove(info.Path); err != nil {
				log.Printf("Warning: failed to remove expired file %s: %v", info.Path, err)
			} else {
				log.Printf("Cleaned up expired file: %s (was uploaded at %v)", info.Name, info.UploadedAt)
			}
			delete(fs.files, id)
		}
	}
}

// Close stops the cleanup goroutine and releases resources.
func (fs *FileStore) Close() error {
	close(fs.stopCh)
	fs.wg.Wait()
	return nil
}

// ErrMalwareDetected is returned when malware is detected in an uploaded file.
type ErrMalwareDetected struct {
	Threat string
}

func (e *ErrMalwareDetected) Error() string {
	return fmt.Sprintf("malware detected: %s", e.Threat)
}

// ErrScannerUnavailable is returned when the scanner is unavailable and fail-open is disabled.
type ErrScannerUnavailable struct{}

func (e *ErrScannerUnavailable) Error() string {
	return "malware scanner unavailable"
}

// Upload saves a file from the reader and returns its metadata.
// If scanning is enabled, the file is scanned for malware before being stored.
func (fs *FileStore) Upload(filename string, r io.Reader) (*FileInfo, error) {
	// Generate unique ID
	id, err := generateID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate file ID: %w", err)
	}

	// Sanitize filename
	safeName := sanitizeFilename(filename)
	if safeName == "" {
		safeName = "file"
	}

	// Create file path
	storedName := fmt.Sprintf("%s_%s", id, safeName)
	filePath := filepath.Join(fs.baseDir, storedName)

	// Create file
	f, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	// Copy with size limit
	limitedReader := io.LimitReader(r, fs.maxSize+1) // +1 to detect overflow
	size, err := io.Copy(f, limitedReader)
	f.Close() // Close before scanning

	if err != nil {
		os.Remove(filePath)
		return nil, fmt.Errorf("failed to write file: %w", err)
	}

	if size > fs.maxSize {
		os.Remove(filePath)
		return nil, fmt.Errorf("file exceeds maximum size of %d bytes", fs.maxSize)
	}

	// Scan for malware if scanner is configured
	if fs.scanner != nil && fs.scanner.IsEnabled() {
		result := fs.scanner.Scan(filePath)
		if result.Error != nil {
			os.Remove(filePath)
			return nil, &ErrScannerUnavailable{}
		}
		if !result.Clean {
			os.Remove(filePath)
			log.Printf("SECURITY: Rejected malware upload - file=%s, threat=%s", filename, result.Threat)
			return nil, &ErrMalwareDetected{Threat: result.Threat}
		}
	}

	now := time.Now()
	info := &FileInfo{
		ID:         id,
		Name:       filename,
		Path:       filePath,
		Size:       size,
		UploadedAt: now,
		ExpiresAt:  now.Add(fs.ttl),
		FileRef:    "upload://" + id,
	}

	fs.mu.Lock()
	fs.files[id] = info
	fs.mu.Unlock()

	log.Printf("Uploaded file: %s (id=%s, size=%d, expires=%v)", filename, id, size, info.ExpiresAt)
	return info, nil
}

// GetPath returns the filesystem path for an upload ID.
// Returns empty string and false if not found.
func (fs *FileStore) GetPath(id string) (string, bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if info, ok := fs.files[id]; ok {
		// Check if expired
		if time.Now().After(info.ExpiresAt) {
			return "", false
		}
		return info.Path, true
	}
	return "", false
}

// Get returns the FileInfo for an upload ID.
func (fs *FileStore) Get(id string) (*FileInfo, bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if info, ok := fs.files[id]; ok {
		// Check if expired
		if time.Now().After(info.ExpiresAt) {
			return nil, false
		}
		return info, true
	}
	return nil, false
}

// List returns all non-expired uploaded files.
func (fs *FileStore) List() []*FileInfo {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	now := time.Now()
	result := make([]*FileInfo, 0, len(fs.files))
	for _, info := range fs.files {
		if now.Before(info.ExpiresAt) {
			result = append(result, info)
		}
	}
	return result
}

// Delete removes a file by ID.
func (fs *FileStore) Delete(id string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	info, ok := fs.files[id]
	if !ok {
		return fmt.Errorf("file not found: %s", id)
	}

	if err := os.Remove(info.Path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove file: %w", err)
	}

	delete(fs.files, id)
	log.Printf("Deleted file: %s (id=%s)", info.Name, id)
	return nil
}

// ResolveUploadURI resolves an upload:// URI to a filesystem path.
// Returns the original path if it's not an upload:// URI.
func (fs *FileStore) ResolveUploadURI(uri string) (string, error) {
	if !strings.HasPrefix(uri, "upload://") {
		return uri, nil
	}

	id := strings.TrimPrefix(uri, "upload://")
	path, ok := fs.GetPath(id)
	if !ok {
		return "", fmt.Errorf("uploaded file not found or expired: %s", id)
	}
	return path, nil
}

// BaseDir returns the storage directory path.
func (fs *FileStore) BaseDir() string {
	return fs.baseDir
}

// TTL returns the configured TTL duration.
func (fs *FileStore) TTL() time.Duration {
	return fs.ttl
}

// generateID creates a cryptographically random ID.
func generateID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// sanitizeFilename removes potentially dangerous characters from filenames.
func sanitizeFilename(name string) string {
	// Get just the base name
	name = filepath.Base(name)

	// Remove any path separators
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")

	// Remove null bytes
	name = strings.ReplaceAll(name, "\x00", "")

	// Remove leading dots (hidden files)
	name = strings.TrimLeft(name, ".")

	// Limit length
	if len(name) > 200 {
		ext := filepath.Ext(name)
		name = name[:200-len(ext)] + ext
	}

	return name
}
