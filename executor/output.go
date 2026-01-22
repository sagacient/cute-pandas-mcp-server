// SPDX-License-Identifier: MPL-2.0
// Copyright 2026 Sagacient <sagacient@gmail.com>
//
// See CONTRIBUTORS.md for full contributor list.

// Package executor provides output management for pandas script executions.
package executor

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ExecutionMetadata holds metadata for an execution's output directory.
type ExecutionMetadata struct {
	ExecutionID string    `json:"execution_id"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// ExecutionInfo represents information about an execution and its files.
type ExecutionInfo struct {
	ExecutionID string    `json:"execution_id"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	Files       []string  `json:"files"`
	OutputPath  string    `json:"output_path"`
}

// OutputManager manages execution output directories with TTL-based cleanup.
type OutputManager struct {
	baseDir    string
	ttl        time.Duration
	mu         sync.RWMutex
	stopCh     chan struct{}
	cleanupWg  sync.WaitGroup
}

// NewOutputManager creates a new OutputManager.
func NewOutputManager(baseDir string, ttl time.Duration) *OutputManager {
	return &OutputManager{
		baseDir: baseDir,
		ttl:     ttl,
		stopCh:  make(chan struct{}),
	}
}

// GenerateExecutionID creates a new unique execution ID.
func GenerateExecutionID() string {
	id := uuid.New().String()
	// Use first 8 chars for shorter IDs
	return fmt.Sprintf("exec-%s", id[:8])
}

// CreateExecutionDir creates a new execution directory with metadata.
func (m *OutputManager) CreateExecutionDir(execID string) (string, error) {
	if m.baseDir == "" {
		return "", fmt.Errorf("output directory not configured")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	execDir := filepath.Join(m.baseDir, execID)
	if err := os.MkdirAll(execDir, 0777); err != nil {
		return "", fmt.Errorf("failed to create execution directory: %w", err)
	}
	
	// Ensure directory is writable by all users (for Docker containers running as different UIDs)
	if err := os.Chmod(execDir, 0777); err != nil {
		return "", fmt.Errorf("failed to set directory permissions: %w", err)
	}

	// Write metadata file
	metadata := ExecutionMetadata{
		ExecutionID: execID,
		CreatedAt:   time.Now(),
		ExpiresAt:   time.Now().Add(m.ttl),
	}

	metadataPath := filepath.Join(execDir, ".metadata.json")
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := os.WriteFile(metadataPath, data, 0644); err != nil {
		return "", fmt.Errorf("failed to write metadata: %w", err)
	}

	return execDir, nil
}

// ListExecutions returns all executions with their metadata and files.
func (m *OutputManager) ListExecutions() ([]ExecutionInfo, error) {
	if m.baseDir == "" {
		return nil, fmt.Errorf("output directory not configured")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	entries, err := os.ReadDir(m.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []ExecutionInfo{}, nil
		}
		return nil, fmt.Errorf("failed to read output directory: %w", err)
	}

	var executions []ExecutionInfo
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "exec-") {
			continue
		}

		execDir := filepath.Join(m.baseDir, entry.Name())
		info, err := m.getExecutionInfo(execDir)
		if err != nil {
			log.Printf("Warning: failed to get execution info for %s: %v", entry.Name(), err)
			continue
		}
		executions = append(executions, *info)
	}

	return executions, nil
}

// ListFiles returns the files in a specific execution directory.
func (m *OutputManager) ListFiles(execID string) ([]string, error) {
	if m.baseDir == "" {
		return nil, fmt.Errorf("output directory not configured")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	execDir := filepath.Join(m.baseDir, execID)
	if _, err := os.Stat(execDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("execution %s not found", execID)
	}

	return m.listFilesInDir(execDir)
}

// GetFile reads the contents of a file from an execution directory.
func (m *OutputManager) GetFile(execID, filename string) ([]byte, error) {
	if m.baseDir == "" {
		return nil, fmt.Errorf("output directory not configured")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Sanitize filename to prevent path traversal
	filename = filepath.Base(filename)
	filePath := filepath.Join(m.baseDir, execID, filename)

	// Ensure the path is still within the execution directory
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, fmt.Errorf("invalid path: %w", err)
	}
	execDir := filepath.Join(m.baseDir, execID)
	absExecDir, _ := filepath.Abs(execDir)
	if !strings.HasPrefix(absPath, absExecDir) {
		return nil, fmt.Errorf("path traversal detected")
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file %s not found in execution %s", filename, execID)
		}
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return data, nil
}

// DeleteExecution removes an execution directory and all its contents.
func (m *OutputManager) DeleteExecution(execID string) error {
	if m.baseDir == "" {
		return fmt.Errorf("output directory not configured")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	execDir := filepath.Join(m.baseDir, execID)
	if _, err := os.Stat(execDir); os.IsNotExist(err) {
		return fmt.Errorf("execution %s not found", execID)
	}

	if err := os.RemoveAll(execDir); err != nil {
		return fmt.Errorf("failed to delete execution: %w", err)
	}

	return nil
}

// DeleteAllExecutions removes all execution directories.
func (m *OutputManager) DeleteAllExecutions() (int, error) {
	if m.baseDir == "" {
		return 0, fmt.Errorf("output directory not configured")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	entries, err := os.ReadDir(m.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to read output directory: %w", err)
	}

	deleted := 0
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "exec-") {
			continue
		}

		execDir := filepath.Join(m.baseDir, entry.Name())
		if err := os.RemoveAll(execDir); err != nil {
			log.Printf("Warning: failed to delete %s: %v", entry.Name(), err)
			continue
		}
		deleted++
	}

	return deleted, nil
}

// StartCleanupLoop starts a background goroutine that periodically cleans up expired executions.
func (m *OutputManager) StartCleanupLoop(interval time.Duration) {
	if m.baseDir == "" {
		log.Printf("Output directory not configured, cleanup loop disabled")
		return
	}

	m.cleanupWg.Add(1)
	go func() {
		defer m.cleanupWg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Run cleanup immediately on start
		m.cleanupExpired()

		for {
			select {
			case <-ticker.C:
				m.cleanupExpired()
			case <-m.stopCh:
				return
			}
		}
	}()
	log.Printf("Output cleanup loop started (interval: %v, TTL: %v)", interval, m.ttl)
}

// Stop stops the cleanup loop.
func (m *OutputManager) Stop() {
	close(m.stopCh)
	m.cleanupWg.Wait()
}

// cleanupExpired removes all expired execution directories.
func (m *OutputManager) cleanupExpired() {
	if m.baseDir == "" {
		return
	}

	entries, err := os.ReadDir(m.baseDir)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("Cleanup: failed to read output directory: %v", err)
		}
		return
	}

	now := time.Now()
	cleaned := 0

	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "exec-") {
			continue
		}

		execDir := filepath.Join(m.baseDir, entry.Name())
		metadata, err := m.readMetadata(execDir)
		if err != nil {
			// If no metadata, check directory age
			info, err := entry.Info()
			if err != nil {
				continue
			}
			if now.Sub(info.ModTime()) > m.ttl {
				if err := os.RemoveAll(execDir); err == nil {
					cleaned++
				}
			}
			continue
		}

		if now.After(metadata.ExpiresAt) {
			m.mu.Lock()
			if err := os.RemoveAll(execDir); err == nil {
				cleaned++
			}
			m.mu.Unlock()
		}
	}

	if cleaned > 0 {
		log.Printf("Cleanup: removed %d expired execution(s)", cleaned)
	}
}

// getExecutionInfo reads execution info from a directory.
func (m *OutputManager) getExecutionInfo(execDir string) (*ExecutionInfo, error) {
	metadata, err := m.readMetadata(execDir)
	if err != nil {
		// Try to construct from directory info
		info, err := os.Stat(execDir)
		if err != nil {
			return nil, err
		}
		metadata = &ExecutionMetadata{
			ExecutionID: filepath.Base(execDir),
			CreatedAt:   info.ModTime(),
			ExpiresAt:   info.ModTime().Add(m.ttl),
		}
	}

	files, _ := m.listFilesInDir(execDir)

	return &ExecutionInfo{
		ExecutionID: metadata.ExecutionID,
		CreatedAt:   metadata.CreatedAt,
		ExpiresAt:   metadata.ExpiresAt,
		Files:       files,
		OutputPath:  execDir,
	}, nil
}

// readMetadata reads the metadata file from an execution directory.
func (m *OutputManager) readMetadata(execDir string) (*ExecutionMetadata, error) {
	metadataPath := filepath.Join(execDir, ".metadata.json")
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, err
	}

	var metadata ExecutionMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

// listFilesInDir lists all files in a directory (excluding metadata).
func (m *OutputManager) listFilesInDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == ".metadata.json" {
			continue
		}
		files = append(files, entry.Name())
	}

	return files, nil
}

// ScanOutputFiles lists files in a specific execution directory (for use after script execution).
func (m *OutputManager) ScanOutputFiles(execDir string) ([]string, error) {
	return m.listFilesInDir(execDir)
}
