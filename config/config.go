// SPDX-License-Identifier: MPL-2.0
// Copyright 2026 Sagacient <sagacient@gmail.com>
//
// See CONTRIBUTORS.md for full contributor list.

// Package config provides configuration loading for the Cute Pandas MCP Server.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all configuration options for the server.
type Config struct {
	// Worker pool settings
	MaxWorkers     int           // Max concurrent container executions
	QueueSize      int           // Max pending requests in queue
	AcquireTimeout time.Duration // Time to wait for an available worker

	// Execution settings
	ExecutionTimeout time.Duration // Max script execution time
	MaxMemoryMB      int64         // Memory limit per container in MB
	MaxCPU           float64       // CPU limit per container (1.0 = 1 core)

	// Docker settings
	DockerImage     string // Docker image to use for pandas execution
	BuildLocal      bool   // Force local build from CutePandas.Dockerfile instead of pulling
	NetworkDisabled bool   // Disable network in containers

	// Server settings
	Transport string // Transport type: "stdio" or "http"
	HTTPPort  int    // Port for HTTP transport

	// Storage settings (HTTP mode file uploads)
	StorageDir    string        // Directory for uploaded files
	UploadTTL     time.Duration // Auto-delete uploaded files after this duration
	MaxUploadSize int64         // Maximum upload file size in bytes

	// Malware scanning settings
	ScanUploads bool   // Enable ClamAV malware scanning for uploads
	ScanOnFail  string // Behavior when scanner unavailable: "reject" or "allow"
}

// DefaultConfig returns the default configuration.
// By default, uses Docker Hub image for instant startup.
// Set BUILD_LOCAL=true to build from CutePandas.Dockerfile instead.
func DefaultConfig() *Config {
	return &Config{
		MaxWorkers:       5,
		QueueSize:        10,
		AcquireTimeout:   30 * time.Second,
		ExecutionTimeout: 60 * time.Second,
		MaxMemoryMB:      512,
		MaxCPU:           1.0,
		DockerImage:      "sagacient/cutepandas:latest", // Docker Hub image for instant startup
		BuildLocal:       false,                          // Set to true to build from CutePandas.Dockerfile
		NetworkDisabled:  true,
		Transport:        "stdio",
		HTTPPort:         8080,
		StorageDir:       defaultStorageDir(),       // ~/.cache/cute-pandas/uploads or /storage in Docker
		UploadTTL:        1 * time.Hour,             // Auto-delete after 1 hour
		MaxUploadSize:    100 * 1024 * 1024,         // 100MB
		ScanUploads:      true,                      // Enable malware scanning by default
		ScanOnFail:       "reject",                  // Reject uploads if scanner unavailable
	}
}

// defaultStorageDir returns the default storage directory.
// Uses /storage if running in Docker (detected via STORAGE_DIR env or /storage exists),
// otherwise uses ~/.cache/cute-pandas/uploads.
func defaultStorageDir() string {
	// Check if /storage exists (Docker environment)
	if _, err := os.Stat("/storage"); err == nil {
		return "/storage"
	}
	// Default to user cache directory
	return "~/.cache/cute-pandas/uploads"
}

// LoadFromEnv loads configuration from environment variables.
func LoadFromEnv() *Config {
	cfg := DefaultConfig()

	if v := os.Getenv("MAX_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.MaxWorkers = n
		}
	}

	if v := os.Getenv("QUEUE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.QueueSize = n
		}
	}

	if v := os.Getenv("ACQUIRE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.AcquireTimeout = d
		}
	}

	if v := os.Getenv("EXECUTION_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.ExecutionTimeout = d
		}
	}

	if v := os.Getenv("MAX_MEMORY_MB"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			cfg.MaxMemoryMB = n
		}
	}

	if v := os.Getenv("MAX_CPU"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			cfg.MaxCPU = f
		}
	}

	if v := os.Getenv("DOCKER_IMAGE"); v != "" {
		cfg.DockerImage = v
	}

	if v := os.Getenv("BUILD_LOCAL"); v != "" {
		cfg.BuildLocal = v == "true" || v == "1"
	}

	if v := os.Getenv("NETWORK_DISABLED"); v != "" {
		cfg.NetworkDisabled = v == "true" || v == "1"
	}

	if v := os.Getenv("TRANSPORT"); v != "" {
		cfg.Transport = v
	}

	if v := os.Getenv("HTTP_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.HTTPPort = n
		}
	}

	if v := os.Getenv("STORAGE_DIR"); v != "" {
		cfg.StorageDir = v
	}

	if v := os.Getenv("UPLOAD_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.UploadTTL = d
		}
	}

	if v := os.Getenv("MAX_UPLOAD_SIZE"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			cfg.MaxUploadSize = n
		}
	}

	if v := os.Getenv("SCAN_UPLOADS"); v != "" {
		cfg.ScanUploads = v == "true" || v == "1"
	}

	if v := os.Getenv("SCAN_ON_FAIL"); v != "" {
		if v == "allow" || v == "reject" {
			cfg.ScanOnFail = v
		}
	}

	return cfg
}
