// SPDX-License-Identifier: MPL-2.0
// Copyright 2026 Sagacient <sagacient@gmail.com>
//
// See CONTRIBUTORS.md for full contributor list.

// Package httpserver provides an HTTP server with MCP and file storage endpoints.
package httpserver

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/sagacient/cute-pandas-mcp-server/storage"

	"github.com/mark3labs/mcp-go/server"
)

// Server wraps the MCP HTTP server and adds storage endpoints.
type Server struct {
	mcpServer   *server.MCPServer
	fileStore   *storage.FileStore
	httpServer  *server.StreamableHTTPServer
	mux         *http.ServeMux
	maxUploadMB int64
}

// NewServer creates a new HTTP server with MCP and storage endpoints.
func NewServer(mcpServer *server.MCPServer, fileStore *storage.FileStore, maxUploadSize int64) *Server {
	s := &Server{
		mcpServer:   mcpServer,
		fileStore:   fileStore,
		mux:         http.NewServeMux(),
		maxUploadMB: maxUploadSize,
	}

	// Create the MCP HTTP server
	s.httpServer = server.NewStreamableHTTPServer(mcpServer)

	// Register storage endpoints
	s.mux.HandleFunc("/storage/upload", s.handleUpload)
	s.mux.HandleFunc("/storage/list", s.handleList)
	s.mux.HandleFunc("/storage/download/", s.handleDownload)
	s.mux.HandleFunc("/storage/delete/", s.handleDelete)

	// Health check
	s.mux.HandleFunc("/health", s.handleHealth)

	return s
}

// Start starts the HTTP server on the given address.
func (s *Server) Start(addr string) error {
	// Create a combined handler that routes to MCP or storage endpoints
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Add CORS headers for browser clients
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Route to storage endpoints
		if strings.HasPrefix(r.URL.Path, "/storage/") || r.URL.Path == "/health" {
			s.mux.ServeHTTP(w, r)
			return
		}

		// Route everything else to MCP server
		s.httpServer.ServeHTTP(w, r)
	})

	log.Printf("HTTP server starting on %s", addr)
	log.Printf("Storage endpoints available at /storage/upload, /storage/list, /storage/download/{id}, /storage/delete/{id}")
	return http.ListenAndServe(addr, handler)
}

// handleUpload handles file uploads via multipart/form-data.
// POST /storage/upload
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Limit request body size (add 1MB for form overhead)
	r.Body = http.MaxBytesReader(w, r.Body, s.maxUploadMB+1024*1024)

	// Parse multipart form
	if err := r.ParseMultipartForm(32 << 20); err != nil { // 32MB in memory
		http.Error(w, fmt.Sprintf("Failed to parse form: %v", err), http.StatusBadRequest)
		return
	}

	// Get the file
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get file: %v", err), http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Upload to storage (includes malware scanning if enabled)
	info, err := s.fileStore.Upload(header.Filename, file)
	if err != nil {
		// Handle specific error types
		switch e := err.(type) {
		case *storage.ErrMalwareDetected:
			// Return 422 Unprocessable Entity for malware
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnprocessableEntity)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error":  "Malware detected",
				"threat": e.Threat,
				"status": http.StatusUnprocessableEntity,
			})
			return
		case *storage.ErrScannerUnavailable:
			// Return 503 Service Unavailable when scanner is down
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error":  "Malware scanner unavailable",
				"status": http.StatusServiceUnavailable,
			})
			return
		default:
			if strings.Contains(err.Error(), "exceeds maximum size") {
				http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, fmt.Sprintf("Failed to store file: %v", err), http.StatusInternalServerError)
			return
		}
	}

	// Return file info
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(info)
}

// handleList returns a list of all uploaded files.
// GET /storage/list
func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	files := s.fileStore.List()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"files": files,
		"count": len(files),
	})
}

// handleDownload returns a file by ID.
// GET /storage/download/{id}
func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract ID from path
	id := strings.TrimPrefix(r.URL.Path, "/storage/download/")
	if id == "" {
		http.Error(w, "File ID required", http.StatusBadRequest)
		return
	}

	// Get file info
	info, ok := s.fileStore.Get(id)
	if !ok {
		http.Error(w, "File not found or expired", http.StatusNotFound)
		return
	}

	// Open file
	file, err := os.Open(info.Path)
	if err != nil {
		http.Error(w, "Failed to open file", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// Set headers
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", info.Name))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size))
	w.Header().Set("X-Content-Type-Options", "nosniff")

	// Stream file
	io.Copy(w, file)
}

// handleDelete removes a file by ID.
// DELETE /storage/delete/{id}
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract ID from path
	id := strings.TrimPrefix(r.URL.Path, "/storage/delete/")
	if id == "" {
		http.Error(w, "File ID required", http.StatusBadRequest)
		return
	}

	// Delete file
	if err := s.fileStore.Delete(id); err != nil {
		if strings.Contains(err.Error(), "not found") {
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("Failed to delete file: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "deleted",
		"id":     id,
	})
}

// handleHealth returns server health status.
// GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "healthy",
		"storage_dir": s.fileStore.BaseDir(),
		"upload_ttl":  s.fileStore.TTL().String(),
	})
}
