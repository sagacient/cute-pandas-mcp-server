// SPDX-License-Identifier: MPL-2.0
// Copyright 2026 Sagacient <sagacient@gmail.com>
//
// See CONTRIBUTORS.md for full contributor list.

// Package main is the entry point for the Cute Pandas MCP Server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sagacient/cute-pandas-mcp-server/config"
	"github.com/sagacient/cute-pandas-mcp-server/executor"
	"github.com/sagacient/cute-pandas-mcp-server/httpserver"
	"github.com/sagacient/cute-pandas-mcp-server/scanner"
	"github.com/sagacient/cute-pandas-mcp-server/storage"
	"github.com/sagacient/cute-pandas-mcp-server/tools"
	"github.com/sagacient/cute-pandas-mcp-server/workerpool"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func main() {
	// Parse command line flags
	var transport string
	flag.StringVar(&transport, "t", "", "Transport type (stdio or http)")
	flag.StringVar(&transport, "transport", "", "Transport type (stdio or http)")
	flag.Parse()

	// Load configuration
	cfg := config.LoadFromEnv()

	// Override transport from flag if provided
	if transport != "" {
		cfg.Transport = transport
	}

	// Create worker pool
	pool := workerpool.NewPool(cfg.MaxWorkers, cfg.AcquireTimeout)

	// Create Docker executor
	exec, err := executor.NewDockerExecutor(
		cfg.DockerImage,
		cfg.MaxMemoryMB,
		cfg.MaxCPU,
		cfg.NetworkDisabled,
		cfg.ExecutionTimeout,
		cfg.BuildLocal,
		cfg.TempDir,
		cfg.OutputDir,
		cfg.OutputTTL,
	)
	if err != nil {
		log.Fatalf("Failed to create Docker executor: %v", err)
	}
	defer exec.Close()

	// Start Docker image build/pull in background (non-blocking)
	if cfg.BuildLocal {
		log.Printf("Checking Docker image: %s (BUILD_LOCAL=true, will build locally)", cfg.DockerImage)
	} else {
		log.Printf("Checking Docker image: %s (will pull from registry)", cfg.DockerImage)
	}
	ctx := context.Background()
	exec.EnsureImageAsync(ctx)

	// Initialize file store and scanner for HTTP mode
	var fileStore *storage.FileStore
	var malwareScanner *scanner.Scanner
	if cfg.Transport == "http" {
		// Initialize malware scanner
		malwareScanner = scanner.NewScanner(scanner.Config{
			Enabled:  cfg.ScanUploads,
			FailOpen: cfg.ScanOnFail == "allow",
		})
		if cfg.ScanUploads {
			if malwareScanner.IsAvailable() {
				log.Printf("Malware scanning enabled (ClamAV available)")
			} else {
				log.Printf("WARNING: Malware scanning enabled but ClamAV not available (scan_on_fail=%s)", cfg.ScanOnFail)
			}
		} else {
			log.Printf("Malware scanning disabled")
		}

		// Initialize file store with scanner
		var err error
		fileStore, err = storage.NewFileStore(cfg.StorageDir, cfg.UploadTTL, cfg.MaxUploadSize, malwareScanner)
		if err != nil {
			log.Fatalf("Failed to create file store: %v", err)
		}
		defer fileStore.Close()
		log.Printf("File storage enabled: dir=%s, ttl=%v, max_size=%d bytes",
			fileStore.BaseDir(), cfg.UploadTTL, cfg.MaxUploadSize)
	}

	// Create MCP server
	mcpServer, pandasTools := createMCPServer(cfg, pool, exec)

	// Set file store on tools if in HTTP mode
	if fileStore != nil {
		pandasTools.SetFileStore(fileStore)
	}

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down...")
		exec.Close()
		if fileStore != nil {
			fileStore.Close()
		}
		os.Exit(0)
	}()

	// Start server based on transport type
	if cfg.Transport == "http" {
		log.Printf("Starting HTTP server on port %d", cfg.HTTPPort)
		httpSrv := httpserver.NewServer(mcpServer, fileStore, cfg.MaxUploadSize)
		addr := fmt.Sprintf(":%d", cfg.HTTPPort)
		if err := httpSrv.Start(addr); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	} else {
		log.Println("Starting stdio server...")
		if err := server.ServeStdio(mcpServer); err != nil {
			log.Fatalf("Stdio server error: %v", err)
		}
	}
}

func createMCPServer(cfg *config.Config, pool *workerpool.Pool, exec *executor.DockerExecutor) (*server.MCPServer, *tools.PandasTools) {
	// Create hooks for logging
	hooks := &server.Hooks{}

	hooks.AddBeforeAny(func(ctx context.Context, id any, method mcp.MCPMethod, message any) {
		log.Printf("[MCP] Request: %s (id=%v)", method, id)
	})

	hooks.AddOnSuccess(func(ctx context.Context, id any, method mcp.MCPMethod, message any, result any) {
		log.Printf("[MCP] Success: %s (id=%v)", method, id)
	})

	hooks.AddOnError(func(ctx context.Context, id any, method mcp.MCPMethod, message any, err error) {
		log.Printf("[MCP] Error: %s (id=%v): %v", method, id, err)
	})

	// Create server
	mcpServer := server.NewMCPServer(
		"cute-pandas",
		"1.0.0",
		server.WithToolCapabilities(true),
		server.WithHooks(hooks),
		server.WithRecovery(),
	)

	// Create tools handler
	pandasTools := tools.NewPandasTools(pool, exec)

	// Register tools
	mcpServer.AddTool(tools.RunScriptTool(), pandasTools.RunScriptHandler)
	mcpServer.AddTool(tools.ReadDataFrameTool(), pandasTools.ReadDataFrameHandler)
	mcpServer.AddTool(tools.AnalyzeDataTool(), pandasTools.AnalyzeDataHandler)
	mcpServer.AddTool(tools.TransformDataTool(), pandasTools.TransformDataHandler)

	// Output management tools
	mcpServer.AddTool(tools.ListOutputsTool(), pandasTools.ListOutputsHandler)
	mcpServer.AddTool(tools.GetOutputTool(), pandasTools.GetOutputHandler)
	mcpServer.AddTool(tools.DeleteOutputsTool(), pandasTools.DeleteOutputsHandler)

	// Add a status tool for checking server health
	mcpServer.AddTool(
		mcp.NewTool("server_status",
			mcp.WithDescription("Get the current status of the Cute Pandas MCP server, including worker pool statistics and Docker image status."),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			stats := pool.Stats()

			imageStatus := "READY"
			if !exec.IsImageReady() {
				if err := exec.ImageBuildError(); err != nil {
					imageStatus = fmt.Sprintf("BUILD FAILED: %v", err)
				} else {
					imageStatus = "BUILDING... (first startup, please wait)"
				}
			}

			serverStatus := "READY"
			if pool.IsFull() {
				serverStatus = "BUSY (all workers occupied)"
			} else if !exec.IsImageReady() {
				serverStatus = "INITIALIZING"
			}

			status := fmt.Sprintf(`Cute Pandas MCP Server Status
==============================
Docker Image:     %s
Image Status:     %s
Max Workers:      %d
Active Workers:   %d
Available Slots:  %d
Total Processed:  %d
Server Status:    %s`,
				cfg.DockerImage,
				imageStatus,
				stats.MaxWorkers,
				stats.ActiveWorkers,
				stats.AvailableSlots,
				stats.TotalProcessed,
				serverStatus,
			)
			return mcp.NewToolResultText(status), nil
		},
	)

	return mcpServer, pandasTools
}
