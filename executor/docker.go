// SPDX-License-Identifier: MPL-2.0
// Copyright 2026 Sagacient <sagacient@gmail.com>
//
// See CONTRIBUTORS.md for full contributor list.

// Package executor provides Docker container management for executing pandas scripts.
package executor

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

// dockerfileName is the name of the Dockerfile to look for.
const dockerfileName = "CutePandas.Dockerfile"

// getDockerfileContent reads the CutePandas.Dockerfile from disk.
// It searches in the current directory and executable directory.
func getDockerfileContent() ([]byte, error) {
	// Locations to search for Dockerfile
	searchPaths := []string{
		dockerfileName,           // Current working directory
		"./" + dockerfileName,    // Explicit current dir
	}

	// Also try executable directory
	if execPath, err := os.Executable(); err == nil {
		execDir := filepath.Dir(execPath)
		searchPaths = append(searchPaths, filepath.Join(execDir, dockerfileName))
	}

	// Try each path
	for _, path := range searchPaths {
		content, err := os.ReadFile(path)
		if err == nil {
			log.Printf("Using Dockerfile from: %s", path)
			return content, nil
		}
	}

	// No fallback - error if not found
	return nil, fmt.Errorf("%s not found. Please ensure %s exists in the current directory or alongside the executable. Searched: %v", dockerfileName, dockerfileName, searchPaths)
}

// ExecutionResult holds the result of a script execution.
type ExecutionResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
	Duration time.Duration
	Error    string
}

// ErrImageNotReady is returned when the Docker image is still being built.
var ErrImageNotReady = fmt.Errorf("Docker image is still being built. Please try again in a minute")

// DockerExecutor manages Docker containers for script execution.
type DockerExecutor struct {
	client           *client.Client
	image            string
	memoryLimit      int64 // in bytes
	cpuLimit         float64
	networkDisabled  bool
	executionTimeout time.Duration
	buildLocal       bool // Force local build instead of pulling

	// Image readiness tracking
	imageReady    bool
	imageBuildErr error
	imageReadyMu  sync.RWMutex
}

// commonDockerSockets lists common Docker socket locations to try.
// Order matters - we try them in sequence and use the first one that works.
func commonDockerSockets() []string {
	homeDir, _ := os.UserHomeDir()
	uid := os.Getuid()

	sockets := []string{
		// Default Docker
		"/var/run/docker.sock",
		// Colima (macOS)
		filepath.Join(homeDir, ".colima", "default", "docker.sock"),
		filepath.Join(homeDir, ".colima", "docker", "docker.sock"),
		// Lima (macOS/Linux)
		filepath.Join(homeDir, ".lima", "default", "sock", "docker.sock"),
		filepath.Join(homeDir, ".lima", "docker", "sock", "docker.sock"),
		// Podman rootless (Linux)
		fmt.Sprintf("/run/user/%d/podman/podman.sock", uid),
		// Rancher Desktop (macOS)
		filepath.Join(homeDir, ".rd", "docker.sock"),
		// Docker Desktop (macOS alternate)
		filepath.Join(homeDir, ".docker", "run", "docker.sock"),
		filepath.Join(homeDir, "Library", "Containers", "com.docker.docker", "Data", "docker.raw.sock"),
	}

	return sockets
}

// findDockerSocket attempts to find a working Docker socket.
// Returns the socket path and a connected client, or an error if none found.
func findDockerSocket() (*client.Client, string, error) {
	// First, check if DOCKER_HOST is already set
	if dockerHost := os.Getenv("DOCKER_HOST"); dockerHost != "" {
		log.Printf("Using DOCKER_HOST from environment: %s", dockerHost)
		cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
		if err != nil {
			return nil, "", fmt.Errorf("failed to connect using DOCKER_HOST=%s: %w", dockerHost, err)
		}
		// Test the connection
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := cli.Ping(ctx); err != nil {
			cli.Close()
			return nil, "", fmt.Errorf("failed to ping Docker at DOCKER_HOST=%s: %w", dockerHost, err)
		}
		return cli, dockerHost, nil
	}

	// Try common socket locations
	var lastErr error
	for _, socketPath := range commonDockerSockets() {
		// Check if socket file exists
		if _, err := os.Stat(socketPath); os.IsNotExist(err) {
			continue
		}

		host := "unix://" + socketPath
		cli, err := client.NewClientWithOpts(
			client.WithHost(host),
			client.WithAPIVersionNegotiation(),
		)
		if err != nil {
			lastErr = err
			continue
		}

		// Test the connection with a ping
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = cli.Ping(ctx)
		cancel()

		if err != nil {
			cli.Close()
			lastErr = fmt.Errorf("socket %s exists but ping failed: %w", socketPath, err)
			continue
		}

		log.Printf("Found working Docker socket: %s", socketPath)
		return cli, host, nil
	}

	if lastErr != nil {
		return nil, "", fmt.Errorf("no working Docker socket found. Last error: %w. Tried: %v", lastErr, commonDockerSockets())
	}
	return nil, "", fmt.Errorf("no Docker socket found. Tried: %v", commonDockerSockets())
}

// NewDockerExecutor creates a new Docker executor.
func NewDockerExecutor(imageName string, memoryMB int64, cpuLimit float64, networkDisabled bool, timeout time.Duration, buildLocal bool) (*DockerExecutor, error) {
	cli, socketPath, err := findDockerSocket()
	if err != nil {
		return nil, fmt.Errorf("failed to find Docker: %w\n\nMake sure Docker, Colima, Lima, Podman, or Rancher Desktop is running.\nYou can also set DOCKER_HOST environment variable manually.", err)
	}

	log.Printf("Connected to Docker via: %s", socketPath)

	return &DockerExecutor{
		client:           cli,
		image:            imageName,
		memoryLimit:      memoryMB * 1024 * 1024, // Convert MB to bytes
		cpuLimit:         cpuLimit,
		networkDisabled:  networkDisabled,
		executionTimeout: timeout,
		buildLocal:       buildLocal,
	}, nil
}

// Close closes the Docker client connection.
func (e *DockerExecutor) Close() error {
	return e.client.Close()
}

// EnsureImageAsync starts pulling or building the Docker image in the background if it doesn't exist.
// By default, it pulls from Docker Hub for instant startup.
// If BuildLocal is true, it builds from CutePandas.Dockerfile instead.
// Returns immediately. Use IsImageReady() to check status.
func (e *DockerExecutor) EnsureImageAsync(ctx context.Context) {
	// Check if image exists locally
	_, _, err := e.client.ImageInspectWithRaw(ctx, e.image)
	if err == nil {
		log.Printf("Docker image %s found locally", e.image)
		e.imageReadyMu.Lock()
		e.imageReady = true
		e.imageReadyMu.Unlock()
		return
	}

	if e.buildLocal {
		log.Printf("Docker image %s not found, building locally (BUILD_LOCAL=true)...", e.image)
	} else {
		log.Printf("Docker image %s not found locally, pulling from registry...", e.image)
	}

	// Build/pull in background
	go func() {
		bgCtx := context.Background() // Use background context

		var resultErr error

		if e.buildLocal {
			// Build locally from CutePandas.Dockerfile
			if err := e.buildImage(bgCtx); err != nil {
				resultErr = fmt.Errorf("failed to build image %s: %w", e.image, err)
			}
		} else {
			// Pull from registry (default behavior)
			if err := e.pullImage(bgCtx); err != nil {
				log.Printf("Failed to pull image: %v", err)
				// Fallback to local build if pull fails
				log.Printf("Attempting to build image locally as fallback...")
				if buildErr := e.buildImage(bgCtx); buildErr != nil {
					resultErr = fmt.Errorf("failed to pull or build image %s: pull error: %v, build error: %w", e.image, err, buildErr)
				}
			}
		}

		e.imageReadyMu.Lock()
		if resultErr != nil {
			e.imageBuildErr = resultErr
			log.Printf("Image preparation failed: %v", resultErr)
		} else {
			e.imageReady = true
			log.Printf("Docker image %s is now ready!", e.image)
		}
		e.imageReadyMu.Unlock()
	}()
}

// pullImage pulls a Docker image from the registry.
func (e *DockerExecutor) pullImage(ctx context.Context) error {
	log.Printf("Pulling Docker image %s...", e.image)

	reader, err := e.client.ImagePull(ctx, e.image, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("failed to pull image %s: %w", e.image, err)
	}
	defer reader.Close()

	// Process pull output and log progress
	decoder := json.NewDecoder(reader)
	for {
		var event struct {
			Status         string `json:"status"`
			Progress       string `json:"progress"`
			ProgressDetail struct {
				Current int64 `json:"current"`
				Total   int64 `json:"total"`
			} `json:"progressDetail"`
			Error string `json:"error"`
		}
		if err := decoder.Decode(&event); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode pull output: %w", err)
		}
		if event.Error != "" {
			return fmt.Errorf("pull error: %s", event.Error)
		}
		if event.Status != "" {
			if event.Progress != "" {
				log.Printf("[docker pull] %s %s", event.Status, event.Progress)
			} else {
				log.Printf("[docker pull] %s", event.Status)
			}
		}
	}

	log.Printf("Successfully pulled image %s", e.image)
	return nil
}

// IsImageReady returns true if the Docker image is ready to use.
func (e *DockerExecutor) IsImageReady() bool {
	e.imageReadyMu.RLock()
	defer e.imageReadyMu.RUnlock()
	return e.imageReady
}

// ImageBuildError returns any error that occurred during image build.
func (e *DockerExecutor) ImageBuildError() error {
	e.imageReadyMu.RLock()
	defer e.imageReadyMu.RUnlock()
	return e.imageBuildErr
}

// WaitForImage blocks until the image is ready or context is cancelled.
func (e *DockerExecutor) WaitForImage(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if e.IsImageReady() {
				return nil
			}
			if err := e.ImageBuildError(); err != nil {
				return err
			}
		}
	}
}

// buildImage builds the Docker image from the embedded Dockerfile.
func (e *DockerExecutor) buildImage(ctx context.Context) error {
	log.Printf("Building Docker image %s...", e.image)

	// Create a tar archive containing the Dockerfile
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	// Get Dockerfile content (from disk or default)
	dockerfileBytes, err := getDockerfileContent()
	if err != nil {
		return fmt.Errorf("failed to get Dockerfile content: %w", err)
	}
	header := &tar.Header{
		Name: "Dockerfile",
		Mode: 0644,
		Size: int64(len(dockerfileBytes)),
	}
	if err := tw.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write tar header: %w", err)
	}
	if _, err := tw.Write(dockerfileBytes); err != nil {
		return fmt.Errorf("failed to write Dockerfile to tar: %w", err)
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("failed to close tar writer: %w", err)
	}

	// Build the image
	buildOptions := types.ImageBuildOptions{
		Tags:       []string{e.image},
		Dockerfile: "Dockerfile",
		Remove:     true,
		NoCache:    false,
	}

	response, err := e.client.ImageBuild(ctx, &buf, buildOptions)
	if err != nil {
		return fmt.Errorf("failed to start image build: %w", err)
	}
	defer response.Body.Close()

	// Process build output and capture image ID
	var imageID string
	decoder := json.NewDecoder(response.Body)
	for {
		var event struct {
			Stream string `json:"stream"`
			Error  string `json:"error"`
			Aux    *struct {
				ID string `json:"ID"`
			} `json:"aux"`
		}
		if err := decoder.Decode(&event); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode build output: %w", err)
		}
		if event.Error != "" {
			return fmt.Errorf("build error: %s", event.Error)
		}
		// Capture image ID from aux field
		if event.Aux != nil && event.Aux.ID != "" {
			imageID = event.Aux.ID
			log.Printf("[docker build] Built image ID: %s", imageID)
		}
		if event.Stream != "" {
			// Log build progress (trim newlines for cleaner output)
			msg := strings.TrimSpace(event.Stream)
			if msg != "" {
				log.Printf("[docker build] %s", msg)
				// Also try to capture image ID from stream (format: "Successfully built <id>")
				if strings.HasPrefix(msg, "Successfully built ") {
					imageID = strings.TrimPrefix(msg, "Successfully built ")
				}
			}
		}
	}

	log.Printf("Build completed, verifying image...")

	// Verify the image exists with the expected tag
	_, _, err = e.client.ImageInspectWithRaw(ctx, e.image)
	if err == nil {
		log.Printf("Successfully built and tagged Docker image %s", e.image)
		return nil
	}

	log.Printf("Image tag not found, attempting to tag manually...")

	// If we have an image ID but tag wasn't applied, tag it manually
	if imageID != "" {
		log.Printf("Tagging image %s as %s", imageID, e.image)
		if err := e.client.ImageTag(ctx, imageID, e.image); err != nil {
			return fmt.Errorf("failed to tag image %s as %s: %w", imageID, e.image, err)
		}

		// Verify again
		_, _, err = e.client.ImageInspectWithRaw(ctx, e.image)
		if err == nil {
			log.Printf("Successfully tagged Docker image %s", e.image)
			return nil
		}
		return fmt.Errorf("image tagging failed, still cannot find %s: %w", e.image, err)
	}

	return fmt.Errorf("build completed but image %s not found and no image ID captured", e.image)
}

// createAccessibleTempDir creates a temporary directory that is accessible to Docker VMs.
// On macOS with Colima/Lima/Docker Desktop, /var/folders is not mounted into the VM,
// but /Users is. So we create temp dirs under ~/.cache/cute-pandas/ instead.
func createAccessibleTempDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		// Fallback to system temp if we can't get home dir
		return os.MkdirTemp("", "pandas-exec-*")
	}

	// Create base cache directory
	cacheDir := filepath.Join(homeDir, ".cache", "cute-pandas", "tmp")
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		// Fallback to system temp
		return os.MkdirTemp("", "pandas-exec-*")
	}

	// Create unique temp directory within our cache dir
	return os.MkdirTemp(cacheDir, "exec-*")
}

// ValidateFilePaths validates that all file paths exist and are accessible.
func ValidateFilePaths(files []string) error {
	for _, f := range files {
		// Check for path traversal attempts
		clean := filepath.Clean(f)
		if strings.Contains(clean, "..") {
			return fmt.Errorf("access denied: path traversal detected in %s", f)
		}

		// Check file exists
		info, err := os.Stat(f)
		if os.IsNotExist(err) {
			return fmt.Errorf("file not found: %s", f)
		}
		if err != nil {
			return fmt.Errorf("cannot access file %s: %w", f, err)
		}

		// Only allow regular files
		if !info.Mode().IsRegular() {
			return fmt.Errorf("access denied: %s is not a regular file", f)
		}
	}
	return nil
}

// ExecuteScript executes a Python script in a Docker container with access to specified files.
func (e *DockerExecutor) ExecuteScript(ctx context.Context, script string, files []string, timeout time.Duration) (*ExecutionResult, error) {
	startTime := time.Now()

	// Check if image is ready
	if !e.IsImageReady() {
		if err := e.ImageBuildError(); err != nil {
			return &ExecutionResult{
				Error:    fmt.Sprintf("Docker image build failed: %v", err),
				ExitCode: 1,
				Duration: time.Since(startTime),
			}, nil
		}
		return &ExecutionResult{
			Error:    "Docker image is still being built. Please try again in a minute. (First startup requires building the pandas environment)",
			ExitCode: 1,
			Duration: time.Since(startTime),
		}, nil
	}

	// Validate files first
	if err := ValidateFilePaths(files); err != nil {
		return &ExecutionResult{
			Error:    err.Error(),
			ExitCode: 1,
			Duration: time.Since(startTime),
		}, nil
	}

	// Use provided timeout or default
	if timeout <= 0 {
		timeout = e.executionTimeout
	}

	// Create execution context with timeout
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Create temp directory for script and output
	// Use a directory under user's home to ensure it's accessible to Docker VMs (Colima/Lima/etc)
	tempDir, err := createAccessibleTempDir()
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Write script to temp file
	scriptPath := filepath.Join(tempDir, "script.py")
	if err := os.WriteFile(scriptPath, []byte(script), 0644); err != nil {
		return nil, fmt.Errorf("failed to write script file: %w", err)
	}

	// Create output directory
	outputDir := filepath.Join(tempDir, "output")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	// Build mounts
	mounts := []mount.Mount{
		{
			Type:     mount.TypeBind,
			Source:   scriptPath,
			Target:   "/script.py",
			ReadOnly: true,
		},
		{
			Type:     mount.TypeBind,
			Source:   outputDir,
			Target:   "/output",
			ReadOnly: false,
		},
	}

	// Mount input files
	for i, f := range files {
		absPath, err := filepath.Abs(f)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path for %s: %w", f, err)
		}
		mounts = append(mounts, mount.Mount{
			Type:     mount.TypeBind,
			Source:   absPath,
			Target:   fmt.Sprintf("/data/input_%d/%s", i, filepath.Base(f)),
			ReadOnly: true,
		})
	}

	// Calculate CPU quota (100000 = 1 CPU)
	cpuQuota := int64(e.cpuLimit * 100000)

	// Create container config
	containerConfig := &container.Config{
		Image:           e.image,
		Cmd:             []string{"/script.py"},
		WorkingDir:      "/",
		NetworkDisabled: e.networkDisabled,
		Env: []string{
			"PYTHONUNBUFFERED=1",
			"PYTHONDONTWRITEBYTECODE=1",
		},
	}

	hostConfig := &container.HostConfig{
		Mounts: mounts,
		Resources: container.Resources{
			Memory:   e.memoryLimit,
			CPUQuota: cpuQuota,
		},
		AutoRemove: false, // We'll remove manually after getting logs
	}

	// Create container
	resp, err := e.client.ContainerCreate(execCtx, containerConfig, hostConfig, nil, nil, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create container: %w", err)
	}
	containerID := resp.ID

	// Ensure container is removed
	defer func() {
		removeCtx, removeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer removeCancel()
		_ = e.client.ContainerRemove(removeCtx, containerID, container.RemoveOptions{Force: true})
	}()

	// Start container
	if err := e.client.ContainerStart(execCtx, containerID, container.StartOptions{}); err != nil {
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	// Wait for container to finish
	statusCh, errCh := e.client.ContainerWait(execCtx, containerID, container.WaitConditionNotRunning)

	var exitCode int64
	select {
	case err := <-errCh:
		if err != nil {
			if execCtx.Err() == context.DeadlineExceeded {
				// Kill the container on timeout
				_ = e.client.ContainerKill(context.Background(), containerID, "SIGKILL")
				return &ExecutionResult{
					Error:    fmt.Sprintf("execution timeout: script exceeded %v", timeout),
					ExitCode: 124, // Standard timeout exit code
					Duration: time.Since(startTime),
				}, nil
			}
			return nil, fmt.Errorf("container wait error: %w", err)
		}
	case status := <-statusCh:
		exitCode = status.StatusCode
	}

	// Get container logs
	logOptions := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	}
	logs, err := e.client.ContainerLogs(context.Background(), containerID, logOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to get container logs: %w", err)
	}
	defer logs.Close()

	// Separate stdout and stderr
	var stdout, stderr bytes.Buffer
	_, err = stdcopy.StdCopy(&stdout, &stderr, logs)
	if err != nil {
		return nil, fmt.Errorf("failed to read container logs: %w", err)
	}

	result := &ExecutionResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: int(exitCode),
		Duration: time.Since(startTime),
	}

	if exitCode != 0 {
		result.Error = fmt.Sprintf("script exited with code %d", exitCode)
	}

	return result, nil
}

// CopyFromContainer copies a file from a container to a local destination.
func (e *DockerExecutor) CopyFromContainer(ctx context.Context, containerID, srcPath string) ([]byte, error) {
	reader, _, err := e.client.CopyFromContainer(ctx, containerID, srcPath)
	if err != nil {
		return nil, fmt.Errorf("failed to copy from container: %w", err)
	}
	defer reader.Close()

	// The response is a tar archive
	tr := tar.NewReader(reader)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read tar: %w", err)
		}

		if header.Typeflag == tar.TypeReg {
			data, err := io.ReadAll(tr)
			if err != nil {
				return nil, fmt.Errorf("failed to read file from tar: %w", err)
			}
			return data, nil
		}
	}

	return nil, fmt.Errorf("file not found in container: %s", srcPath)
}

// BuildFileMapping creates a mapping from original file paths to container paths.
func BuildFileMapping(files []string) map[string]string {
	mapping := make(map[string]string)
	for i, f := range files {
		containerPath := fmt.Sprintf("/data/input_%d/%s", i, filepath.Base(f))
		mapping[f] = containerPath
	}
	return mapping
}
