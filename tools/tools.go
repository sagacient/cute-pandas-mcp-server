// SPDX-License-Identifier: MPL-2.0
// Copyright 2026 Sagacient <sagacient@gmail.com>
//
// See CONTRIBUTORS.md for full contributor list.

// Package tools provides MCP tool definitions and handlers for pandas operations.
package tools

import (
	"context"
	"github.com/sagacient/cute-pandas-mcp-server/executor"
	"github.com/sagacient/cute-pandas-mcp-server/storage"
	"github.com/sagacient/cute-pandas-mcp-server/workerpool"
	"fmt"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

// PandasTools holds the tools and their dependencies.
type PandasTools struct {
	pool      *workerpool.Pool
	executor  *executor.DockerExecutor
	fileStore *storage.FileStore // Optional, for HTTP mode upload:// resolution
}

// NewPandasTools creates a new PandasTools instance.
func NewPandasTools(pool *workerpool.Pool, exec *executor.DockerExecutor) *PandasTools {
	return &PandasTools{
		pool:     pool,
		executor: exec,
	}
}

// SetFileStore sets the file store for upload:// URI resolution.
// This should be called when running in HTTP mode.
func (t *PandasTools) SetFileStore(fs *storage.FileStore) {
	t.fileStore = fs
}

// resolveFilePath resolves a file path, handling upload:// URIs.
func (t *PandasTools) resolveFilePath(path string) (string, error) {
	if !strings.HasPrefix(path, "upload://") {
		return path, nil
	}

	if t.fileStore == nil {
		return "", fmt.Errorf("upload:// URIs are only supported in HTTP mode")
	}

	return t.fileStore.ResolveUploadURI(path)
}

// resolveFilePaths resolves multiple file paths.
func (t *PandasTools) resolveFilePaths(paths []string) ([]string, error) {
	resolved := make([]string, len(paths))
	for i, p := range paths {
		r, err := t.resolveFilePath(p)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve path %q: %w", p, err)
		}
		resolved[i] = r
	}
	return resolved, nil
}

// RunScriptTool returns the run_pandas_script tool definition.
func RunScriptTool() mcp.Tool {
	return mcp.NewTool("run_pandas_script",
		mcp.WithDescription("Execute a pandas/Python script with access to specified files. The script has access to pandas, numpy, and common data science libraries."),
		mcp.WithString("script",
			mcp.Required(),
			mcp.Description("Python code to execute. Use resolve_path(original_path) to get container paths for mounted files. Use save_output(df, filename) to save results."),
		),
		mcp.WithArray("files",
			mcp.Required(),
			mcp.Description("List of file paths to mount (read-only). These files will be accessible in the script via resolve_path()."),
			mcp.Items(map[string]interface{}{"type": "string"}),
		),
		mcp.WithNumber("timeout",
			mcp.Description("Maximum execution time in seconds (default: 60)"),
		),
	)
}

// RunScriptHandler handles the run_pandas_script tool.
func (t *PandasTools) RunScriptHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// Try to acquire a worker slot
	if err := t.pool.Acquire(ctx); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	defer t.pool.Release()

	// Extract arguments
	script, err := request.RequireString("script")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid parameter 'script': %v", err)), nil
	}

	filesArg := request.GetArguments()["files"]
	files, err := toStringSlice(filesArg)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid parameter 'files': %v", err)), nil
	}

	// Resolve upload:// URIs to actual paths
	resolvedFiles, err := t.resolveFilePaths(files)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	timeout := time.Duration(request.GetFloat("timeout", 60)) * time.Second

	// Build file mapping using original paths as keys for user reference
	fileMapping := make(map[string]string)
	for i, originalPath := range files {
		containerPath := fmt.Sprintf("/data/input_%d/%s", i, getBaseName(resolvedFiles[i]))
		fileMapping[originalPath] = containerPath
	}

	// Wrap the script with helpers
	wrappedScript := executor.WrapScript(script, fileMapping)

	// Execute with resolved paths
	result, err := t.executor.ExecuteScript(ctx, wrappedScript, resolvedFiles, timeout)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("execution error: %v", err)), nil
	}

	// Format output
	output := formatExecutionResult(result)
	return mcp.NewToolResultText(output), nil
}

// ReadDataFrameTool returns the read_dataframe tool definition.
func ReadDataFrameTool() mcp.Tool {
	return mcp.NewTool("read_dataframe",
		mcp.WithDescription("Read a file into a pandas DataFrame and return summary information including shape, columns, dtypes, memory usage, and a preview."),
		mcp.WithString("file_path",
			mcp.Required(),
			mcp.Description("Path to the data file (CSV, Excel, JSON, or Parquet)"),
		),
		mcp.WithNumber("preview_rows",
			mcp.Description("Number of rows to preview (default: 5)"),
		),
	)
}

// ReadDataFrameHandler handles the read_dataframe tool.
func (t *PandasTools) ReadDataFrameHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// Try to acquire a worker slot
	if err := t.pool.Acquire(ctx); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	defer t.pool.Release()

	// Extract arguments
	filePath, err := request.RequireString("file_path")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid parameter 'file_path': %v", err)), nil
	}

	// Resolve upload:// URI if needed
	resolvedPath, err := t.resolveFilePath(filePath)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	previewRows := int(request.GetFloat("preview_rows", 5))
	if previewRows < 1 {
		previewRows = 5
	}

	// Build file mapping
	files := []string{resolvedPath}
	fileMapping := executor.BuildFileMapping(files)
	containerPath := fileMapping[resolvedPath]

	// Generate script
	script := executor.ReadDataFrameScript(containerPath, previewRows)

	// Execute
	result, err := t.executor.ExecuteScript(ctx, script, files, 0)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("execution error: %v", err)), nil
	}

	output := formatExecutionResult(result)
	return mcp.NewToolResultText(output), nil
}

// AnalyzeDataTool returns the analyze_data tool definition.
func AnalyzeDataTool() mcp.Tool {
	return mcp.NewTool("analyze_data",
		mcp.WithDescription("Perform statistical analysis on a dataset. Supports describe, info, correlation, value counts, and groupby operations."),
		mcp.WithString("file_path",
			mcp.Required(),
			mcp.Description("Path to the data file"),
		),
		mcp.WithString("analysis_type",
			mcp.Required(),
			mcp.Description("Type of analysis to perform"),
			mcp.Enum("describe", "info", "corr", "value_counts", "groupby"),
		),
		mcp.WithArray("columns",
			mcp.Description("Specific columns to analyze (optional, defaults to all)"),
			mcp.Items(map[string]interface{}{"type": "string"}),
		),
		mcp.WithString("group_by",
			mcp.Description("Column to group by (required for groupby analysis)"),
		),
	)
}

// AnalyzeDataHandler handles the analyze_data tool.
func (t *PandasTools) AnalyzeDataHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// Try to acquire a worker slot
	if err := t.pool.Acquire(ctx); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	defer t.pool.Release()

	// Extract arguments
	filePath, err := request.RequireString("file_path")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid parameter 'file_path': %v", err)), nil
	}

	// Resolve upload:// URI if needed
	resolvedPath, err := t.resolveFilePath(filePath)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	analysisType, err := request.RequireString("analysis_type")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid parameter 'analysis_type': %v", err)), nil
	}

	var columns []string
	if colsArg := request.GetArguments()["columns"]; colsArg != nil {
		columns, _ = toStringSlice(colsArg)
	}

	groupBy := request.GetString("group_by", "")

	// Build file mapping
	files := []string{resolvedPath}
	fileMapping := executor.BuildFileMapping(files)
	containerPath := fileMapping[resolvedPath]

	// Generate script
	script := executor.AnalyzeDataScript(containerPath, analysisType, columns, groupBy)

	// Execute
	result, err := t.executor.ExecuteScript(ctx, script, files, 0)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("execution error: %v", err)), nil
	}

	output := formatExecutionResult(result)
	return mcp.NewToolResultText(output), nil
}

// TransformDataTool returns the transform_data tool definition.
func TransformDataTool() mcp.Tool {
	return mcp.NewTool("transform_data",
		mcp.WithDescription("Apply pandas transformations to a dataset and return the result. Supports filter, select, drop, sort, rename, dropna, fillna, and more."),
		mcp.WithString("input_file",
			mcp.Required(),
			mcp.Description("Path to the input data file"),
		),
		mcp.WithArray("operations",
			mcp.Required(),
			mcp.Description(`List of operations to apply. Each operation is an object with 'type' and type-specific parameters.
Supported operations:
- filter: {type: "filter", column: "col", operator: ">|<|==|!=|>=|<=|contains|isin", value: ...}
- select: {type: "select", columns: ["col1", "col2"]}
- drop: {type: "drop", columns: ["col1"]}
- sort: {type: "sort", column: "col", ascending: true/false}
- rename: {type: "rename", mapping: {"old": "new"}}
- dropna: {type: "dropna", subset: ["col1"]} (subset optional)
- fillna: {type: "fillna", column: "col", fill_value: 0} (column optional)
- head: {type: "head", n: 10}
- tail: {type: "tail", n: 10}
- sample: {type: "sample", n: 100} or {type: "sample", frac: 0.1}
- unique: {type: "unique", columns: ["col1"]} (columns optional)`),
			mcp.Items(map[string]interface{}{"type": "object"}),
		),
		mcp.WithString("output_format",
			mcp.Description("Output format: csv, json, or parquet (default: csv)"),
			mcp.Enum("csv", "json", "parquet"),
		),
	)
}

// TransformDataHandler handles the transform_data tool.
func (t *PandasTools) TransformDataHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// Try to acquire a worker slot
	if err := t.pool.Acquire(ctx); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	defer t.pool.Release()

	// Extract arguments
	inputFile, err := request.RequireString("input_file")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid parameter 'input_file': %v", err)), nil
	}

	// Resolve upload:// URI if needed
	resolvedPath, err := t.resolveFilePath(inputFile)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	opsArg := request.GetArguments()["operations"]
	operations, err := toOperations(opsArg)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid parameter 'operations': %v", err)), nil
	}

	outputFormat := request.GetString("output_format", "csv")

	// Build file mapping
	files := []string{resolvedPath}
	fileMapping := executor.BuildFileMapping(files)
	containerPath := fileMapping[resolvedPath]

	// Generate script
	script := executor.TransformDataScript(containerPath, operations, outputFormat)

	// Execute
	result, err := t.executor.ExecuteScript(ctx, script, files, 0)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("execution error: %v", err)), nil
	}

	output := formatExecutionResult(result)
	return mcp.NewToolResultText(output), nil
}

// Helper functions

func formatExecutionResult(result *executor.ExecutionResult) string {
	output := ""

	if result.Stdout != "" {
		output += result.Stdout
	}

	if result.Stderr != "" {
		if output != "" {
			output += "\n"
		}
		output += "=== Stderr ===\n" + result.Stderr
	}

	if result.Error != "" {
		if output != "" {
			output += "\n"
		}
		output += "=== Error ===\n" + result.Error
	}

	output += fmt.Sprintf("\n\n[Execution completed in %v with exit code %d]", result.Duration.Round(time.Millisecond), result.ExitCode)

	return output
}

func toStringSlice(v interface{}) ([]string, error) {
	if v == nil {
		return nil, fmt.Errorf("value is nil")
	}

	switch val := v.(type) {
	case []string:
		return val, nil
	case []interface{}:
		result := make([]string, len(val))
		for i, item := range val {
			str, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("item at index %d is not a string", i)
			}
			result[i] = str
		}
		return result, nil
	default:
		return nil, fmt.Errorf("expected array, got %T", v)
	}
}

func toOperations(v interface{}) ([]map[string]interface{}, error) {
	if v == nil {
		return nil, fmt.Errorf("value is nil")
	}

	switch val := v.(type) {
	case []map[string]interface{}:
		return val, nil
	case []interface{}:
		result := make([]map[string]interface{}, len(val))
		for i, item := range val {
			m, ok := item.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("item at index %d is not an object", i)
			}
			result[i] = m
		}
		return result, nil
	default:
		return nil, fmt.Errorf("expected array of objects, got %T", v)
	}
}

// getBaseName returns the base name of a file path.
func getBaseName(path string) string {
	// Handle both forward and backslashes
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == '\\' {
			return path[i+1:]
		}
	}
	return path
}

// ListOutputsTool returns the list_outputs tool definition.
func ListOutputsTool() mcp.Tool {
	return mcp.NewTool("list_outputs",
		mcp.WithDescription("List all execution outputs or files in a specific execution. Returns execution IDs with their output files."),
		mcp.WithString("exec_id",
			mcp.Description("Optional execution ID to list files for. If omitted, lists all executions."),
		),
	)
}

// ListOutputsHandler handles the list_outputs tool.
func (t *PandasTools) ListOutputsHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	outputManager := t.executor.GetOutputManager()
	if outputManager == nil {
		return mcp.NewToolResultError("Output management not configured. Set OUTPUT_DIR to enable output persistence."), nil
	}

	execID, _ := request.GetArguments()["exec_id"].(string)

	if execID != "" {
		// List files for specific execution
		files, err := outputManager.ListFiles(execID)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Failed to list files: %v", err)), nil
		}

		output := fmt.Sprintf("Files in execution %s:\n", execID)
		if len(files) == 0 {
			output += "  (no files)\n"
		} else {
			for _, f := range files {
				output += fmt.Sprintf("  - %s\n", f)
			}
		}
		return mcp.NewToolResultText(output), nil
	}

	// List all executions
	executions, err := outputManager.ListExecutions()
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Failed to list executions: %v", err)), nil
	}

	if len(executions) == 0 {
		return mcp.NewToolResultText("No executions found."), nil
	}

	output := fmt.Sprintf("Found %d execution(s):\n\n", len(executions))
	for _, exec := range executions {
		output += fmt.Sprintf("Execution: %s\n", exec.ExecutionID)
		output += fmt.Sprintf("  Created: %s\n", exec.CreatedAt.Format(time.RFC3339))
		output += fmt.Sprintf("  Expires: %s\n", exec.ExpiresAt.Format(time.RFC3339))
		output += fmt.Sprintf("  Path: %s\n", exec.OutputPath)
		if len(exec.Files) == 0 {
			output += "  Files: (none)\n"
		} else {
			output += fmt.Sprintf("  Files: %d\n", len(exec.Files))
			for _, f := range exec.Files {
				output += fmt.Sprintf("    - %s\n", f)
			}
		}
		output += "\n"
	}

	return mcp.NewToolResultText(output), nil
}

// GetOutputTool returns the get_output tool definition.
func GetOutputTool() mcp.Tool {
	return mcp.NewTool("get_output",
		mcp.WithDescription("Get the contents of an output file from an execution."),
		mcp.WithString("exec_id",
			mcp.Required(),
			mcp.Description("The execution ID containing the file."),
		),
		mcp.WithString("filename",
			mcp.Required(),
			mcp.Description("The name of the file to retrieve."),
		),
	)
}

// GetOutputHandler handles the get_output tool.
func (t *PandasTools) GetOutputHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	outputManager := t.executor.GetOutputManager()
	if outputManager == nil {
		return mcp.NewToolResultError("Output management not configured. Set OUTPUT_DIR to enable output persistence."), nil
	}

	execID, err := request.RequireString("exec_id")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid parameter 'exec_id': %v", err)), nil
	}

	filename, err := request.RequireString("filename")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("invalid parameter 'filename': %v", err)), nil
	}

	data, err := outputManager.GetFile(execID, filename)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Failed to get file: %v", err)), nil
	}

	// Return as text if it's text-like, otherwise indicate binary
	if isTextFile(filename) {
		return mcp.NewToolResultText(string(data)), nil
	}

	// For binary files, return base64 encoded or just metadata
	return mcp.NewToolResultText(fmt.Sprintf("Binary file: %s (%d bytes)\nExecution: %s\nFilename: %s", 
		filename, len(data), execID, filename)), nil
}

// DeleteOutputsTool returns the delete_outputs tool definition.
func DeleteOutputsTool() mcp.Tool {
	return mcp.NewTool("delete_outputs",
		mcp.WithDescription("Delete output files from an execution or all executions."),
		mcp.WithString("exec_id",
			mcp.Description("Optional execution ID to delete. If omitted, deletes ALL executions (use with caution)."),
		),
	)
}

// DeleteOutputsHandler handles the delete_outputs tool.
func (t *PandasTools) DeleteOutputsHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	outputManager := t.executor.GetOutputManager()
	if outputManager == nil {
		return mcp.NewToolResultError("Output management not configured. Set OUTPUT_DIR to enable output persistence."), nil
	}

	execID, _ := request.GetArguments()["exec_id"].(string)

	if execID != "" {
		// Delete specific execution
		if err := outputManager.DeleteExecution(execID); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Failed to delete execution: %v", err)), nil
		}
		return mcp.NewToolResultText(fmt.Sprintf("Successfully deleted execution %s", execID)), nil
	}

	// Delete all executions
	count, err := outputManager.DeleteAllExecutions()
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Failed to delete executions: %v", err)), nil
	}

	return mcp.NewToolResultText(fmt.Sprintf("Successfully deleted %d execution(s)", count)), nil
}

// isTextFile returns true if the file extension suggests a text file.
func isTextFile(filename string) bool {
	textExtensions := []string{".txt", ".csv", ".json", ".xml", ".html", ".md", ".py", ".log", ".yaml", ".yml"}
	lower := strings.ToLower(filename)
	for _, ext := range textExtensions {
		if strings.HasSuffix(lower, ext) {
			return true
		}
	}
	return false
}
