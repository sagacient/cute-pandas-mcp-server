# Cute Pandas MCP Server

An MCP (Model Context Protocol) server that provides isolated pandas/Python execution environments for data analysis tasks.

## Features

- **Isolated Execution**: Each request runs in a fresh Docker container
- **File Access Control**: Only explicitly provided files are accessible
- **Worker Pool**: Configurable concurrent execution limits
- **Security**: Network disabled, resource limits, non-root execution
- **Full Pandas Support**: pandas, numpy, scipy, scikit-learn, and more
- **HTTP File Upload**: Upload files via HTTP endpoints in HTTP transport mode

## Prerequisites

- Go 1.23+
- A container runtime (any of the following):
  - Docker Desktop
  - Colima
  - Lima
  - Podman
  - Rancher Desktop

The server **auto-detects** common Docker socket locations, so it works out of the box with most setups.

## Quick Start

### 1. Build the Server

```bash
go mod tidy
go build -o cute-pandas-server .
```

### 2. Run the Server

**Default: Pull from Docker Hub (Instant Start)**

The server uses a pre-built Docker Hub image by default for instant startup:

```bash
./cute-pandas-server
```

**Override: Local Build**

To build from `CutePandas.Dockerfile` instead of pulling:

```bash
BUILD_LOCAL=true ./cute-pandas-server
```

> **Note:** Local build may take a few minutes the first time as it downloads Python and installs pandas dependencies.

**Stdio mode (for MCP clients like Claude Desktop):**
```bash
./cute-pandas-server
```

**HTTP mode (with file upload support):**
```bash
./cute-pandas-server -t http
# or
TRANSPORT=http ./cute-pandas-server

# With custom storage settings
TRANSPORT=http UPLOAD_TTL=2h MAX_UPLOAD_SIZE=209715200 ./cute-pandas-server
```

## Configuration

Configure via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_WORKERS` | 5 | Maximum concurrent container executions |
| `QUEUE_SIZE` | 10 | Max pending requests in queue |
| `ACQUIRE_TIMEOUT` | 30s | Time to wait for an available worker |
| `EXECUTION_TIMEOUT` | 60s | Max script execution time |
| `MAX_MEMORY_MB` | 512 | Memory limit per container in MB |
| `MAX_CPU` | 1.0 | CPU limit per container |
| `DOCKER_IMAGE` | cutepandas/cutepandas:latest | Docker image to use |
| `BUILD_LOCAL` | false | Set to `true` to build from `CutePandas.Dockerfile` instead of pulling |
| `NETWORK_DISABLED` | true | Disable network in containers |
| `TRANSPORT` | stdio | Transport type: stdio or http |
| `HTTP_PORT` | 8080 | Port for HTTP transport |
| `STORAGE_DIR` | `~/.cache/cute-pandas/uploads` (native) or `/storage` (Docker) | Directory for uploaded files |
| `UPLOAD_TTL` | `1h` | Auto-delete uploaded files after this duration (e.g., `30m`, `2h`) |
| `MAX_UPLOAD_SIZE` | `104857600` (100MB) | Maximum upload file size in bytes |
| `SCAN_UPLOADS` | `true` | Enable ClamAV malware scanning for uploaded files |
| `SCAN_ON_FAIL` | `reject` | Behavior when scanner unavailable: `reject` or `allow` |

## MCP Tools

### `run_pandas_script`

Execute arbitrary pandas/Python code with access to specified files.

```json
{
  "script": "import pandas as pd\ndf = pd.read_csv(resolve_path('/path/to/data.csv'))\nprint(df.describe())",
  "files": ["/path/to/data.csv"],
  "timeout": 60
}
```

**Helper functions available in scripts:**
- `resolve_path(original_path)` - Convert original file path to container path
- `save_output(df, filename, format='csv')` - Save DataFrame to output directory
- `FILE_MAPPING` - Dictionary of original paths to container paths

### `read_dataframe`

Read a file and return DataFrame information.

```json
{
  "file_path": "/path/to/data.csv",
  "preview_rows": 10
}
```

**Returns:** Shape, columns, dtypes, memory usage, null counts, and preview rows.

### `analyze_data`

Perform statistical analysis on a dataset.

```json
{
  "file_path": "/path/to/data.csv",
  "analysis_type": "describe",
  "columns": ["col1", "col2"],
  "group_by": "category"
}
```

**Analysis types:**
- `describe` - Statistical summary
- `info` - DataFrame info (shape, types, memory, nulls)
- `corr` - Correlation matrix (numeric columns)
- `value_counts` - Value counts for each column
- `groupby` - Group by analysis (requires `group_by` parameter)

### `transform_data`

Apply transformations to a dataset.

```json
{
  "input_file": "/path/to/data.csv",
  "operations": [
    {"type": "filter", "column": "age", "operator": ">", "value": 18},
    {"type": "select", "columns": ["name", "age", "city"]},
    {"type": "sort", "column": "age", "ascending": false}
  ],
  "output_format": "csv"
}
```

**Supported operations:**
- `filter` - Filter rows: `{column, operator, value}`
  - Operators: `==`, `!=`, `>`, `>=`, `<`, `<=`, `contains`, `isin`
- `select` - Select columns: `{columns: [...]}`
- `drop` - Drop columns: `{columns: [...]}`
- `sort` - Sort rows: `{column, ascending}`
- `rename` - Rename columns: `{mapping: {old: new}}`
- `dropna` - Drop null values: `{subset: [...]}` (optional)
- `fillna` - Fill null values: `{column, fill_value}`
- `head` - Take first N rows: `{n}`
- `tail` - Take last N rows: `{n}`
- `sample` - Random sample: `{n}` or `{frac}`
- `unique` - Remove duplicates: `{columns: [...]}` (optional)

### `server_status`

Get server health and worker pool statistics.

```json
{}
```

## HTTP Mode File Upload (HTTP Transport Only)

When running in HTTP mode (`TRANSPORT=http`), the server provides REST endpoints for file upload and management. This allows remote clients to upload files that can then be referenced in MCP tool calls.

### Storage Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/storage/upload` | POST | Upload a file (multipart/form-data) |
| `/storage/list` | GET | List all uploaded files |
| `/storage/download/{id}` | GET | Download a file by ID |
| `/storage/delete/{id}` | DELETE | Delete a file by ID |
| `/health` | GET | Server health check |

### Upload Example

```bash
# Upload a file
curl -X POST http://localhost:8080/storage/upload \
  -F "file=@/path/to/sales.csv"
```

**Response:**
```json
{
  "id": "a1b2c3d4e5f6...",
  "name": "sales.csv",
  "size": 1024,
  "uploaded_at": "2026-01-21T10:00:00Z",
  "expires_at": "2026-01-21T11:00:00Z",
  "file_ref": "upload://a1b2c3d4e5f6..."
}
```

### Using Uploaded Files in Tool Calls

Use the `file_ref` value (e.g., `upload://a1b2c3d4e5f6...`) in any file path parameter:

```json
{
  "tool": "read_dataframe",
  "arguments": {
    "file_path": "upload://a1b2c3d4e5f6...",
    "preview_rows": 10
  }
}
```

### List Uploaded Files

```bash
curl http://localhost:8080/storage/list
```

**Response:**
```json
{
  "files": [
    {
      "id": "a1b2c3d4e5f6...",
      "name": "sales.csv",
      "size": 1024,
      "uploaded_at": "2026-01-21T10:00:00Z",
      "expires_at": "2026-01-21T11:00:00Z",
      "file_ref": "upload://a1b2c3d4e5f6..."
    }
  ],
  "count": 1
}
```

### Delete a File

```bash
curl -X DELETE http://localhost:8080/storage/delete/a1b2c3d4e5f6...
```

### Automatic Cleanup

Uploaded files are automatically deleted after the TTL expires (default: 1 hour). Configure with `UPLOAD_TTL` environment variable:

```bash
# Keep files for 30 minutes
UPLOAD_TTL=30m TRANSPORT=http ./cute-pandas-server

# Keep files for 2 hours
UPLOAD_TTL=2h TRANSPORT=http ./cute-pandas-server
```

### Malware Scanning

When running in Docker, uploaded files are automatically scanned for malware using ClamAV before being stored. This helps protect against malicious files being uploaded and processed.

**Scan Results:**

| Result | HTTP Status | Description |
|--------|-------------|-------------|
| Clean | 201 Created | File stored successfully |
| Infected | 422 Unprocessable Entity | Malware detected, file rejected |
| Scanner Error | 503 Service Unavailable | Scanner unavailable (if `SCAN_ON_FAIL=reject`) |

**Malware Detection Response:**
```json
{
  "error": "Malware detected",
  "threat": "Win.Trojan.Agent-123456",
  "status": 422
}
```

**Configuration:**

```bash
# Disable scanning (not recommended for production)
SCAN_UPLOADS=false TRANSPORT=http ./cute-pandas-server

# Allow uploads when scanner is unavailable (fail-open)
SCAN_ON_FAIL=allow TRANSPORT=http ./cute-pandas-server
```

**Notes:**
- ClamAV is embedded in the Docker image
- Virus definitions are updated on container startup
- Scanning adds a small latency to uploads (typically <1s for most files)
- For native (non-Docker) deployments, install ClamAV separately

## MCP Client Integration

### Option 1: Native Binary

Add to your MCP client config (e.g., `~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "cute-pandas": {
      "command": "/path/to/cute-pandas-server",
      "args": []
    }
  }
}
```

### Option 2: Docker Container

First, build the Docker image:

```bash
docker build -t cute-pandas-mcp-server .
```

Then configure your MCP client:

**For Docker Desktop (Linux/Windows):**
```json
{
  "mcpServers": {
    "cute-pandas": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm",
        "-v", "/var/run/docker.sock:/var/run/docker.sock",
        "-v", "/Users/user/:/Users/user/:ro",
        "cute-pandas-mcp-server"
      ]
    }
  }
}
```

**For Colima (macOS):**
```json
{
  "mcpServers": {
    "cute-pandas": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm",
        "-v", "/Users/user//.colima/default/docker.sock:/var/run/docker.sock",
        "-v", "/Users/user/:/Users/user/:ro",
        "cute-pandas-mcp-server"
      ]
    }
  }
}
```

> **Note:** Replace `/Users/user/` with your actual home directory path if your MCP client doesn't expand environment variables (e.g., `/Users/yourname` on macOS, `/home/yourname` on Linux).

**Required mounts:**
| Mount | Purpose |
|-------|---------|
| Docker socket | Allows server to create pandas execution containers |
| `/Users/user/:/Users/user/:ro` | Makes your files accessible to pandas scripts (read-only) |

**Flags explained:**
- `-i` = Interactive mode (required for stdio transport)
- `--rm` = Remove container when done
- `:ro` = Read-only mount for security

## Example Usage

### Reading and Analyzing a CSV

```
User: Read the sales data from ~/data/sales.csv and show me a summary

Claude: [Uses read_dataframe tool]
The dataset has 10,000 rows and 8 columns:
- date (datetime64)
- product (object)
- quantity (int64)
- price (float64)
...
```

### Custom Analysis

```
User: Calculate the average sales by product category

Claude: [Uses run_pandas_script tool]
import pandas as pd
df = pd.read_csv(resolve_path('~/data/sales.csv'))
result = df.groupby('category')['sales'].mean()
print(result)
```

### Data Transformation

```
User: Filter sales > $100 and sort by date

Claude: [Uses transform_data tool]
{
  "input_file": "~/data/sales.csv",
  "operations": [
    {"type": "filter", "column": "sales", "operator": ">", "value": 100},
    {"type": "sort", "column": "date", "ascending": true}
  ]
}
```

## Security

- **File Isolation**: Only files explicitly listed in the request are mounted
- **Network Disabled**: Containers cannot access the network
- **Resource Limits**: CPU and memory limits enforced
- **Non-Root**: Scripts run as non-root user
- **Read-Only Mounts**: Input files cannot be modified
- **Path Sanitization**: Path traversal attacks are blocked
- **Ephemeral Containers**: Destroyed after each request

## Development

### Running Tests

```bash
go test ./...
```

### Building for Different Platforms

```bash
# Linux
GOOS=linux GOARCH=amd64 go build -o cute-pandas-server-linux .

# macOS (Intel)
GOOS=darwin GOARCH=amd64 go build -o cute-pandas-server-darwin-amd64 .

# macOS (Apple Silicon)
GOOS=darwin GOARCH=arm64 go build -o cute-pandas-server-darwin-arm64 .

# Windows
GOOS=windows GOARCH=amd64 go build -o cute-pandas-server.exe .
```

## License

[Mozilla Public License 2.0 (MPL-2.0)](LICENCE)
