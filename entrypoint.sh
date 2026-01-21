#!/bin/bash
set -e

# Entrypoint script for Cute Pandas MCP Server
# Starts ClamAV daemon in background and immediately starts MCP server
# All output goes to stderr to avoid corrupting STDIO JSON communication

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1" >&2
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1" >&2
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

# Start ClamAV in background if scanning is enabled (non-blocking)
if [ "${SCAN_UPLOADS}" = "true" ] || [ "${SCAN_UPLOADS}" = "1" ]; then
    log_info "Malware scanning enabled - starting ClamAV in background"
    
    # Run ClamAV initialization entirely in background
    (
        # Update virus definitions
        freshclam --stdout 2>&1 | while read line; do
            echo "[freshclam] $line" >&2
        done
        
        # Ensure proper permissions
        chown -R clamav:clamav /var/lib/clamav /var/run/clamav 2>/dev/null || true
        
        # Start clamd daemon
        clamd 2>&1 >&2 &
        
        # Wait for it to be ready (logging only)
        for i in $(seq 1 120); do
            if clamdscan --ping 2>/dev/null; then
                log_info "ClamAV daemon ready"
                break
            fi
            sleep 1
        done
    ) &
    
    log_info "ClamAV initializing in background (scans will work once ready)"
else
    log_info "Malware scanning disabled (SCAN_UPLOADS=${SCAN_UPLOADS})"
fi

# Start the MCP server immediately (don't wait for ClamAV)
log_info "Starting Cute Pandas MCP Server..."
exec ./cute-pandas-server "$@"
