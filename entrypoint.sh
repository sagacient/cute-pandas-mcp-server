#!/bin/bash
set -e

# Entrypoint script for Cute Pandas MCP Server
# Starts ClamAV daemon (if scanning enabled) and then the MCP server

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Start ClamAV if scanning is enabled
if [ "${SCAN_UPLOADS}" = "true" ] || [ "${SCAN_UPLOADS}" = "1" ]; then
    log_info "Malware scanning is enabled"
    
    # Update virus definitions (non-blocking, run in background)
    log_info "Updating ClamAV virus definitions..."
    freshclam --stdout 2>&1 | while read line; do
        echo "[freshclam] $line"
    done &
    FRESHCLAM_PID=$!
    
    # Wait a moment for freshclam to start
    sleep 2
    
    # Start clamd daemon
    log_info "Starting ClamAV daemon..."
    
    # Ensure proper permissions
    chown -R clamav:clamav /var/lib/clamav /var/run/clamav 2>/dev/null || true
    
    # Start clamd in background
    clamd &
    CLAMD_PID=$!
    
    # Wait for clamd to be ready (up to 60 seconds)
    log_info "Waiting for ClamAV daemon to initialize..."
    for i in $(seq 1 60); do
        if clamdscan --ping 2>/dev/null; then
            log_info "ClamAV daemon is ready"
            break
        fi
        if [ $i -eq 60 ]; then
            log_warn "ClamAV daemon not ready after 60s, scanning may fail initially"
        fi
        sleep 1
    done
    
    # Wait for freshclam to finish (or timeout)
    if kill -0 $FRESHCLAM_PID 2>/dev/null; then
        log_info "Waiting for virus definition update to complete..."
        wait $FRESHCLAM_PID 2>/dev/null || true
    fi
    
    log_info "ClamAV initialization complete"
else
    log_info "Malware scanning is disabled (SCAN_UPLOADS=${SCAN_UPLOADS})"
fi

# Start the MCP server
log_info "Starting Cute Pandas MCP Server..."
exec ./cute-pandas-server "$@"
