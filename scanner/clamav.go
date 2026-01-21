// SPDX-License-Identifier: MPL-2.0
// Copyright 2026 Sagacient <sagacient@gmail.com>
//
// See CONTRIBUTORS.md for full contributor list.

// Package scanner provides malware scanning capabilities using ClamAV.
package scanner

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"sync"
)

// ScanResult holds the result of a malware scan.
type ScanResult struct {
	Clean    bool   // True if file is clean
	Threat   string // Name of detected threat (empty if clean)
	Error    error  // Error during scanning (nil if successful)
	Scanned  bool   // True if scan was actually performed
}

// Scanner provides malware scanning using ClamAV.
type Scanner struct {
	enabled     bool
	failOpen    bool   // If true, allow uploads when scanner fails
	clamdSocket string // Path to clamd socket (optional)
	mu          sync.Mutex
	available   bool
	checkedOnce bool
}

// Config holds scanner configuration.
type Config struct {
	Enabled     bool   // Enable/disable scanning
	FailOpen    bool   // If true, allow uploads when scanner unavailable
	ClamdSocket string // Optional: path to clamd socket
}

// NewScanner creates a new ClamAV scanner.
func NewScanner(cfg Config) *Scanner {
	s := &Scanner{
		enabled:     cfg.Enabled,
		failOpen:    cfg.FailOpen,
		clamdSocket: cfg.ClamdSocket,
	}

	if s.enabled {
		// Check if ClamAV is available on startup
		s.checkAvailability()
	}

	return s
}

// checkAvailability verifies that ClamAV is installed and running.
func (s *Scanner) checkAvailability() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.checkedOnce {
		return
	}
	s.checkedOnce = true

	// Try clamdscan first (faster, uses daemon)
	if s.clamdSocket != "" {
		cmd := exec.Command("clamdscan", "--version")
		if err := cmd.Run(); err == nil {
			s.available = true
			log.Printf("ClamAV scanner available (clamdscan with socket: %s)", s.clamdSocket)
			return
		}
	}

	// Try clamdscan without specific socket
	cmd := exec.Command("clamdscan", "--version")
	if err := cmd.Run(); err == nil {
		s.available = true
		log.Printf("ClamAV scanner available (clamdscan)")
		return
	}

	// Fallback to clamscan (slower, standalone)
	cmd = exec.Command("clamscan", "--version")
	if err := cmd.Run(); err == nil {
		s.available = true
		log.Printf("ClamAV scanner available (clamscan - standalone mode)")
		return
	}

	s.available = false
	log.Printf("WARNING: ClamAV not available. Scanning will be %s",
		map[bool]string{true: "skipped (fail-open mode)", false: "rejected (fail-closed mode)"}[s.failOpen])
}

// IsEnabled returns whether scanning is enabled.
func (s *Scanner) IsEnabled() bool {
	return s.enabled
}

// IsAvailable returns whether ClamAV is available.
func (s *Scanner) IsAvailable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.available
}

// Scan scans a file for malware.
// Returns ScanResult with:
// - Clean=true if file is safe
// - Clean=false, Threat=name if malware detected
// - Error if scanning failed
func (s *Scanner) Scan(filePath string) ScanResult {
	if !s.enabled {
		return ScanResult{Clean: true, Scanned: false}
	}

	s.mu.Lock()
	available := s.available
	s.mu.Unlock()

	if !available {
		if s.failOpen {
			log.Printf("WARNING: ClamAV unavailable, allowing file without scan: %s", filePath)
			return ScanResult{Clean: true, Scanned: false}
		}
		return ScanResult{
			Clean:   false,
			Error:   fmt.Errorf("malware scanner unavailable and fail-open is disabled"),
			Scanned: false,
		}
	}

	// Try clamdscan first (uses daemon, faster)
	result := s.scanWithClamdscan(filePath)
	if result.Error != nil {
		// Fallback to clamscan if daemon not responding
		log.Printf("clamdscan failed, trying clamscan: %v", result.Error)
		result = s.scanWithClamscan(filePath)
	}

	return result
}

// scanWithClamdscan scans using the clamd daemon.
func (s *Scanner) scanWithClamdscan(filePath string) ScanResult {
	args := []string{"--no-summary", "--infected"}
	if s.clamdSocket != "" {
		args = append(args, "--socket="+s.clamdSocket)
	}
	args = append(args, filePath)

	cmd := exec.Command("clamdscan", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	output := stdout.String()

	// Exit code 0 = clean, 1 = infected, 2 = error
	if exitErr, ok := err.(*exec.ExitError); ok {
		exitCode := exitErr.ExitCode()
		if exitCode == 1 {
			// Infected - parse threat name
			threat := parseThreatName(output)
			log.Printf("MALWARE DETECTED in %s: %s", filePath, threat)
			return ScanResult{Clean: false, Threat: threat, Scanned: true}
		} else if exitCode == 2 {
			// Error
			return ScanResult{
				Error:   fmt.Errorf("clamdscan error: %s", strings.TrimSpace(stderr.String())),
				Scanned: false,
			}
		}
	} else if err != nil {
		return ScanResult{Error: fmt.Errorf("failed to run clamdscan: %w", err), Scanned: false}
	}

	// Clean
	log.Printf("File scanned clean: %s", filePath)
	return ScanResult{Clean: true, Scanned: true}
}

// scanWithClamscan scans using standalone clamscan (slower but no daemon needed).
func (s *Scanner) scanWithClamscan(filePath string) ScanResult {
	cmd := exec.Command("clamscan", "--no-summary", "--infected", filePath)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	output := stdout.String()

	// Exit code 0 = clean, 1 = infected, 2 = error
	if exitErr, ok := err.(*exec.ExitError); ok {
		exitCode := exitErr.ExitCode()
		if exitCode == 1 {
			// Infected
			threat := parseThreatName(output)
			log.Printf("MALWARE DETECTED in %s: %s", filePath, threat)
			return ScanResult{Clean: false, Threat: threat, Scanned: true}
		} else if exitCode == 2 {
			// Error
			return ScanResult{
				Error:   fmt.Errorf("clamscan error: %s", strings.TrimSpace(stderr.String())),
				Scanned: false,
			}
		}
	} else if err != nil {
		return ScanResult{Error: fmt.Errorf("failed to run clamscan: %w", err), Scanned: false}
	}

	// Clean
	log.Printf("File scanned clean: %s", filePath)
	return ScanResult{Clean: true, Scanned: true}
}

// parseThreatName extracts the threat name from ClamAV output.
// Format: "/path/to/file: ThreatName FOUND"
func parseThreatName(output string) string {
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasSuffix(line, "FOUND") {
			// Extract threat name between : and FOUND
			parts := strings.Split(line, ":")
			if len(parts) >= 2 {
				threat := strings.TrimSpace(parts[len(parts)-1])
				threat = strings.TrimSuffix(threat, "FOUND")
				threat = strings.TrimSpace(threat)
				return threat
			}
		}
	}
	return "Unknown threat"
}

// ErrMalwareDetected is returned when malware is found in a file.
type ErrMalwareDetected struct {
	Threat   string
	FilePath string
}

func (e *ErrMalwareDetected) Error() string {
	return fmt.Sprintf("malware detected: %s", e.Threat)
}

// ErrScannerUnavailable is returned when the scanner is not available.
type ErrScannerUnavailable struct{}

func (e *ErrScannerUnavailable) Error() string {
	return "malware scanner unavailable"
}
