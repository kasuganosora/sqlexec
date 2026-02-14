package utils

import "strings"

// ParseRemoteAddr parses remote address in format "ip:port" or "[ipv6]:port"
// Returns (ip, port)
func ParseRemoteAddr(remoteAddr string) (string, string) {
	// Handle IPv6 format: [ipv6]:port
	if len(remoteAddr) > 0 && remoteAddr[0] == '[' {
		// Find closing bracket
		closeBracket := strings.Index(remoteAddr, "]")
		if closeBracket == -1 {
			// Malformed, return as-is
			return remoteAddr, ""
		}
		ip := remoteAddr[:closeBracket+1] // Include brackets
		// Check for port after bracket
		if closeBracket+1 < len(remoteAddr) && remoteAddr[closeBracket+1] == ':' {
			return ip, remoteAddr[closeBracket+2:]
		}
		return ip, ""
	}

	// Handle IPv4 format: ip:port
	// Find last colon for port separation
	lastColon := strings.LastIndex(remoteAddr, ":")
	if lastColon == -1 {
		return remoteAddr, ""
	}
	return remoteAddr[:lastColon], remoteAddr[lastColon+1:]
}
