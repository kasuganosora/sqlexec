package session

import "regexp"

var traceIDPattern = regexp.MustCompile(`/\*\s*trace_id\s*=\s*([^\s*]+)\s*\*/`)

// ExtractTraceID extracts trace_id from SQL comment like /*trace_id=abc123*/
// Returns the trace ID and the SQL with the comment removed.
// If no trace_id comment is found, returns empty string and the original SQL.
func ExtractTraceID(sql string) (traceID string, cleanSQL string) {
	match := traceIDPattern.FindStringSubmatchIndex(sql)
	if match == nil {
		return "", sql
	}
	// match[2], match[3] is the capture group range
	traceID = sql[match[2]:match[3]]
	cleanSQL = sql[:match[0]] + sql[match[1]:]
	return traceID, cleanSQL
}
