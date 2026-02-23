package response

import (
	"testing"

	"github.com/kasuganosora/sqlexec/server/protocol"
)

// --- OKBuilder ---

func TestNewOKBuilder(t *testing.T) {
	b := NewOKBuilder()
	if b == nil {
		t.Fatal("NewOKBuilder returned nil")
	}
}

func TestOKBuilder_Build(t *testing.T) {
	b := NewOKBuilder()
	pkt := b.Build(3, 10, 42, 1)

	if pkt.SequenceID != 3 {
		t.Errorf("SequenceID = %d, want 3", pkt.SequenceID)
	}
	if pkt.OkInPacket.Header != 0x00 {
		t.Errorf("Header = 0x%02x, want 0x00", pkt.OkInPacket.Header)
	}
	if pkt.OkInPacket.AffectedRows != 10 {
		t.Errorf("AffectedRows = %d, want 10", pkt.OkInPacket.AffectedRows)
	}
	if pkt.OkInPacket.LastInsertId != 42 {
		t.Errorf("LastInsertId = %d, want 42", pkt.OkInPacket.LastInsertId)
	}
	if pkt.OkInPacket.Warnings != 1 {
		t.Errorf("Warnings = %d, want 1", pkt.OkInPacket.Warnings)
	}
	if pkt.OkInPacket.StatusFlags != protocol.SERVER_STATUS_AUTOCOMMIT {
		t.Errorf("StatusFlags = %d, want SERVER_STATUS_AUTOCOMMIT(%d)", pkt.OkInPacket.StatusFlags, protocol.SERVER_STATUS_AUTOCOMMIT)
	}
}

func TestOKBuilder_Build_ZeroValues(t *testing.T) {
	b := NewOKBuilder()
	pkt := b.Build(0, 0, 0, 0)

	if pkt.SequenceID != 0 {
		t.Errorf("SequenceID = %d, want 0", pkt.SequenceID)
	}
	if pkt.OkInPacket.AffectedRows != 0 {
		t.Errorf("AffectedRows = %d, want 0", pkt.OkInPacket.AffectedRows)
	}
	if pkt.OkInPacket.LastInsertId != 0 {
		t.Errorf("LastInsertId = %d, want 0", pkt.OkInPacket.LastInsertId)
	}
	if pkt.OkInPacket.Warnings != 0 {
		t.Errorf("Warnings = %d, want 0", pkt.OkInPacket.Warnings)
	}
}

func TestOKBuilder_Build_MaxSequenceID(t *testing.T) {
	b := NewOKBuilder()
	pkt := b.Build(255, 1, 1, 0)
	if pkt.SequenceID != 255 {
		t.Errorf("SequenceID = %d, want 255", pkt.SequenceID)
	}
}

// --- EOFBuilder ---

func TestNewEOFBuilder(t *testing.T) {
	b := NewEOFBuilder()
	if b == nil {
		t.Fatal("NewEOFBuilder returned nil")
	}
}

func TestEOFBuilder_Build(t *testing.T) {
	b := NewEOFBuilder()
	pkt := b.Build(5, 2, protocol.SERVER_STATUS_AUTOCOMMIT)

	if pkt.SequenceID != 5 {
		t.Errorf("SequenceID = %d, want 5", pkt.SequenceID)
	}
	if pkt.Header != 0xFE {
		t.Errorf("Header = 0x%02x, want 0xFE", pkt.Header)
	}
	if pkt.Warnings != 2 {
		t.Errorf("Warnings = %d, want 2", pkt.Warnings)
	}
	if pkt.StatusFlags != protocol.SERVER_STATUS_AUTOCOMMIT {
		t.Errorf("StatusFlags = %d, want %d", pkt.StatusFlags, protocol.SERVER_STATUS_AUTOCOMMIT)
	}
}

func TestEOFBuilder_Build_ZeroValues(t *testing.T) {
	b := NewEOFBuilder()
	pkt := b.Build(0, 0, 0)

	if pkt.Header != 0xFE {
		t.Errorf("Header = 0x%02x, want 0xFE", pkt.Header)
	}
	if pkt.Warnings != 0 {
		t.Errorf("Warnings = %d, want 0", pkt.Warnings)
	}
	if pkt.StatusFlags != 0 {
		t.Errorf("StatusFlags = %d, want 0", pkt.StatusFlags)
	}
}

// --- ErrorBuilder ---

func TestNewErrorBuilder(t *testing.T) {
	b := NewErrorBuilder()
	if b == nil {
		t.Fatal("NewErrorBuilder returned nil")
	}
}

func TestErrorBuilder_Build(t *testing.T) {
	b := NewErrorBuilder()
	pkt := b.Build(1, 1045, "28000", "Access denied")

	if pkt.SequenceID != 1 {
		t.Errorf("SequenceID = %d, want 1", pkt.SequenceID)
	}
	if pkt.Header != 0xFF {
		t.Errorf("Header = 0x%02x, want 0xFF", pkt.Header)
	}
	if pkt.ErrorCode != 1045 {
		t.Errorf("ErrorCode = %d, want 1045", pkt.ErrorCode)
	}
	if pkt.SqlState != "28000" {
		t.Errorf("SqlState = %q, want %q", pkt.SqlState, "28000")
	}
	if pkt.SqlStateMarker != "#" {
		t.Errorf("SqlStateMarker = %q, want %q", pkt.SqlStateMarker, "#")
	}
	if pkt.ErrorMessage != "Access denied" {
		t.Errorf("ErrorMessage = %q, want %q", pkt.ErrorMessage, "Access denied")
	}
}

func TestErrorBuilder_Build_EmptySqlState(t *testing.T) {
	b := NewErrorBuilder()
	pkt := b.Build(2, 1000, "", "Unknown error")

	if pkt.SqlState != "" {
		t.Errorf("SqlState = %q, want empty", pkt.SqlState)
	}
	if pkt.SqlStateMarker != "" {
		t.Errorf("SqlStateMarker = %q, want empty (no marker when SqlState is empty)", pkt.SqlStateMarker)
	}
}

func TestErrorBuilder_Build_DifferentCodes(t *testing.T) {
	b := NewErrorBuilder()
	codes := []uint16{1000, 1045, 1064, 1146, 2002}
	for _, code := range codes {
		pkt := b.Build(0, code, "HY000", "test")
		if pkt.ErrorCode != code {
			t.Errorf("ErrorCode = %d, want %d", pkt.ErrorCode, code)
		}
	}
}

// --- ResultSetBuilder ---

func TestNewResultSetBuilder(t *testing.T) {
	b := NewResultSetBuilder()
	if b == nil {
		t.Fatal("NewResultSetBuilder returned nil")
	}
}

func TestBuildColumnCountPacket(t *testing.T) {
	tests := []struct {
		name       string
		sequenceID uint8
		count      uint64
		wantLen    int // total packet length = 4 (header) + encoded length
	}{
		{"zero", 0, 0, 5},         // 1-byte encoding
		{"one", 1, 1, 5},          // 1-byte encoding
		{"250", 2, 250, 5},        // 1-byte encoding (max for single byte)
		{"251", 3, 251, 7},        // 0xfc + 2 bytes
		{"65535", 4, 65535, 7},    // 0xfc + 2 bytes (max for 2-byte)
		{"65536", 5, 65536, 8},    // 0xfd + 3 bytes
		{"16M-1", 6, 16777215, 8}, // 0xfd + 3 bytes (max for 3-byte)
		{"16M", 7, 16777216, 13},  // 0xfe + 8 bytes
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pkt, err := BuildColumnCountPacket(tt.sequenceID, tt.count)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(pkt) != tt.wantLen {
				t.Errorf("packet length = %d, want %d", len(pkt), tt.wantLen)
			}
			// Verify sequence ID is at byte 3
			if pkt[3] != tt.sequenceID {
				t.Errorf("sequenceID in packet = %d, want %d", pkt[3], tt.sequenceID)
			}
		})
	}
}

func TestEncodeLengthEncodedInteger(t *testing.T) {
	tests := []struct {
		name      string
		value     uint64
		wantFirst byte
		wantLen   int
	}{
		{"zero", 0, 0x00, 1},
		{"one", 1, 0x01, 1},
		{"250", 250, 250, 1},
		{"251", 251, 0xfc, 3},
		{"1000", 1000, 0xfc, 3},
		{"65535", 65535, 0xfc, 3},
		{"65536", 65536, 0xfd, 4},
		{"16777215", 16777215, 0xfd, 4},
		{"16777216", 16777216, 0xfe, 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeLengthEncodedInteger(tt.value)
			if len(result) != tt.wantLen {
				t.Errorf("length = %d, want %d", len(result), tt.wantLen)
			}
			if result[0] != tt.wantFirst {
				t.Errorf("first byte = 0x%02x, want 0x%02x", result[0], tt.wantFirst)
			}
		})
	}
}

func TestEncodeLengthEncodedInteger_RoundTrip(t *testing.T) {
	// Verify that the encoded value can be decoded back correctly
	values := []uint64{0, 1, 250, 251, 1000, 65535, 65536, 16777215, 16777216, 1<<32 - 1}

	for _, v := range values {
		encoded := encodeLengthEncodedInteger(v)
		var decoded uint64

		switch {
		case encoded[0] < 251:
			decoded = uint64(encoded[0])
		case encoded[0] == 0xfc:
			decoded = uint64(encoded[1]) | uint64(encoded[2])<<8
		case encoded[0] == 0xfd:
			decoded = uint64(encoded[1]) | uint64(encoded[2])<<8 | uint64(encoded[3])<<16
		case encoded[0] == 0xfe:
			decoded = uint64(encoded[1]) | uint64(encoded[2])<<8 | uint64(encoded[3])<<16 |
				uint64(encoded[4])<<24 | uint64(encoded[5])<<32 | uint64(encoded[6])<<40 |
				uint64(encoded[7])<<48 | uint64(encoded[8])<<56
		}

		if decoded != v {
			t.Errorf("round-trip failed for %d: got %d", v, decoded)
		}
	}
}
