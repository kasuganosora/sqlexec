package packet_parsers

import (
	"testing"

	"github.com/kasuganosora/sqlexec/server/protocol"
)

func TestPingParser_CommandAndName(t *testing.T) {
	p := NewPingPacketParser()
	if p.Command() != protocol.COM_PING {
		t.Errorf("Command = 0x%02x, want 0x%02x", p.Command(), protocol.COM_PING)
	}
	if p.Name() != "COM_PING" {
		t.Errorf("Name = %q, want %q", p.Name(), "COM_PING")
	}
}

func TestPingParser_Parse(t *testing.T) {
	p := NewPingPacketParser()
	pkt := &protocol.Packet{Payload: []byte{protocol.COM_PING}}
	result, err := p.Parse(pkt)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if _, ok := result.(*protocol.ComPingPacket); !ok {
		t.Errorf("Parse result type = %T, want *ComPingPacket", result)
	}
}

func TestQuitParser_CommandAndName(t *testing.T) {
	p := NewQuitPacketParser()
	if p.Command() != protocol.COM_QUIT {
		t.Errorf("Command = 0x%02x, want 0x%02x", p.Command(), protocol.COM_QUIT)
	}
	if p.Name() != "COM_QUIT" {
		t.Errorf("Name = %q, want %q", p.Name(), "COM_QUIT")
	}
}

func TestQuitParser_Parse(t *testing.T) {
	p := NewQuitPacketParser()
	pkt := &protocol.Packet{Payload: []byte{protocol.COM_QUIT}}
	result, err := p.Parse(pkt)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if _, ok := result.(*protocol.ComQuitPacket); !ok {
		t.Errorf("Parse result type = %T, want *ComQuitPacket", result)
	}
}

func TestSetOptionParser_CommandAndName(t *testing.T) {
	p := NewSetOptionPacketParser()
	if p.Command() != protocol.COM_SET_OPTION {
		t.Errorf("Command = 0x%02x, want 0x%02x", p.Command(), protocol.COM_SET_OPTION)
	}
	if p.Name() != "COM_SET_OPTION" {
		t.Errorf("Name = %q, want %q", p.Name(), "COM_SET_OPTION")
	}
}

func TestSetOptionParser_Parse(t *testing.T) {
	p := NewSetOptionPacketParser()
	pkt := &protocol.Packet{Payload: []byte{protocol.COM_SET_OPTION, 0x00, 0x00}}
	result, err := p.Parse(pkt)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if _, ok := result.(*protocol.ComSetOptionPacket); !ok {
		t.Errorf("Parse result type = %T, want *ComSetOptionPacket", result)
	}
}

func TestQueryParser_CommandAndName(t *testing.T) {
	p := NewQueryPacketParser()
	if p.Command() != protocol.COM_QUERY {
		t.Errorf("Command = 0x%02x, want 0x%02x", p.Command(), protocol.COM_QUERY)
	}
	if p.Name() != "COM_QUERY" {
		t.Errorf("Name = %q, want %q", p.Name(), "COM_QUERY")
	}
}

func TestQueryParser_Parse(t *testing.T) {
	p := NewQueryPacketParser()
	pkt := &protocol.Packet{Payload: []byte{protocol.COM_QUERY, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1'}}
	result, err := p.Parse(pkt)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if _, ok := result.(*protocol.ComQueryPacket); !ok {
		t.Errorf("Parse result type = %T, want *ComQueryPacket", result)
	}
}

func TestInitDBParser_CommandAndName(t *testing.T) {
	p := NewInitDBPacketParser()
	if p.Command() != protocol.COM_INIT_DB {
		t.Errorf("Command = 0x%02x, want 0x%02x", p.Command(), protocol.COM_INIT_DB)
	}
	if p.Name() != "COM_INIT_DB" {
		t.Errorf("Name = %q, want %q", p.Name(), "COM_INIT_DB")
	}
}

func TestInitDBParser_Parse(t *testing.T) {
	p := NewInitDBPacketParser()
	pkt := &protocol.Packet{Payload: []byte{protocol.COM_INIT_DB, 't', 'e', 's', 't'}}
	result, err := p.Parse(pkt)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if _, ok := result.(*protocol.ComInitDBPacket); !ok {
		t.Errorf("Parse result type = %T, want *ComInitDBPacket", result)
	}
}

func TestFieldListParser_CommandAndName(t *testing.T) {
	p := NewFieldListPacketParser()
	if p.Command() != protocol.COM_FIELD_LIST {
		t.Errorf("Command = 0x%02x, want 0x%02x", p.Command(), protocol.COM_FIELD_LIST)
	}
	if p.Name() != "COM_FIELD_LIST" {
		t.Errorf("Name = %q, want %q", p.Name(), "COM_FIELD_LIST")
	}
}

func TestFieldListParser_Parse(t *testing.T) {
	p := NewFieldListPacketParser()
	pkt := &protocol.Packet{Payload: []byte{protocol.COM_FIELD_LIST, 't', 'e', 's', 't'}}
	result, err := p.Parse(pkt)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if _, ok := result.(*protocol.ComFieldListPacket); !ok {
		t.Errorf("Parse result type = %T, want *ComFieldListPacket", result)
	}
}

func TestProcessKillParser_CommandAndName(t *testing.T) {
	p := NewProcessKillPacketParser()
	if p.Command() != protocol.COM_PROCESS_KILL {
		t.Errorf("Command = 0x%02x, want 0x%02x", p.Command(), protocol.COM_PROCESS_KILL)
	}
	if p.Name() != "COM_PROCESS_KILL" {
		t.Errorf("Name = %q, want %q", p.Name(), "COM_PROCESS_KILL")
	}
}

func TestProcessKillParser_Parse(t *testing.T) {
	p := NewProcessKillPacketParser()
	pkt := &protocol.Packet{Payload: []byte{protocol.COM_PROCESS_KILL, 0x01, 0x00, 0x00, 0x00}}
	result, err := p.Parse(pkt)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if _, ok := result.(*protocol.ComProcessKillPacket); !ok {
		t.Errorf("Parse result type = %T, want *ComProcessKillPacket", result)
	}
}
