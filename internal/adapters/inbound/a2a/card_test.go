package a2a

import (
	"encoding/json"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

// TestCardShape pins the identity, interface, and skill surface of the Agent
// Card: the discovery contract other contexts program against.
func TestCardShape(t *testing.T) {
	card := Card(Config{AgentURL: "http://wes-work-planning-a2a.warehouse-systems.svc.cluster.local:8093/", Version: "1.0.0"})

	if card.Name != "wes-work-planning" {
		t.Errorf("card name = %q, want wes-work-planning", card.Name)
	}
	if card.Version != "1.0.0" {
		t.Errorf("card version = %q, want 1.0.0", card.Version)
	}
	if len(card.SupportedInterfaces) != 1 {
		t.Fatalf("card has %d interfaces, want 1", len(card.SupportedInterfaces))
	}
	iface := card.SupportedInterfaces[0]
	if iface.URL != "http://wes-work-planning-a2a.warehouse-systems.svc.cluster.local:8093/" {
		t.Errorf("interface url = %q, want the configured agent URL", iface.URL)
	}
	if iface.ProtocolBinding != a2a.TransportProtocolJSONRPC {
		t.Errorf("interface protocol = %q, want JSONRPC", iface.ProtocolBinding)
	}
	if card.Capabilities.Streaming {
		t.Error("streaming must not be advertised: this is a message-only agent")
	}
	if card.SecuritySchemes != nil {
		t.Error("card must declare no security schemes: the surface is unauthenticated (ADR-0016)")
	}
	if card.Provider == nil || card.Provider.Org != "IQVO" {
		t.Errorf("provider = %+v, want IQVO", card.Provider)
	}
}

// TestCardSkills pins the curated two-skill surface (ADR-0046): the curation
// discipline of ADR-0008 — goal-shaped skills, not one per endpoint.
func TestCardSkills(t *testing.T) {
	card := Card(Config{AgentURL: "http://localhost:8093/", Version: "1.0.0"})

	want := map[string]bool{skillFlowStatus: false, skillReleaseNextWork: false}
	for _, s := range card.Skills {
		if _, known := want[s.ID]; !known {
			t.Errorf("unexpected skill %q on the card", s.ID)
		}
		want[s.ID] = true
		if s.Name == "" || s.Description == "" {
			t.Errorf("skill %q has an empty name or description", s.ID)
		}
		if len(s.Examples) == 0 {
			t.Errorf("skill %q advertises no example", s.ID)
		}
		// Every example must be valid JSON of the wire contract, and must
		// name its own skill — a card that lies about its contract is worse
		// than no card.
		for _, ex := range s.Examples {
			var req skillRequest
			if err := json.Unmarshal([]byte(ex), &req); err != nil {
				t.Errorf("skill %q example is not valid skill-request JSON: %v", s.ID, err)
				continue
			}
			if req.Skill != s.ID {
				t.Errorf("skill %q example names skill %q", s.ID, req.Skill)
			}
		}
	}
	for id, seen := range want {
		if !seen {
			t.Errorf("skill %q missing from the card", id)
		}
	}
}

// TestCardSerializable proves the card marshals to the wire JSON other
// frameworks' resolvers expect at /.well-known/agent-card.json.
func TestCardSerializable(t *testing.T) {
	card := Card(Config{AgentURL: "http://localhost:8093/", Version: "1.0.0"})
	raw, err := json.Marshal(card)
	if err != nil {
		t.Fatalf("marshal card: %v", err)
	}
	var back a2a.AgentCard
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal card: %v\n%s", err, raw)
	}
	if back.Name != card.Name || len(back.Skills) != len(card.Skills) {
		t.Errorf("card did not round-trip: %+v", back)
	}
}
