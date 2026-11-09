package a2a

import (
	"fmt"
	"time"

	"github.com/claudioed/wes-work-planning/internal/application/usecases"
	"github.com/claudioed/wes-work-planning/internal/domain/workunit"
)

// problemTypeBase namespaces this context's problem types, mirroring the
// RFC 7807 practice of the HTTP adapter (ADR-0005).
const problemTypeBase = "https://wes-work-planning.invalid/problems"

// problem is the RFC 7807-shaped rejection an agent client receives instead
// of a result: domain rejections and request-shape failures are business
// answers, not transport errors, so they travel in the message envelope.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
	Status int    `json:"status"`
}

// summary renders the one-line form used for the reply's text part.
func (p *problem) summary() string {
	return fmt.Sprintf("%s (%d): %s", p.Title, p.Status, p.Detail)
}

func problemMalformedRequest(detail string) *problem {
	return &problem{Type: problemTypeBase + "/malformed-skill-request", Title: "Malformed skill request", Detail: detail, Status: 400}
}

func problemUnknownSkill(skill string) *problem {
	return &problem{Type: problemTypeBase + "/unknown-skill", Title: "Unknown skill", Detail: fmt.Sprintf("skill %q is not offered by this agent; the Agent Card lists the supported skills", skill), Status: 400}
}

func problemInvalidPathId(err error) *problem {
	return &problem{Type: problemTypeBase + "/invalid-path-id", Title: "Invalid pathId argument", Detail: err.Error(), Status: 400}
}

// envelope is the structured payload of every skill answer: exactly one of
// Data (a skill DTO) or Problem (why there is no result), plus the skill id
// the answer belongs to.
type envelope struct {
	Skill   string   `json:"skill"`
	Data    skillDTO `json:"data,omitempty"`
	Problem *problem `json:"problem,omitempty"`
}

// skillDTO is the union of the skill result DTOs. A2A parts are
// machine-readable JSON; each skill fills its own field and Summary, keeping
// the envelope flat and versioned as one contract.
type skillDTO struct {
	FlowStatus   *flowStatus   `json:"flowStatus,omitempty"`
	ReleasedWork *releasedWork `json:"releasedWork,omitempty"`
	summary      string
}

// Summary is the human-readable one-liner for the reply's text part.
func (s skillDTO) Summary() string { return s.summary }

// flowStatus is the composed read DTO for the flow_status skill: the
// SampleBacklog read model and the RebalanceDecision recommendation, side by
// side, so a delegating agent answers "how is this path doing and what should
// be done" in one exchange. Domain types never cross the boundary.
type flowStatus struct {
	PathId         string                  `json:"pathId"`
	Telemetry      backlogTelemetry        `json:"telemetry"`
	Recommendation rebalanceRecommendation `json:"recommendation"`
	summary        string
}

func (f flowStatus) Summary() string { return f.summary }

// backlogTelemetry mirrors the MCP tool DTO of the same read model.
type backlogTelemetry struct {
	PathId             string `json:"pathId"`
	BacklogDepth       int    `json:"backlogDepth"`
	WIP                int    `json:"wip"`
	Mode               string `json:"mode"`
	OverAlarmThreshold bool   `json:"overAlarmThreshold"`
}

// rebalanceRecommendation mirrors the MCP tool DTO of the same read model.
type rebalanceRecommendation struct {
	PathId       string `json:"pathId"`
	Action       string `json:"action"`
	BacklogDepth int    `json:"backlogDepth"`
	WIP          int    `json:"wip"`
}

// toFlowStatus composes the two read models into the skill DTO and its
// one-line summary.
func toFlowStatus(s usecases.BacklogSnapshot, r usecases.RebalanceRecommendation) flowStatus {
	telemetry := backlogTelemetry{
		PathId:             s.PathId.String(),
		BacklogDepth:       s.BacklogDepth,
		WIP:                s.WIP,
		Mode:               s.Mode,
		OverAlarmThreshold: s.OverAlarmThreshold,
	}
	rec := rebalanceRecommendation{
		PathId:       r.PathId.String(),
		Action:       r.Action.String(),
		BacklogDepth: r.BacklogDepth,
		WIP:          r.WIP,
	}
	alarm := ""
	if telemetry.OverAlarmThreshold {
		alarm = ", OVER ALARM"
	}
	return flowStatus{
		PathId:         telemetry.PathId,
		Telemetry:      telemetry,
		Recommendation: rec,
		summary: fmt.Sprintf("%s: backlog %d, WIP %d, %s%s; recommendation %s",
			telemetry.PathId, telemetry.BacklogDepth, telemetry.WIP, telemetry.Mode, alarm, rec.Action),
	}
}

// releasedWork is the write DTO for the release_next_work skill: the minimal
// view of the admitted unit. Only the id, CPT, and external reference are
// exposed — the domain WorkUnit aggregate never leaks across the boundary,
// exactly as on the MCP surface.
type releasedWork struct {
	WorkUnitId string `json:"workUnitId"`
	CPT        string `json:"cpt"`
	Ref        string `json:"ref"`
	summary    string
}

func (r releasedWork) Summary() string { return r.summary }

// toReleasedWork maps the released domain WorkUnit into the DTO.
func toReleasedWork(u *workunit.WorkUnit) releasedWork {
	dto := releasedWork{
		WorkUnitId: u.Id(),
		CPT:        u.CPT().Time().UTC().Format(time.RFC3339),
		Ref:        u.Reference(),
	}
	dto.summary = fmt.Sprintf("released %s on %s (CPT %s)", dto.WorkUnitId, dto.Ref, dto.CPT)
	return dto
}
