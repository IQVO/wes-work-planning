package architecture

// Reserved-event sensor (fleet gap fix D26).
//
// RateDeviationDetected is declared in internal/domain/shared/events.go and
// in apis/asyncapi.yaml, but no ADR defines WHEN it is raised (ADR-0003 and
// ADR-0020 both defer the rate rule). The contract therefore marks it
// "reserved: not emitted", and the analytics report documents its column as
// always 0. This sensor keeps those documents true: the moment production
// code constructs the event, it fails and asks for the ADR and the doc
// updates that must ship with the rule.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

const reservedEvent = "RateDeviationDetected"

// reservedEventConstructions returns the positions in f where the reserved
// event is built: a NewRateDeviationDetected(...) call or a
// RateDeviationDetected{...} literal.
func reservedEventConstructions(fset *token.FileSet, f *ast.File) []string {
	var out []string
	ast.Inspect(f, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CallExpr:
			if callName(node) == "New"+reservedEvent {
				out = append(out, fset.Position(node.Pos()).String())
			}
		case *ast.CompositeLit:
			if typeName(node.Type) == reservedEvent {
				out = append(out, fset.Position(node.Pos()).String())
			}
		}
		return true
	})
	return out
}

// typeName returns T for T and pkg.T type expressions.
func typeName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		return t.Sel.Name
	}
	return ""
}

// callName returns the bare name of the called function: F for both
// F(...) and pkg.F(...).
func callName(call *ast.CallExpr) string {
	return typeName(call.Fun)
}

// TestRateDeviationDetectedStaysReserved fails when non-test code outside
// the event's own declaration raises RateDeviationDetected.
func TestRateDeviationDetectedStaysReserved(t *testing.T) {
	declaration := filepath.Join("internal", "domain", "shared", "events.go")
	for _, p := range goFiles(t, "internal", "cmd") {
		if strings.HasSuffix(p, declaration) {
			continue
		}
		fset, f := parseFile(t, p)
		for _, pos := range reservedEventConstructions(fset, f) {
			t.Errorf("%s", archViolation("contents", "RateDeviationDetected stays reserved",
				pos+": RateDeviationDetected is raised, but no ADR defines when (D26). FIX: write the ADR for the rate rule, "+
					"then drop the \"Reserved\" markers in apis/asyncapi.yaml, docs/docs/analytics/release-throughput-report.md, "+
					"docs/docs/ddd/domain-events.md and docs/docs/api/events.md, and remove this sensor"))
		}
	}
}

// TestRateDeviationDetectedSensorBites proves the sensor flags both ways of
// building the event and ignores unrelated events.
func TestRateDeviationDetectedSensorBites(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want int
	}{
		{"constructor call", `package x
func f() { _ = shared.NewRateDeviationDetected(p, now) }`, 1},
		{"composite literal", `package x
func f() { _ = shared.RateDeviationDetected{PathId: p} }`, 1},
		{"other event", `package x
func f() { _ = shared.NewPathThrottled(p, now) }`, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, "src.go", tc.src, 0)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := len(reservedEventConstructions(fset, f)); got != tc.want {
				t.Fatalf("flagged %d constructions, want %d", got, tc.want)
			}
		})
	}
}
