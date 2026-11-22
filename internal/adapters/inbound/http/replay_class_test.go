package http

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// (a) Every problem type declares exactly one replay class on its catalogue
// entry (ADR 0050). There is no runtime default: a new problem type is
// classified by the PR that adds it, or this test is red.
func TestEveryProblemTypeDeclaresAReplayClass(t *testing.T) {
	types := allProblemTypes()
	if len(types) == 0 {
		t.Fatal("the catalogue accessor returned no problem types: the test would pass on nothing")
	}
	classOf := map[string]replayClass{}
	for _, p := range types {
		t.Run(p.Slug, func(t *testing.T) {
			if !validReplayClass(p.Class) {
				t.Errorf("problem type %q (%d) declares no replay class (%q): set FINAL, OUTCOME or RETRYABLE on its catalogue entry (.claude/rules/fleet/idempotency-and-outbox.md)", p.Slug, p.Status, p.Class)
			}
			if p.Status >= http.StatusInternalServerError && p.Class != classRetryable {
				t.Errorf("problem type %q answers %d: a 5xx is never stored, so its class is RETRYABLE, got %q", p.Slug, p.Status, p.Class)
			}
			if prev, ok := classOf[p.Slug]; ok && prev != p.Class {
				t.Errorf("slug %q is declared %s and %s: one problem type has exactly one class", p.Slug, prev, p.Class)
			}
			classOf[p.Slug] = p.Class
		})
	}
}

// The check above must bite: an unclassified entry and a misspelt class are refused.
func TestTheUnclassifiedEntryCheckBites(t *testing.T) {
	for _, c := range []replayClass{"", "final", "STORED", "OUTCOME ", "Final"} {
		if validReplayClass(c) {
			t.Errorf("class %q is accepted; only FINAL, OUTCOME and RETRYABLE are classes", c)
		}
	}
	for _, c := range []replayClass{classFinal, classOutcome, classRetryable} {
		if !validReplayClass(c) {
			t.Errorf("class %q is refused", c)
		}
	}
}

// The decision the middleware takes, for every status and class: 2xx stored,
// 5xx never, the rest by the declared class, and a refusal without a class (or
// with a misspelt one) is released — the safe fallback of ADR 0050.
func TestStorableDecisionByStatusAndClass(t *testing.T) {
	cases := []struct {
		code  int
		class replayClass
		want  bool
	}{
		{http.StatusCreated, "", true},
		{http.StatusOK, classRetryable, true},
		{http.StatusInternalServerError, classFinal, false},
		{http.StatusInternalServerError, classOutcome, false},
		{http.StatusServiceUnavailable, "", false},
		{http.StatusBadRequest, classFinal, true},
		{http.StatusUnprocessableEntity, classFinal, true},
		{http.StatusConflict, classOutcome, true},
		{http.StatusBadRequest, classOutcome, true},
		{http.StatusNotFound, classFinal, true},
		{http.StatusGone, classFinal, true},
		{http.StatusConflict, classRetryable, false},
		{http.StatusNotFound, classRetryable, false},
		{http.StatusUnprocessableEntity, classRetryable, false},
		{http.StatusConflict, "", false},
		{http.StatusBadRequest, "final", false},
		{http.StatusMovedPermanently, "", false},
	}
	for _, c := range cases {
		rec := &keyCapture{ResponseRecorder: httptest.NewRecorder()}
		rec.markReplayClass(c.class)
		rec.WriteHeader(c.code)
		if got := rec.storable(); got != c.want {
			t.Errorf("status %d class %q: storable = %v, want %v", c.code, c.class, got, c.want)
		}
	}
}

// problem literals are the catalogue: they may only be written in the two
// files that declare the catalogue — errors.go (problemCatalog) and
// replay_class.go (standaloneProblems) — and each carries its class. A
// literal built at a call site would escape the unclassified-entry check
// above.
func TestEveryProblemLiteralIsInTheCatalogueAndClassified(t *testing.T) {
	literals := problemLiterals(t)
	if len(literals) == 0 {
		t.Fatal("no problem literal found: the scan would pass on nothing")
	}
	for _, l := range literals {
		if l.file != "errors.go" && l.file != "replay_class.go" {
			t.Errorf("%s: a problem literal outside the catalogue files: declare it there so it carries a class", l.pos)
		}
		if !literalDeclaresClass(l.lit) {
			t.Errorf("%s: this problem literal declares no class", l.pos)
		}
	}
}

type problemLiteral struct {
	file string
	pos  token.Position
	lit  *ast.CompositeLit
}

// problemLiterals finds every catalogue-shaped composite literal of the
// package's non-test files: an untyped (elided) literal with at least three
// positional elements whose second and third are strings — the
// problemCategory entry shape {err, slug, title, class} — or an untyped
// keyed literal naming both `slug` and `class` — the standaloneProblem
// shape. A typed problemDetails literal at a call site matches neither.
func problemLiterals(t *testing.T) []problemLiteral {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var found []problemLiteral
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok || lit.Type != nil || len(lit.Elts) == 0 {
				return true
			}
			if !isCatalogueLiteral(lit) {
				return true
			}
			found = append(found, problemLiteral{file: name, pos: fset.Position(lit.Pos()), lit: lit})
			return true
		})
	}
	return found
}

// isCatalogueLiteral reports whether lit has the shape of a catalogue entry
// (see problemLiterals).
func isCatalogueLiteral(lit *ast.CompositeLit) bool {
	positional := 0
	keyed := map[string]bool{}
	for _, e := range lit.Elts {
		if kv, ok := e.(*ast.KeyValueExpr); ok {
			if k, ok := kv.Key.(*ast.Ident); ok {
				keyed[k.Name] = true
			}
			continue
		}
		positional++
	}
	if keyed["slug"] && keyed["class"] {
		return true // standaloneProblem
	}
	if positional >= 3 {
		if s2, ok := lit.Elts[1].(*ast.BasicLit); ok && s2.Kind == token.STRING {
			if s3, ok := lit.Elts[2].(*ast.BasicLit); ok && s3.Kind == token.STRING {
				return true // problemCategory
			}
		}
	}
	return false
}

// literalDeclaresClass: a keyed literal names `class`; a positional
// problemCategory literal has a fourth element.
func literalDeclaresClass(lit *ast.CompositeLit) bool {
	positional := 0
	for _, e := range lit.Elts {
		if kv, ok := e.(*ast.KeyValueExpr); ok {
			if k, ok := kv.Key.(*ast.Ident); ok && k.Name == "class" {
				return true
			}
			continue
		}
		positional++
	}
	return positional >= 4
}

// The ADR's class table is the human copy of the catalogue: one row per slug,
// same class (ADR 0050).
func TestTheADRClassTableMatchesTheCatalogue(t *testing.T) {
	raw, err := os.ReadFile("../../../../docs/docs/adr/0050-every-problem-type-has-a-replay-class.md")
	if err != nil {
		t.Fatalf("the ADR with the class table: %v", err)
	}
	row := regexp.MustCompile("(?m)^\\| `([a-z0-9-]+)` \\| (\\d{3}) \\| (FINAL|OUTCOME|RETRYABLE) \\|")
	inADR := map[string]string{}
	for _, m := range row.FindAllStringSubmatch(string(raw), -1) {
		if _, dup := inADR[m[1]]; dup {
			t.Errorf("slug %q is listed twice in the ADR table", m[1])
		}
		inADR[m[1]] = m[3]
	}
	inCode := map[string]string{}
	for _, p := range allProblemTypes() {
		inCode[p.Slug] = string(p.Class)
	}
	for slug, class := range inCode {
		if got, ok := inADR[slug]; !ok {
			t.Errorf("problem type %q (%s) is missing from the ADR table", slug, class)
		} else if got != class {
			t.Errorf("problem type %q is %s in the catalogue and %s in the ADR", slug, class, got)
		}
	}
	for slug := range inADR {
		if _, ok := inCode[slug]; !ok {
			t.Errorf("the ADR lists %q, which is not a problem type of the catalogue", slug)
		}
	}
}

// classFor and the catalogue agree, and the internal-error fallback is
// RETRYABLE (a 5xx is never stored; the class is nominal).
func TestClassForMatchesTheCatalogue(t *testing.T) {
	for _, e := range problemCatalog {
		if got := classFor(e.err); got != e.class {
			t.Errorf("classFor(%v) = %q, the catalogue entry declares %q", e.err, got, e.class)
		}
	}
	if got := classFor(errNothingMatchesThis); got != classRetryable {
		t.Errorf("classOf the fallback = %q, want RETRYABLE", got)
	}
}

// errNothingMatchesThis matches no catalogue entry: the internal-error
// fallback of classFor.
var errNothingMatchesThis = errors.New("no catalogue entry matches this sentinel")
