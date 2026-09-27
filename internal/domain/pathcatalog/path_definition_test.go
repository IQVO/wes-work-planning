package pathcatalog_test

import (
	"errors"
	"testing"

	"github.com/claudioed/wes-work-planning/internal/domain/pathcatalog"
)

func fleetCatalogue() *pathcatalog.Catalogue {
	return pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}},
		{Id: "PACK", MatchPrefix: "pack", RequiredCapabilities: []string{"pack"}},
		{Id: "REBIN", MatchPrefix: "rebin", RequiredCapabilities: []string{"rebin"}},
		{Id: "SLAM", MatchPrefix: "slam", RequiredCapabilities: []string{"slam"}},
	})
}

func TestCatalogue_Lookup_UnknownPathId_ReturnsError(t *testing.T) {
	cat := pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}},
	})
	if _, err := cat.Lookup("rebin"); !errors.Is(err, pathcatalog.ErrUnknownPath) {
		t.Fatalf("want ErrUnknownPath, got %v", err)
	}
}

func TestCatalogue_Lookup_KnownPathId_ReturnsDefinition(t *testing.T) {
	cat := pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}},
	})
	def, err := cat.Lookup("PICK")
	if err != nil || def.Id != "PICK" {
		t.Fatalf("unexpected lookup result: %+v, err=%v", def, err)
	}
}

func TestCatalogue_Lookup_AllFourDeclaredPaths(t *testing.T) {
	cat := fleetCatalogue()
	for _, want := range []string{"PICK", "PACK", "REBIN", "SLAM"} {
		def, err := cat.Lookup(want)
		if err != nil {
			t.Fatalf("Lookup(%q): unexpected error %v", want, err)
		}
		if def.Id != want {
			t.Fatalf("Lookup(%q): got id %q", want, def.Id)
		}
	}
}

// The real regression this test exists to prevent (see
// fulfillment-execution's ADR-0017 addendum): actual path_id values in
// this fleet are not the bare canonical id.
func TestCatalogue_Lookup_RealFleetPathIdVariants(t *testing.T) {
	cat := fleetCatalogue()

	cases := []struct{ pathId, wantId string }{
		{"pick", "PICK"},
		{"pick-zone-a", "PICK"},
		{"pick-soak", "PICK"},
		{"pick-t5-imbalance", "PICK"},
		{"pack-soak", "PACK"},
		{"PICK", "PICK"},
		{"Pick-Zone-B", "PICK"},
	}
	for _, tc := range cases {
		def, err := cat.Lookup(tc.pathId)
		if err != nil {
			t.Fatalf("Lookup(%q): unexpected error %v", tc.pathId, err)
		}
		if def.Id != tc.wantId {
			t.Fatalf("Lookup(%q): got id %q, want %q", tc.pathId, def.Id, tc.wantId)
		}
	}
}

func TestCatalogue_Lookup_DoesNotMatchBareSubstringPrefix(t *testing.T) {
	cat := fleetCatalogue()
	if _, err := cat.Lookup("picking-station"); !errors.Is(err, pathcatalog.ErrUnknownPath) {
		t.Fatalf("want ErrUnknownPath, got %v", err)
	}
}

func TestCatalogue_Ids_ReturnsEveryDeclaredId(t *testing.T) {
	cat := pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}},
		{Id: "PACK", MatchPrefix: "pack", RequiredCapabilities: []string{"pack"}},
	})
	if len(cat.Ids()) != 2 {
		t.Fatalf("expected 2 ids, got %v", cat.Ids())
	}
}

func TestCatalogue_EmptyCatalogue_EveryLookupFails(t *testing.T) {
	cat := pathcatalog.New(nil)
	if _, err := cat.Lookup("pick"); !errors.Is(err, pathcatalog.ErrUnknownPath) {
		t.Fatalf("want ErrUnknownPath, got %v", err)
	}
}

// Two declared paths with an EQUAL-length MatchPrefix that both match the
// same id is not a realistic fleet catalogue (loader validation rejects
// duplicate prefixes upstream), but Lookup's own tie-break rule must
// still be deterministic rather than depend on map/slice iteration
// order: the FIRST-declared matching definition wins ties, matching a
// literal "first one registered, in declaration order" reading of "the
// LONGEST matching prefix wins" when two are equally long.
func TestCatalogue_Lookup_TiedPrefixLength_FirstDeclaredWins(t *testing.T) {
	cat := pathcatalog.New([]pathcatalog.PathDefinition{
		{Id: "FIRST", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}},
		{Id: "SECOND", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}},
	})
	def, err := cat.Lookup("pick")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if def.Id != "FIRST" {
		t.Fatalf("expected the first-declared definition to win a length tie, got %q", def.Id)
	}
}

// TestCatalogue_Lookup_SkipsEmptyMatchPrefix proves an undeclared
// (empty-string) MatchPrefix can never match — not even the empty id —
// and never shadows or outranks a real declaration: Lookup treats it as
// absent rather than as a zero-length prefix that matches everything.
func TestCatalogue_Lookup_SkipsEmptyMatchPrefix(t *testing.T) {
	cases := []struct {
		name    string
		defs    []pathcatalog.PathDefinition
		lookup  string
		wantId  string
		wantErr error
	}{
		{
			name: "empty prefix does not shadow or outrank a real declaration",
			defs: []pathcatalog.PathDefinition{
				{Id: "UNDECLARED", MatchPrefix: "", RequiredCapabilities: []string{"pick"}},
				{Id: "PICK", MatchPrefix: "pick", RequiredCapabilities: []string{"pick"}},
			},
			lookup: "pick",
			wantId: "PICK",
		},
		{
			name: "empty prefix does not match the empty id",
			defs: []pathcatalog.PathDefinition{
				{Id: "UNDECLARED", MatchPrefix: "", RequiredCapabilities: []string{"pick"}},
			},
			lookup:  "",
			wantErr: pathcatalog.ErrUnknownPath,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cat := pathcatalog.New(tc.defs)
			def, err := cat.Lookup(tc.lookup)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("want %v, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if def.Id != tc.wantId {
				t.Fatalf("got id %q, want %q", def.Id, tc.wantId)
			}
		})
	}
}
