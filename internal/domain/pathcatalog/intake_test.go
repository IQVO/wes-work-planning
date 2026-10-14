package pathcatalog

import "testing"

// ADR-0040 / fleet decision E5: a path admits a demand source when it
// declares no intake, or when its intake lists the source.
func TestPathDefinition_Admits(t *testing.T) {
	cases := []struct {
		name   string
		intake []string
		want   bool
	}{
		{"no intake declared", nil, true},
		{"intake lists the source", []string{"CUSTOMER_ORDER", IntakeSlotRelocation}, true},
		{"intake excludes the source", []string{"CUSTOMER_ORDER"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := PathDefinition{Id: "RELOCATE", MatchPrefix: "relocate", Intake: tc.intake}
			if got := d.Admits(IntakeSlotRelocation); got != tc.want {
				t.Fatalf("Admits = %v, want %v", got, tc.want)
			}
		})
	}
}
