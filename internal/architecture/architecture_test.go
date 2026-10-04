// Package architecture contains executable architecture fitness tests (the
// Go equivalent of ArchUnit, via github.com/arch-go/arch-go) that enforce
// this service's hexagonal dependency rule: dependencies point inward only,
// domain is at the center, and only cmd/ wires every layer together.
package architecture

import (
	"testing"

	archgo "github.com/arch-go/arch-go/api"
	"github.com/arch-go/arch-go/api/configuration"
)

// modulePath is this repo's Go module path, from go.mod.
const modulePath = "github.com/claudioed/wes-work-planning"

// Package glob patterns for arch-go, matching this repo's real layout:
// internal/domain/*, internal/application/{ports,usecases}, and
// internal/adapters/{inbound,outbound}/*.
const (
	domainPackages       = "**.internal.domain.**"
	applicationPackages  = "**.internal.application.**"
	inboundPackages      = "**.internal.adapters.inbound.**"
	outboundPackages     = "**.internal.adapters.outbound.**"
	allInternalPackages  = "**.internal.**"
	cmdPackages          = "**.cmd.**"
	applicationPortsPkgs = "**.internal.application.ports.**"
)

// runDependenciesRule executes a single arch-go dependencies rule and fails
// the test with the offending package(s)/detail(s) if it doesn't pass.
func runDependenciesRule(t *testing.T, rule *configuration.DependenciesRule) {
	t.Helper()

	moduleInfo := configuration.Load(modulePath)
	cfg := configuration.Config{
		DependenciesRules: []*configuration.DependenciesRule{rule},
	}

	result := archgo.CheckArchitecture(moduleInfo, cfg)

	if result.DependenciesRuleResult != nil && result.DependenciesRuleResult.Passes {
		return
	}

	for _, ruleResult := range result.DependenciesRuleResult.Results {
		for _, v := range ruleResult.Verifications {
			if v.Passes {
				continue
			}

			for _, d := range v.Details {
				t.Errorf("%s", archViolation("dependency", "package "+v.Package, d))
			}
		}
	}

	t.FailNow()
}

// TestHexagonalDependencyRule encodes the strict dependency rule from
// CLAUDE.md: domain depends on nothing, application depends only on domain,
// inbound and outbound adapters never depend on each other, and only cmd/
// is allowed to wire together every layer.
func TestHexagonalDependencyRule(t *testing.T) {
	t.Run("domain has no internal dependencies except domain", func(t *testing.T) {
		runDependenciesRule(t, &configuration.DependenciesRule{
			Package: domainPackages,
			ShouldOnlyDependsOn: &configuration.Dependencies{
				Internal: []string{domainPackages},
			},
		})
	})

	t.Run("application depends only on domain", func(t *testing.T) {
		runDependenciesRule(t, &configuration.DependenciesRule{
			Package: applicationPackages,
			ShouldOnlyDependsOn: &configuration.Dependencies{
				Internal: []string{domainPackages, applicationPackages},
			},
		})
	})

	t.Run("inbound adapters do not depend on outbound adapters", func(t *testing.T) {
		runDependenciesRule(t, &configuration.DependenciesRule{
			Package: inboundPackages,
			ShouldNotDependsOn: &configuration.Dependencies{
				Internal: []string{outboundPackages},
			},
		})
	})

	t.Run("outbound adapters do not depend on inbound adapters", func(t *testing.T) {
		runDependenciesRule(t, &configuration.DependenciesRule{
			Package: outboundPackages,
			ShouldNotDependsOn: &configuration.Dependencies{
				Internal: []string{inboundPackages},
			},
		})
	})

	t.Run("only cmd wires every layer", func(t *testing.T) {
		runDependenciesRule(t, &configuration.DependenciesRule{
			Package: allInternalPackages,
			ShouldNotDependsOn: &configuration.Dependencies{
				Internal: []string{cmdPackages},
			},
		})
	})
}

// TestApplicationPortsContainOnlyInterfaces encodes an existing convention
// of this codebase: internal/application/ports defines driven-port
// interfaces for adapters to implement, and must never itself contain a
// struct, function, or method (that would mean a port stopped being a pure
// contract and started leaking implementation into the application layer).
func TestApplicationPortsContainOnlyInterfaces(t *testing.T) {
	moduleInfo := configuration.Load(modulePath)
	cfg := configuration.Config{
		ContentRules: []*configuration.ContentsRule{
			{
				Package:                     applicationPortsPkgs,
				ShouldOnlyContainInterfaces: true,
			},
		},
	}

	result := archgo.CheckArchitecture(moduleInfo, cfg)

	if result.ContentsRuleResult != nil && result.ContentsRuleResult.Passes {
		return
	}

	for _, ruleResult := range result.ContentsRuleResult.Results {
		for _, v := range ruleResult.Verifications {
			if v.Passes {
				continue
			}

			for _, d := range v.Details {
				t.Errorf("%s", archViolation("dependency", "package "+v.Package, d))
			}
		}
	}

	t.FailNow()
}
