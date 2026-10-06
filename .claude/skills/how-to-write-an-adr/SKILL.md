---
name: how-to-write-an-adr
description: Write an Architecture Decision Record in this repo's numbering and format, including companion ADRs for cross-repo changes. Use when a design decision should be recorded or a change contradicts an existing ADR.
---

# How to write an ADR

Use when a change is architecturally significant — a new bounded-context
integration, a reversal of a prior decision, a cross-repo contract change,
or anything a future reader would otherwise have to reverse-engineer from
the diff. Not every change needs one: a bug fix or a routine feature
addition inside an already-decided architecture doesn't.

## Numbering and location

`docs/docs/adr/NNNN-kebab-case-title.md`, four-digit zero-padded,
sequential — check the highest existing number
(`git ls-tree --name-only origin/develop -- docs/docs/adr/` and pick the
next integer, never reuse or guess). This repo's highest as of this guide
is `0018-path-capacity-changed.md` (18 ADRs total, `0001` through `0018`).
`docs/docs/adr/index.md` lists them; you don't need to touch it when adding
a new ADR — it's generated/maintained separately.

## Frontmatter (Docusaurus needs all fields)

```yaml
---
id: NNNN-kebab-case-title
slug: /adr/NNNN-kebab-case-title
title: NN. Title (a short noun phrase, matching the heading)
sidebar_label: NN. Short label for the nav sidebar
description: One or two sentences — this shows up in search and link
  previews, so make it stand alone without the rest of the doc.
---
```

`id`/`slug` are the full kebab-case filename (minus `.md`); `title`/
`sidebar_label` repeat the number as plain text (`"18. ..."`, not `#18`).
Watch the `sidebar_position` field specifically: ADRs 0001 through 0011 in
this repo carry it explicitly (e.g. `sidebar_position: 11` on
`0011-analytical-data-product.md`), but 0012 onward (`0012` through the
current `0018`) dropped it — sidebar ordering for those relies on the
numeric filename prefix alone. Match whichever convention the immediately
preceding ADR uses rather than copying an old one; mixing the two within
the same sidebar has not been tested. Getting the `id`/`slug`/filename
inconsistent is the most common cause of a broken sidebar entry or 404
after merge — verify by running the docs build (see below) before opening
the PR.

## Format: Michael Nygard's template

```markdown
# NNNN. Title (a short noun phrase)

## Status
Accepted | Proposed | Deprecated | Superseded by ADR-XXXX

## Context
The forces at play — technical, business, constraints — that make this
decision necessary. Write in the past tense, as if explaining to someone
who wasn't there. State the alternatives seriously considered, not just
the one chosen; a reader six months from now needs to know a simpler
option was weighed and rejected, not assume nobody thought of it.

## Decision
What was actually decided, stated as an active, present-tense
declaration ("we will..."). Be specific about the mechanism, not just the
intent — this section should let a reader implement the same decision
from scratch without asking follow-up questions.

## Consequences
What becomes easier, what becomes harder, and what future work this
creates or forecloses. Be honest about the downsides — an ADR that only
lists benefits reads as marketing, not a decision record.
```

The `## Decision` section is the part worth the most editing effort: see
ADR-0018 (`docs/docs/adr/0018-path-capacity-changed.md`,
"Publish per-path, per-CPT remaining admission capacity as
`PathCapacityChanged`") for a model example in this repo — it states the
exact event shape, names the specific `ReleaseFed`/`FlowFed` `Known` rule
with worked cases, explains WHY correlation is by `CutoffAt` timestamp
rather than a sibling service's `cptId`, and closes with an explicit
"Alternatives considered and rejected" subsection naming four rejected
designs and why each lost. That level of specificity — not just "we
publish an event" but the exact field-by-field semantics and the
rejected alternatives — is the bar for this repo's ADRs.

## Superseding an earlier ADR

Don't edit the old ADR's Decision section. Add a `## Status` line noting
`Superseded by ADR-XXXX` on the OLD one, and open the new ADR referencing
it. This repo's real example: **ADR-0016 supersedes ADR-0015.** ADR-0015
("REST identity via static-bearer scopes") adopted a fleet-wide auth
decision; the fleet later reversed it, and ADR-0016
(`docs/docs/adr/0016-remove-rest-mcp-static-bearer-auth.md`) records the
reversal with the exact wording pattern to copy:

```markdown
## Status

Accepted — implemented in the same change that introduced this record.
Supersedes [ADR-0015](./0015-rest-identity-static-bearer-scopes.md).
```

Note ADR-0015 itself is left in place, unedited, as the historical record
of why the layer existed — ADR-0016's own Context section says exactly
this: it's "the pointer from 'why is there no auth here' back to 'there
used to be, see ADR-0015 and ADR-0016.'" Never delete or rewrite a
superseded ADR.

## Cross-repo decisions: use a companion ADR, not one repo's private opinion

When a decision genuinely spans two bounded-context repos, write ONE ADR
per repo, each referencing the other explicitly as "the companion ADR"
with a one-line description of the split of responsibility. This repo's
own ADR-0018 is a real example of the pattern even without a literal
"companion ADR" pair: its Context section opens by citing
order-management's ADR-0014 ("The delivery promise is a CPT window
derived from fulfillment capability", order-management PR #52) by name,
quotes the exact port signature that ADR defined
(`ports.PathCapacity.Remaining(pathId, cptId) (units int, known bool)`),
and explicitly states that decision "names **this repo** as the future
real source of that data ... and says the record for that message belongs
**here**." Don't write the decision once in one repo and expect the other
repo's readers to find it; each bounded context's docs site is read
independently — cite the other repo's ADR by number and PR, don't
paraphrase it from memory.

## After writing: regenerate and verify the docs build

```bash
cd docs
npm ci
npm run build   # onBrokenLinks / onBrokenAnchors — this WILL fail if the
                 # frontmatter/slug is wrong or a cross-reference link
                 # (e.g. a `./NNNN-old-slug.md` link) is broken
```

A broken ADR link or malformed frontmatter fails the build with a clear
Docusaurus error, not a silent 404 — always run this locally before
opening the PR.
