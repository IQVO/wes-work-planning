# Harness incident log

Rule: **every failure that reached CI, a scheduled run, or a human without being caught earlier ends with four things**:
1. the fix,
2. a sensor or guide that would have caught it (a `repo_lint` rule, an architecture fitness test, a guide-lint check,
   a hook, a skill),
3. the sensor rolled out to the whole fleet (not just the repo that broke),
4. a row here. A row with no guard is an open gap and must say so.

Sensors are prompts: a failing check has to say WHAT broke, WHY the rule exists and the exact FIX, so an agent can
self-correct from the message. If an incident needed a human to explain the failure, the message is not good enough yet.

| Date | Symptom | Root cause | Fix | Guard | Status |
|---|---|---|---|---|---|
| 2026-10-17 | two healthy `TaskCompleted` events (each a second, DISTINCT task completing an already-completed work unit, 2026-10-14 e2e run) sat dead-lettered in `warehouse.fulfillment.events.dlq` after three pointless retries each | deterministic domain error dead-lettered as poison: `ApplyTaskCompleted` returned `workunit.ErrAlreadyCompleted` as an error, and the consumer's `dispatch()` retries then dead-letters ANY handler error; the new-event-id case dodged both existing no-op classifications (processed-mark and unknown-work-unit) | PR `fix/taskcompleted-already-completed-skip`: classify `ErrAlreadyCompleted` as `TaskCompletedAlreadyCompleted` inside the use case's atomic scope — one INFO line, event marked processed, nil returned; ADR-0043; the two DLQ rows are e2e fixtures and need no redrive | `TestApplyTaskCompleted_AlreadyCompletedWorkUnitIsALoggedSkip` (usecase, proven red on develop first) + `TestHandleFulfillmentEvent_AlreadyCompletedWorkUnitIsNotDeadLettered` (consumer seam); `TestApplyTaskCompleted_OtherErrorsPropagateAndLeaveNoMark` keeps unclassified domain errors (`ErrNotReleased`) propagating | guarded in this repo. **Open gap, fleet-wide**: no sensor checks that a consumer classifies deterministic domain no-ops per error kind — each repo's tests cover only the errors its author thought of |
