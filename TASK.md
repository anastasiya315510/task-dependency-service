
# TDS-142 · Dependency visibility for tasks

| | |
|---|---|
| **Type** | Story (Epic: *Project Planning — Dependencies*) |
| **Priority** | High |
| **Components** | Task Service |
| **Labels** | `dependencies` `planning` `customer-requested` |
| **Reporter** | Product |
| **Status** | Ready for Dev |

---

## Background

Our customers run real projects in the tool. A task almost never stands alone —
it can't start until the thing it builds on is done. Today a task can already
point at the one task it directly depends on, and tasks stream into our system
continuously from the customers' own tools and integrations (so they don't arrive
in a tidy, predictable order).

What's missing is the part people actually plan around: when someone opens a task,
they can't see the **full chain of work that has to happen before it**. They see
"this depends on X," but not what X depends on, and what *that* depends on, all the
way back to the work that has no prerequisites of its own. Support and a couple of
design-partner accounts have asked for this directly — it's the difference between
"I know my next blocker" and "I understand everything standing between me and
done."

## Problem statement

> As a user looking at a task, I want to see the complete dependency chain leading
> up to it — every task it relies on, in order, up to the original root task — so I
> can understand what must be finished first and in what sequence.

## Scope of this story

Build the dependency-chain capability on top of the existing service:

1. **Ingest tasks from the incoming stream.** Tasks created or updated upstream
   must reliably become part of our records and be available to read back. Today
   nothing picks these up — that's the first gap to close. Include a simple way to
   put test tasks onto the stream so the flow can be demonstrated end to end.

2. **Expose a task's dependency chain.** Given any task, a user can retrieve the
   ordered list of tasks it depends on — its direct dependency, then *that* task's
   dependency, and so on up to the root. The existing "get a single task" endpoint
   stays as-is; this is the new capability built beside it.

## Acceptance criteria

- [x] A task that enters through the incoming stream can afterwards be looked up in
      the service.
- [x] For any task, a user can retrieve its **full ordered dependency chain**, from
      its immediate dependency up to the root task.
- [x] A task with no dependencies (a root) returns an empty chain, not an error.
- [x] Asking for the chain of a task that doesn't exist gives a clear "not found"
      response.
- [x] **The chain is correct no matter what order the task events arrived in** — a
      task may show up before the task it depends on, and the result must still be
      right once both are present.
- [x] **The same task event arriving more than once never duplicates or corrupts a
      task.** Upstream systems retry and replay; that's expected and must be safe.
- [x] The dependency view stays **responsive as projects grow** — many tasks, and
      long dependency chains, should not make it fall over or crawl.
- [x] The feature **degrades gracefully** if a supporting part of the system is
      temporarily unavailable, rather than failing the user outright.

## Out of scope (for this story)

- Editing or deleting tasks through this service (tasks are owned upstream).
- Showing a task's *dependents* (the tasks that rely on it) — this story is the
  upward chain only.
- Any UI work — this is the service capability only.
- Multi-dependency tasks: for now a task depends on at most one other task.

## Notes / known shape of the data

- A task has an id, a title, and at most one task it directly depends on.
- A task with no dependency is a root.
- The structure is a tree (each task has a single direct dependency), so a task
  cannot end up depending on itself through a loop under normal operation — but if
  bad data ever introduced one, the service should not hang or crash.

## Definition of done

The capability works end to end against the running stack, the behaviour above is
demonstrable (including out-of-order arrival and a replayed event), and there's
enough automated checking that we'd trust a change to it without re-testing by
hand.
