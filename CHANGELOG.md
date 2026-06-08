# Changelog

## 0.1.5

### Fixed — inter-task input-staging regression (introduced in 0.1.4)
- 0.1.4 pushed task outputs with `s5cmd cp --exclude ".exitcode" ./ <dest>` in the
  worker bootstrap. With a **non-wildcard directory source**, an s5cmd
  exclude-filter suppresses the recursive upload, so the task's outputs never
  reached S3. Every downstream task that consumed an upstream output then failed
  at stage-in with `404 NoSuchKey`, making any multi-step pipeline unrunnable.
  (A/B-verified on `dylangrblr/Vinotype`: 0.1.4 → 19/19 BWAMEM2 stage-in 404s;
  0.1.2 clean.)
- **Fix:** push outputs with a plain recursive `s5cmd cp ./ <dest>` (no
  exclude-filter — the proven pre-0.1.4 form), then write + push `.exitcode`
  strictly **LAST**, *after* the output push. The "exitcode-last" preemption
  safety added in 0.1.4 is preserved — a preemption mid-push leaves no remote
  `.exitcode`, so `synchronizeCompletion()` returns null and the task is retried
  — it just no longer depends on the broken exclude-filter. `_exit_code` is now
  initialised early so the EXIT trap is safe under `set -u`.

## 0.1.4

### Reliability
- Push the remote `.exitcode` strictly LAST in the worker bootstrap (separate
  `s5cmd cp` after the outputs push, which now excludes `.exitcode`). The
  operator-side handler polls for the remote `.exitcode` and only then pulls
  outputs, so "exitcode present" now implies "outputs already staged". This
  fixes a class of spot/preemption failures where a node was reclaimed mid
  push-back, landing `.exitcode = 0` without the output files — the handler
  trusted exit 0 and the task then failed with "Missing output file(s)". With
  the reorder, an interrupted push leaves no remote `.exitcode`, so
  `synchronizeCompletion()` returns null and the task is cleanly retried.

## 0.1.3

### Logging
- Standardised all runtime log prefixes to `nf-nomad-s5cmd:` (was a mix of
  `nf-s5cmd:` / `nf-nomad-s5cmd:`). The on-disk `nf-s5cmd-task` dir name is
  unchanged.
- Quieter by default so plugin logs don't overshadow Nextflow's own stdout on
  the head job or the task's stdout on compute jobs:
  - Demoted lifecycle messages (`plugin started`, `Found <s5cmd>`, `session
    complete`, `swept staged inputs`, bin-dir upload, external-S3 staging) from
    `info` to `debug`. The one-time `active; endpoint=…` banner stays at `info`;
    warnings/errors are unchanged.
  - Changed the default `cp.logLevel` for s5cmd staging commands from `info` to
    `error`, so s5cmd no longer prints a line per transferred file into task
    output. Raise to `info`/`debug` via `nomad.s5cmd.cp.logLevel` when debugging.
