# Changelog

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
