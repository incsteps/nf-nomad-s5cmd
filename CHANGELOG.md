# Changelog

## 0.1.7

### Fixed — large same-bucket S3 inputs failed staging with `IncompleteBody`
Staging a large `path` input that lives in the **same MinIO bucket** as the work
dir used a server-side S3→S3 copy (`s5cmd cp s3://… s3://…`), which the cluster's
MinIO rejects for large objects (`IncompleteBody`, HTTP 400) — e.g. a 2.3 GB
CheckV reference DB. **Fix:** same-bucket S3 inputs now route through a local-disk
round-trip (download with the configured endpoint → re-upload to `inputs/`), the
same pattern external-bucket inputs already use. This converts the flaky
server-side COPY into a reliable GET + PUT and sidesteps the failure entirely.
External-bucket inputs are unchanged; purely local inputs still use a single
direct upload. **Known cost:** large reference DBs are downloaded to the head's
temp dir before re-upload (a host-volume for reference DBs is the future
efficiency fix).

### Fixed — `Invalid prefix or suffix` at task setup for nested stage names
The local round-trip derived the temp-file suffix directly from the stage name.
When a stage name carries a subdirectory (e.g. FASTQC stages its reads under a
nested name), the `/` leaked into the `Files.createTempFile` suffix and Java threw
`Invalid prefix or suffix`. **Fix:** the temp-file suffix is reduced to a
path-separator-free leaf (`tempSuffixFor`); the temp file is a transient local
buffer, so its name has no bearing on the upload destination. Applied to both the
same-bucket and external-input paths.

## 0.1.6

### Fixed — silent partial-failure of s5cmd staging (rc=0 masking)
s5cmd exits `rc=0` even when individual per-file copies fail (errors go only to
stderr), so two real failures went undetected:
- **Stage-in:** a failed per-task input copy (e.g. `IncompleteBody` on a large
  reference-DB object) left the input missing, the task ran anyway, and a
  downstream step 404'd. **Fix:** each stage-in `s5cmd cp` now captures stderr,
  fails the task (non-zero exit) on `ERROR`/`IncompleteBody`, and verifies the
  local target exists — aborting the chained stage-in on the first bad input.
- **Push-back:** the EXIT-trap recursive `s5cmd cp ./ <remote>` could drop files
  (e.g. `versions.yml`) while returning `rc=0`; the success `.exitcode` was still
  written, so Nextflow proceeded and 404'd on the missing output. **Fix:** capture
  the push output, detect `rc!=0`/`ERROR`/`IncompleteBody`, and override
  `.exitcode` to non-zero so the task is marked failed and retried instead of
  proceeding with incomplete outputs.

Detection-only — the copy mechanism, endpoint, and retry behaviour are unchanged.
Test-first: `S5cmdRcSafetySpec` (characterization + previously-`@PendingFeature`
specs, now green). See `ISSUE-nf-nomad-s5cmd-large-object-staging`.

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

## 0.1.2

_Documented retroactively: both behaviours shipped in 0.1.2 (2026-06-04, commits
`86c4eb0` and `a3574a2`) but were never written up here or in the configuration
reference. Recorded now so operators can find them — and switch them off._

### Added — node-local input cleanup after each task (`workDir.cleanupLocal`, default `true`)
In distributed-workdir mode a task's inputs exist twice: on S3 under the per-task
`inputs/` prefix, and on node-local scratch once the worker stages them in. The
local copies are now deleted as soon as the task's command completes, before
outputs are pushed back. This reclaims worker disk immediately instead of at end
of run, and keeps inputs out of the worker→S3 push-back, which would otherwise
re-upload bytes the head had already placed under `inputs/`.

Declared **outputs** and anything `-resume` depends on are never touched. An input
that is also a declared output — an in-place modification staged and published
under the same name — is detected and kept, so the cleanup cannot delete a result.

### Added — end-of-run sweep of S3 staged inputs (`workDir.cleanupRemoteInputs`, default `true`)
On session completion the per-task `inputs/` prefixes are removed from the S3 work
dir (`s3://<bucket>/<prefix>/*/inputs/*`). Staged inputs are needed only during the
run — worker stage-in and retries — and `-resume` reuses task **outputs**, never
inputs, so the sweep does not compromise a resumed run.

Best-effort by design: a non-zero return (for example when there is nothing to
remove) is logged as a warning and the run continues. It never fails a workflow.

Set either key to `false` to keep the copies, which is what you want when debugging
a staging problem — the inputs a task actually received are the evidence.

