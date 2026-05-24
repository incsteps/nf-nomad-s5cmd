# nf-nomad-s5cmd

High-throughput S3 file staging **and** distributed-workdir backend for Nextflow
on Nomad. Built against the [`s5cmd`](https://github.com/peak/s5cmd)
command-line client and the `DistributedWorkdirProvider` SPI introduced in
[`nf-nomad`](https://github.com/nextflow-io/nf-nomad)'s `feature/remoteworkdir`
branch.

Use **nf-nomad-s5cmd** when:

- Your Nomad cluster has **no shared filesystem** between head node and
  workers — you need every task's `.command.*` files and outputs to live on
  S3-compatible storage and be pulled / pushed by the worker itself.
- You want **massively parallel** S3 transfers — s5cmd is 10–30× faster than
  `aws s3 cp` on directories of small-to-medium files.
- You want **deterministic, reproducible** CLI semantics — every transfer is
  a visible `s5cmd cp` invocation in `.command.run`.

> Use the shared-FS path (nf-nomad's `volume` directive) when your cluster
> already has NFS / FusionFS / a CSI volume mounted across workers. Use this
> plugin when it doesn't.

## Status

**Experimental.** Tracks `nf-nomad`'s `feature/remoteworkdir` branch
(distributed-workdir SPI) and Nextflow 25.10.x. Requires both plugins to be
loaded for the distributed-workdir mode.

## How it integrates with nf-nomad

`nf-nomad` exposes a `DistributedWorkdirProvider` SPI under
`nextflow.nomad.executor.spi.*`. `nf-nomad-s5cmd` registers
`S5cmdNomadInteropFactory` as a PF4J `@Extension`; `nf-nomad` discovers it at
runtime via `Plugins.getExtensions(DistributedWorkdirProviderFactory.class)`.
Activation is driven entirely by Nextflow config — no nf-nomad-side wiring
needed.

When `nomad.s5cmd.workDir.enabled = true` and a bucket is configured, the
SPI takes over per-task lifecycle:

1. **Pre-submit (head node):** the operator side uploads `.command.*` to
   `s3://<bucket>/<prefix>/<task-relative-path>/` via `s5cmd cp`.
2. **On the worker:** the Nomad task's submit command is a single bash
   bootstrap script that pulls `.command.*` down from S3, runs the task,
   writes `.exitcode`, and pushes the whole task dir back to S3.
3. **Post-completion (head node):** nf-nomad polls the remote `.exitcode`
   marker and reconciles task state. `nf-nomad`'s reconciliation logic
   trusts a `0` exit code from either signal (remote `.exitcode` or local
   `.command.exit`) over a Nomad alloc-state failure.

When `nomad.s5cmd.workDir.enabled = false` (the default) and only the
file-staging mode is enabled, the plugin still installs an
`S5cmdFileCopyStrategy` for paths matching `nomad.s5cmd.paths`. This is
useful on shared-FS clusters that want s5cmd's throughput for input/output
staging only.

## Quick start

### 1. Build both plugins locally as `99.99.99`

For now both plugins must be built from source. From the `nf-nomad` checkout
on `feature/remoteworkdir`:

```bash
./gradlew clean installPlugin -Pversion=99.99.99
```

From the `nf-nomad-s5cmd` checkout:

```bash
./gradlew clean installPlugin -Pversion=99.99.99
```

Both will be installed under `${NXF_PLUGINS_DIR:-~/.nextflow/plugins}/`.

### 2. Worker prerequisites

Every Nomad **client** node needs:

- The `s5cmd` binary on a path the task container can reach. Recommended
  delivery is a **host volume** — drop the binary at
  `/opt/nomad/scratch/bin/s5cmd` on every client and mount it into tasks:

  ```hcl
  # client.hcl
  host_volume "s5cmd-bin" {
    path      = "/opt/nomad/scratch/bin"
    read_only = true
  }
  ```

- Network reachability to your S3 endpoint (MinIO, rustfs, AWS, etc).
- Credentials available to the task — easiest path is to inject
  `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` via Nomad secrets / env
  passthrough. nf-nomad-s5cmd does not manage credentials itself.

### 3. `nextflow.config`

Canonical config shape — all keys live under `nomad.s5cmd.*`:

```groovy
plugins {
    id 'nf-nomad@99.99.99'
    id 'nf-s5cmd@99.99.99'
}

nomad {
    // standard nf-nomad executor config
    client {
        address = 'http://localhost:4646'
    }
    jobs {
        // mount the host-volume that carries the s5cmd binary
        volumes = [[type: 'host', name: 's5cmd-bin', path: '/opt/s5cmd', readOnly: true]]
    }

    s5cmd {
        enabled = true

        // S3 endpoint + creds (env-driven for safety)
        s3 {
            endpoint        = "${System.getenv('NF_S5CMD_ENDPOINT') ?: 'http://localhost:9000'}"
            region          = 'us-east-1'
            accessKeyId     = "${System.getenv('AWS_ACCESS_KEY_ID')}"
            secretAccessKey = "${System.getenv('AWS_SECRET_ACCESS_KEY')}"
            usePathStyle    = true   // required for MinIO / rustfs
        }

        // Which s3 path prefixes are routed through s5cmd for I/O staging
        // (file-staging mode). Paths outside this list fall through to
        // Nextflow's default cp / ln strategy.
        paths = [ 's3://my-bucket/' ]

        // s5cmd transfer tunables (applied to every `s5cmd cp` invocation)
        cp {
            concurrency = 5      // -c   parallel parts per file
            numWorkers  = 64     // -numworkers
            retryCount  = 3      // -r
            partSize    = 50     // MB
            logLevel    = 'info'
        }

        // ↓↓↓ Distributed-workdir mode — opt in for non-shared-FS clusters
        workDir {
            enabled            = true
            bucket             = 's3://nextflow-work'
            prefix             = 'sessions/'         // optional sub-path
            completionTimeout  = '60s'               // remote .exitcode poll deadline
        }
    }
}
```

The minimum triple to activate the SPI: `nomad.s5cmd.enabled = true` +
`nomad.s5cmd.workDir.enabled = true` + `nomad.s5cmd.workDir.bucket` set.

### Legacy top-level scope

A bare top-level `s5cmd { … }` block is still accepted with a one-shot
deprecation warning routed through `S5cmdConfigLocator`. Migrate to
`nomad.s5cmd { … }` — the legacy path will be removed once existing
pipelines migrate.

## Testing with `nextflow-io/rnaseq-nf` and `nf-core/demo`

Two pinned test pipelines for end-to-end verification of the
distributed-workdir mode.

### Prereqs (both pipelines)

1. Local Nomad (server + ≥1 client) running and reachable at `NOMAD_ADDR`.
2. Local MinIO running and reachable at `NF_S5CMD_ENDPOINT` (default
   `http://localhost:9000`). Create the bucket the workDir will write to:

   ```bash
   mc alias set local "$NF_S5CMD_ENDPOINT" "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY"
   mc mb local/nextflow-work
   ```

3. Both plugins built as `99.99.99` per § "Quick start".
4. `s5cmd` host-volume in place on every Nomad client.
5. Save the canonical `nextflow.config` above somewhere as
   `~/nf-s5cmd-test.config`.

### Run `nextflow-io/rnaseq-nf` (v2.3, commit `4b41025`)

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export NF_S5CMD_ENDPOINT=http://localhost:9000

nextflow run nextflow-io/rnaseq-nf \
  -r v2.3 \
  -profile docker \
  -c ~/nf-s5cmd-test.config \
  -with-trace trace-rnaseq.txt \
  -with-report report-rnaseq.html
```

### Run `nf-core/demo` (1.1.0, commit `45904cb`)

```bash
nextflow run nf-core/demo \
  -r 1.1.0 \
  -profile test,docker \
  -c ~/nf-s5cmd-test.config \
  --outdir s3://nextflow-work/demo-out \
  -with-trace trace-demo.txt
```

### Verification checklist

After either run:

- `s5cmd ls s3://nextflow-work/sessions/` shows one prefix per task.
- Inside each task prefix: `.command.sh`, `.command.run`, `.command.out`,
  `.command.err`, `.exitcode`, and the produced outputs.
- Every task in `trace-*.txt` has `status: COMPLETED` and a non-empty
  `realtime`.
- Nomad UI: each job's logs show the bootstrap script pulling `.command.*`
  from S3 via `s5cmd cp`, running the task, then pushing the work-dir back.
- The Nextflow `.nextflow.log` shows
  `Selected distributed-workdir provider: s5cmd` near session start.

### What "good" looks like

| Signal | Meaning |
|---|---|
| `Selected distributed-workdir provider: s5cmd` in `.nextflow.log` | SPI extension was discovered by nf-nomad |
| `nomad.s5cmd.workDir.enabled=true` echoed in nf-nomad's startup config dump | The activation triple is correctly read |
| `.exitcode` present in every task's S3 prefix after completion | Worker bootstrap script ran to completion |
| Trace `realtime` ≈ task duration (not Nomad-alloc duration) | Reconciliation trusted the remote `.exitcode` |
| No `WARN .* alloc-state failure` lines when tasks succeeded | Reconciliation correctly suppressed false alloc-state failures |

## Configuration reference (summary)

| Key | Default | Meaning |
|---|---|---|
| `nomad.s5cmd.enabled` | `false` | Master switch for the plugin |
| `nomad.s5cmd.paths` | `[]` | s3:// URL prefixes routed through `S5cmdFileCopyStrategy` |
| `nomad.s5cmd.s3.endpoint` | — | S3 API endpoint URL |
| `nomad.s5cmd.s3.region` | `us-east-1` | S3 region |
| `nomad.s5cmd.s3.accessKeyId` | `$AWS_ACCESS_KEY_ID` | S3 access key |
| `nomad.s5cmd.s3.secretAccessKey` | `$AWS_SECRET_ACCESS_KEY` | S3 secret |
| `nomad.s5cmd.s3.usePathStyle` | `false` | Set `true` for MinIO / rustfs |
| `nomad.s5cmd.cp.concurrency` | `5` | s5cmd `-c` parallel parts per file |
| `nomad.s5cmd.cp.numWorkers` | `64` | s5cmd `-numworkers` |
| `nomad.s5cmd.cp.retryCount` | `3` | s5cmd `-r` |
| `nomad.s5cmd.cp.partSize` | `50` | MB |
| `nomad.s5cmd.cp.logLevel` | `info` | `debug` / `info` / `warn` (s5cmd expects lowercase) |
| `nomad.s5cmd.workDir.enabled` | `false` | Activate SPI distributed-workdir mode |
| `nomad.s5cmd.workDir.bucket` | — | `s3://<bucket>` root for session work-dirs |
| `nomad.s5cmd.workDir.prefix` | — | Optional path prefix under the bucket |
| `nomad.s5cmd.workDir.completionTimeout` | `60s` | Max wait for remote `.exitcode` |

## Requirements

- `nf-nomad` on `feature/remoteworkdir` (or any post-`2f444df` cut once
  released) for the `DistributedWorkdirProvider` SPI.
- `s5cmd` binary on every Nomad client — host-volume install at
  `/opt/nomad/scratch/bin/s5cmd` is recommended.
- An S3-compatible endpoint (AWS, MinIO, rustfs, Ceph RGW, …).
- Nextflow ≥ 25.10.

## Operator deployment requirements (host-volume mode)

The following three conditions must all be true before submitting a Nextflow
job with `workDir.enabled = true` on a Nomad cluster that delivers s5cmd via
a host volume.

### 1 — s5cmd binary on the host volume

`s5cmd` must be pre-installed inside the host volume's source directory on
every Nomad client, under a `bin/` sub-directory that the bootstrap script
adds to `PATH`.  Example for a host volume whose source path is
`/opt/abc-seedling/nf-work`:

```
/opt/abc-seedling/nf-work/bin/s5cmd    ← must be executable
```

The bootstrap script prepends `<volume-mount>/bin` to `PATH` before any
s5cmd call.  If the binary is missing the bootstrap exits immediately and
the Nomad alloc fails at the version-check step (first line in the script).

The recommended production delivery mechanism is a Nomad `sysbatch` job that
downloads the correct s5cmd release tarball and installs it on every client.
Manual `sudo curl` installs work for testing.

### 2 — Nomad ACL policy: `host_volume` capability

The Nomad token used to submit jobs **must** carry a `host_volume` policy
capability in addition to the namespace `write` policy.  A bare namespace
write is insufficient when the job spec includes a `host_volume` stanza.

```hcl
# excerpt from the token's ACL policy
namespace "<your-namespace>" {
    policy = "write"
}
host_volume "<your-volume-name>" {
    policy = "write"
}
```

When the capability is missing, Nomad returns HTTP 403 with an empty response
body.  The Nomad Java client surfaces this as an `ApiException` with a blank
message (not "Permission denied") — PATCH the ACL policy via
`/v1/acl/policy/<name>` to add the capability.

### 3 — `jobs.volumes` uses `path:`, not `mountPath:`

`nf-nomad`'s `JobVolume` parser only recognises the keys
`['type', 'name', 'path', 'workDir', 'readOnly']`.  The key `mountPath` is
silently dropped, which causes the mount destination to fall back to an
internal computation that produces the wrong path (e.g. `/s3-bucket/work`
instead of `/nxf-work`).

Use `path:` in every volume entry:

```groovy
// ✓ correct
jobs.volumes = [
    [type: 'host', name: 'nf-work', path: '/nxf-work', readOnly: false]
]

// ✗ wrong — mountPath is silently ignored
jobs.volumes = [
    [type: 'host', name: 'nf-work', mountPath: '/nxf-work', readOnly: false]
]
```

When exactly one volume is present with no explicit `workDir` key, nf-nomad
auto-assigns `workDir = true` to it.  Combined with `path: '/nxf-work'` this
correctly mounts the volume at `/nxf-work` inside the container.

---

## License

Eclipse Public License 2.0 (EPL-2.0) — see [`COPYING`](COPYING).

The plugin is intentionally licensed under EPL-2.0 (rather than Apache-2.0)
to encourage modifications to be contributed back upstream. EPL's
"file-level copyleft" applies only to derivative works of EPL-licensed
source files; it does not extend to user pipelines or other plugins that
merely use the plugin at runtime.
