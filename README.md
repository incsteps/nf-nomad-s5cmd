# nf-s5cmd

High-throughput S3 file staging for Nextflow via the [`s5cmd`](https://github.com/peak/s5cmd)
command-line client. Designed as a focused alternative to Nextflow's built-in
S3 transfer path when you need:

- Massively parallel object copies (s5cmd is ~10–30× faster than `aws s3 cp`
  on directories of small-to-medium files).
- A single fast binary, easy to ship as a Nomad/Kubernetes artifact, no AWS
  SDK pulled into the head container.
- Deterministic CLI semantics — every transfer is a reproducible
  `s5cmd cp` invocation visible in `.command.run`.

> Use **nf-nomad-s5cmd** when your pipeline lives entirely on S3-compatible
> storage and you need raw throughput from `s5cmd cp`'s worker pool.

## Status

**Experimental / development** — built only for the `nf-nomad` plugin. Tracks Nextflow 25.10.x.

## Quick start

`nextflow.config`:

```groovy
plugins {
    id 'nf-s5cmd@0.1.0'
}

s5cmd {
    // S3 endpoint + credentials (MinIO / rustfs / AWS)
    endpoint        = 'http://rustfs.aither:9900'
    accessKeyId     = '${env.AWS_ACCESS_KEY_ID}'
    secretAccessKey = '${env.AWS_SECRET_ACCESS_KEY}'
    region          = 'us-east-1'
    forcePathStyle  = true            // required for MinIO/rustfs

    // Which path prefixes get s5cmd staging (others fall through to
    // Nextflow's default ln/cp strategy)
    pathMapping = ['s3://my-bucket': true]

    // s5cmd-specific tunables
    transfer {
        concurrency = 5     // -c   parallel parts per file
        numWorkers  = 256   // -numworkers
        retryCount  = 10    // -r
        logLevel    = 'INFO'
    }
}
```

Then run as usual:

```bash
nextflow run main.nf
```

Any input/output path that starts with a configured `pathMapping` prefix is
staged via `s5cmd cp` instead of `cp`/`ln -s`.

## Requirements

- `s5cmd` binary on PATH (head node + every compute node) — installable from
  https://github.com/peak/s5cmd/releases or via your distribution's package
  manager.
- An S3-compatible endpoint (AWS, MinIO, rustfs, Ceph RGW, ...).
- Nextflow ≥ 25.10.

## Configuration reference

See [`USAGE.md`](USAGE.md) for the full reference once the plugin matures
beyond the initial scaffold.

## Composition with nf-nomad

When deployed alongside `nf-nomad` on an HPC/Nomad cluster, Nextflow's
`BashWrapperBuilder` picks up the `S5cmdFileCopyStrategy` for matching paths,
and the resulting `s5cmd cp` calls in `.command.run` are executed inside
whichever Nomad container the worker runs in. No nf-nomad changes are required.

For shipping `s5cmd` to every Nomad client, register the binary in `tools.toml`
and use a sysbatch job to drop it at a stable host-volume path (e.g.
`/opt/nomad/scratch/bin/s5cmd`) that workers can mount.

## License

Eclipse Public License 2.0 (EPL-2.0) — see [`COPYING`](COPYING).

The plugin is intentionally licensed under EPL-2.0 (rather than Apache-2.0) to
encourage modifications to be contributed back upstream. EPL's "file-level
copyleft" applies only to derivative works of EPL-licensed source files; it
does not extend to user pipelines or other plugins that merely use the
plugin at runtime.
