/*
 * Copyright 2026-, Incremental Steps Software Solutions OÜ
 *
 * This program and the accompanying materials are made available under
 * the terms of the Eclipse Public License 2.0 which is available at
 *
 *     https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package nextflow.nomad.s5cmd.spi

import groovy.util.logging.Slf4j
import nextflow.exception.ProcessSubmitException
import nextflow.executor.ScriptFileCopyStrategy
import nextflow.nomad.executor.NomadLifecycleTaskSpec
import nextflow.nomad.executor.spi.DistributedWorkdirProvider
import nextflow.processor.TaskRun
import nextflow.nomad.s5cmd.config.S5cmdConfig
import nextflow.nomad.s5cmd.config.S5cmdWorkDirConfig
import nextflow.nomad.s5cmd.strategy.S5cmdCommandBuilder
import nextflow.nomad.s5cmd.strategy.S5cmdFileCopyStrategy

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

/**
 * nf-nomad-s5cmd's distributed-workdir backend for nf-nomad. Registered via the
 * {@link S5cmdNomadInteropFactory} PF4J extension — nf-nomad picks it up
 * through {@code Plugins.getExtensions(DistributedWorkdirProviderFactory.class)}.
 *
 * <p>v1 supports <b>bootstrap mode</b> only:</p>
 * <ol>
 *   <li>Pre-submit (operator side): upload {@code .command.*} to
 *       {@code s3://bucket/prefix/<task-relative-path>/} via {@code s5cmd cp}.</li>
 *   <li>On the worker, the {@code submitCommand} is a single bash
 *       script that pulls the {@code .command.*} files down, runs the task,
 *       writes {@code .exitcode}, and pushes the whole task dir back up.</li>
 *   <li>Post-completion (operator side): poll the remote {@code .exitcode}
 *       marker, then sync all artifacts back to {@code task.workDir}.</li>
 * </ol>
 *
 * <p>Sidecar mode (lifecycle-based prestart/poststop) and transfer-policy
 * enforcement are intentionally out of scope for v1; the bootstrap-
 * embedded flow above is sufficient for the standard pipeline use case.</p>
 */
@Slf4j
class S5cmdNomadInterop implements DistributedWorkdirProvider {

    // The leading $$ escapes Nomad's job-spec interpolation so the literal
    // string ${NOMAD_TASK_DIR:-$PWD} reaches bash at task runtime. Without
    // the escape, Nomad's parser sees `${NOMAD_TASK_DIR:-...}` and rejects
    // the colon-default form ("Failed Validation: Template interpolation
    // doesn't expect a colon at this location").
    private static final String TASK_DIR_VAR = '$${NOMAD_TASK_DIR:-$PWD}/nf-s5cmd-task'

    private final TaskRun task
    private final Map sessionConfig
    private final Path sessionWorkDir
    private final S5cmdConfig s5cmdConfig
    private final S5cmdWorkDirConfig workDir
    private final S5cmdCommandBuilder cmdBuilder

    final boolean enabled
    final long completionTimeoutMillis

    private List<String> submitCommand = Collections.emptyList()
    private Map<String, String> submitEnv = Collections.emptyMap()

    S5cmdNomadInterop(TaskRun task, Map sessionConfig, Path sessionWorkDir) {
        this.task = task
        this.sessionConfig = sessionConfig ?: Collections.emptyMap()
        this.sessionWorkDir = sessionWorkDir

        // Resolve scope through the canonical locator (nomad.s5cmd, with
        // a deprecation-warned fallback to legacy top-level s5cmd).
        Map<String, Object> s5cmdScope =
                nextflow.nomad.s5cmd.config.S5cmdConfigLocator.locate(
                        (Map<String, Object>) this.sessionConfig)
        Map<String, Object> baseMap = s5cmdScope ?: Collections.<String, Object>emptyMap()

        // Merge per-task overrides from `process.nomadOptions = [s5cmd: ...]`
        // (see S5cmdProcessOptions for the resolution rule).
        Map<String, Object> effective = nextflow.nomad.s5cmd.config.S5cmdProcessOptions
                .mergePerTask(baseMap, task)
        this.s5cmdConfig = S5cmdConfig.fromMap(effective)
        this.workDir = s5cmdConfig.workDir
        this.cmdBuilder = new S5cmdCommandBuilder(s5cmdConfig)
        this.enabled = s5cmdConfig.enabled && workDir.enabled
        this.completionTimeoutMillis = workDir.completionTimeout?.millis ?: 60_000L
    }

    // ── duck-typed DistributedWorkdirProvider surface ─────────────────────

    String name() { 's5cmd' }

    boolean isEnabled() { enabled }

    /** Bootstrap mode only — staging happens inline in the bootstrap script. */
    boolean isExternallyStaged() { false }

    void prepare() {
        if( !enabled ) return
        validateConfiguration()
        uploadCommandFiles()
        uploadInputFiles()
        uploadBinDirs()
        submitEnv = buildTransferEnv()
        submitCommand = ['bash', '-c', bootstrapScript()]
    }

    List<String> getSubmitCommand() { submitCommand }
    Map<String, String> getSubmitEnv() { submitEnv }

    /** No lifecycle tasks in bootstrap mode. */
    @Override
    List<NomadLifecycleTaskSpec> getLifecycleTasks() { Collections.emptyList() }

    /**
     * Returns a {@link S5cmdFileCopyStrategy} so {@code .command.run} stages
     * its inputs from the per-task remote dir via {@code s5cmd cp}. Outputs
     * are still pushed back wholesale by the bootstrap script after the task
     * exits — see {@link #bootstrapScript}.
     */
    @Override
    ScriptFileCopyStrategy createCopyStrategy(boolean stagingDisabled) {
        return new S5cmdFileCopyStrategy(
                s5cmdConfig,
                task?.workDir,
                task?.targetDir,
                task?.config?.getStageInMode(),
                task?.config?.getStageOutMode(),
                remoteTaskDir())
    }

    Integer synchronizeCompletion() {
        if( !enabled ) return null
        validateConfiguration()
        Integer remoteExit = awaitRemoteExitCode()
        copyAllArtifacts()
        if( remoteExit != null ) writeLocalExitCode(remoteExit)
        return remoteExit
    }

    String getRemoteExitHint() { remoteExitFile() }

    // ── internals ─────────────────────────────────────────────────────────

    protected void validateConfiguration() {
        s5cmdConfig.validate()
        if( !workDir.bucket ) {
            throw new ProcessSubmitException(
                '[NOMAD] nf-nomad-s5cmd workDir.enabled=true but `s5cmd.workDir.bucket` is missing')
        }
    }

    protected String remoteTaskDir() {
        return workDir.rootUrl() + taskRemotePath() + '/'
    }

    protected String remoteExitFile() { remoteTaskDir() + '.exitcode' }

    protected String taskRemotePath() {
        String fromSession = relativePathFromSessionWorkDir(task.workDir, sessionWorkDir)
        if( !fromSession ) {
            throw new ProcessSubmitException(
                "[NOMAD] nf-nomad-s5cmd interop requires task workDir `${task.workDir}` to be nested under session workDir `${sessionWorkDir}`")
        }
        if( !isNextflowWorkPathLayout(fromSession) ) {
            throw new ProcessSubmitException(
                "[NOMAD] nf-nomad-s5cmd interop requires Nextflow workDir layout `NN/HASH`; found `${fromSession}`")
        }
        return fromSession
    }

    protected void uploadCommandFiles() {
        // Use s5cmd cp with a glob to upload .command.* into the remote task dir.
        // Prefix with envExports so the operator-side s5cmd inherits the same
        // AWS_*/S3_* identity the worker bootstrap script will use — without
        // depending on the operator's shell environment having them set.
        // toS3Uri() normalises the NIO-cloud-path URI to the canonical s3://bucket/key
        // form that s5cmd expects. Path.toString() drops the scheme entirely; toUri()
        // gives s3:///bucket/key (empty authority, bucket embedded in path) — both wrong.
        // The trailing '/' is the directory separator: .command.* files live *inside*
        // task.workDir, not adjacent to it (s3://.../HASH/.command.sh not s3://.../HASH.command.sh).
        String src = toS3Uri(task.workDir) + '/.command.*'
        String dst = remoteTaskDir()
        String cmd = cmdBuilder.buildCopy(src, dst, true)
        runShell(prefixWithEnvExports(cmd))
    }

    /**
     * Upload pipeline-level {@code bin/} directories to the per-task remote
     * dir under {@code .nxf-bin/<idx>/}. The worker bootstrap script then
     * pulls them down and prepends them to PATH before running
     * {@code .command.run} — necessary for pipelines whose processes invoke
     * helper scripts shipped in the pipeline's own {@code bin/} (e.g.
     * {@code nextflow-io/rnaseq-nf} and most nf-core pipelines).
     *
     * <p>Without this, Nextflow's auto-emitted
     * {@code export PATH="$PATH:/nxf-work/.nxf-home/assets/<owner>/<repo>/bin"}
     * inside {@code .command.run} would point at a directory that exists
     * only on the head's filesystem; workers see a non-existent path and
     * the helper scripts are reported as "command not found".</p>
     *
     * <p>Uploaded once per task for simplicity (bin/ is typically a few KB
     * of shell scripts; idempotent re-uploads are cheap). A session-level
     * cache could batch this in future.</p>
     */
    protected void uploadBinDirs() {
        List<Path> binDirs = resolveBinDirs()
        if( !binDirs ) {
            log.debug "[NOMAD] nf-nomad-s5cmd: no pipeline bin/ dirs for task `${task?.name}`"
            return
        }
        String sessionId = resolveSessionId()
        // Per-session dedupe: bin/ contents are identical across all tasks in a
        // session, so upload once per (session, binDirAbsPath) pair. Subsequent
        // tasks reference the same session-level remote location.
        Set<String> uploaded = uploadedBinDirsBySession
                .computeIfAbsent(sessionId, { _k -> ConcurrentHashMap.newKeySet() } as java.util.function.Function)
        for( int i = 0; i < binDirs.size(); i++ ) {
            Path binDir = binDirs.get(i)
            if( binDir == null || !Files.isDirectory(binDir) ) continue
            String src = binDir.toString().endsWith('/') ? binDir.toString() : (binDir.toString() + '/')
            String dst = sessionBinDirUrl(sessionId, i)
            // Track by absolute path. If two sessions submit the same bin/ they
            // each upload once (different sessionIds → different keys).
            String key = src
            if( !uploaded.add(key) ) {
                log.debug "[NOMAD] nf-nomad-s5cmd: bin dir `${binDir}` already uploaded for session ${sessionId}; skipping"
                continue
            }
            log.debug "[NOMAD] nf-nomad-s5cmd: uploading pipeline bin dir `${binDir}` to ${dst} (session=${sessionId})"
            String cmd = cmdBuilder.buildCopyDir(src, dst, true)
            try {
                runShell(prefixWithEnvExports(cmd))
            }
            catch (Exception e) {
                log.warn "[NOMAD] nf-nomad-s5cmd: failed to upload pipeline bin dir `${binDir}` (${e.message}); pipeline scripts may not be available on workers"
                uploaded.remove(key)   // allow retry on next task
            }
        }
    }

    /**
     * Per-session dedupe map: {@code sessionId → set of already-uploaded bin
     * dir absolute paths}. Static so it persists across the per-task
     * S5cmdNomadInterop instances within a single head-process lifetime.
     * Session IDs scope it; concurrent sessions never collide.
     */
    private static final Map<String, Set<String>> uploadedBinDirsBySession =
            new ConcurrentHashMap<String, Set<String>>()

    /** Session-level remote URL where bin dir #idx lives for sessionId. */
    protected String sessionBinDirUrl(String sessionId, int idx) {
        return sessionBinRoot(sessionId) + "${idx}/"
    }

    /** Session-level remote root for all bin dirs of this session, trailing-slashed. */
    protected String sessionBinRoot(String sessionId) {
        return workDir.rootUrl() + "_nxf-bin/${sessionId}/"
    }

    /**
     * Resolve the Nextflow session unique ID. Used as the dedupe + URL key.
     * Falls back to a stable per-JVM UUID if Session isn't reachable (tests,
     * non-standard execution contexts).
     */
    protected String resolveSessionId() {
        try {
            def sess = task?.processor?.session
            if( sess != null ) {
                Object id = sess.uniqueId
                if( id != null ) return id.toString()
            }
        } catch (Throwable ignored) { /* fall through */ }
        return jvmFallbackSessionId
    }

    private static final String jvmFallbackSessionId = UUID.randomUUID().toString()

    /**
     * Resolve the pipeline's bin directories from the task. The session
     * (reached via {@code task.processor.session}) is the authoritative
     * source — TaskRun itself doesn't carry the bin dir in vanilla Nextflow.
     */
    protected List<Path> resolveBinDirs() {
        List<Path> out = new ArrayList<>()
        if( task == null ) return out
        Object sess = null
        try {
            sess = task.processor?.session
        } catch (Throwable t) {
            log.debug "[NOMAD] nf-nomad-s5cmd: cannot reach task.processor.session — ${t.message}"
            return out
        }
        if( sess == null ) return out
        // Modern API: getBinDirs() → List<Path>
        try {
            Object multi = sess.getBinDirs()
            if( multi instanceof List ) {
                for( Object p : (List) multi ) if( p instanceof Path ) out.add((Path) p)
            }
        } catch (Throwable t) {
            log.debug "[NOMAD] nf-nomad-s5cmd: session.getBinDirs() failed — ${t.message}"
        }
        // Older API: getBinDir() → Path
        if( out.isEmpty() ) {
            try {
                Object single = sess.getBinDir()
                if( single instanceof Path ) out.add((Path) single)
            } catch (Throwable t) {
                log.debug "[NOMAD] nf-nomad-s5cmd: session.getBinDir() failed — ${t.message}"
            }
        }
        return out
    }

    /**
     * Upload each task input file to {@code <remoteTaskDir>/inputs/<stageName>}
     * so the worker can pull them back via {@code s5cmd cp} as emitted by
     * {@link S5cmdFileCopyStrategy#getStageInputFilesScript}. This is the
     * piece that lifts s5cmd-workdir mode beyond no-input pipelines like
     * {@code nextflow-io/hello} — without this, real pipelines fail at
     * stage-in (their {@code .command.run} references files only present
     * on the head's filesystem).
     *
     * <h3>S3 source routing</h3>
     * <p>Two S3 paths are handled differently:</p>
     * <ul>
     *   <li><b>Same-bucket inputs</b> (e.g. an S3 path already within
     *       {@code workDir.bucket}): fast in-cluster S3→S3 copy via the
     *       configured MinIO endpoint.</li>
     *   <li><b>External-bucket inputs</b> (e.g. {@code s3://ngi-igenomes/…}
     *       from public AWS iGenomes): a two-step cross-endpoint transfer —
     *       (1) download from the external endpoint via a bare s5cmd invocation
     *       (no custom {@code --endpoint-url}, so it hits AWS S3 directly;
     *       public buckets use {@code --no-sign-request}), then
     *       (2) upload the local temp copy to the cluster's MinIO inputs dir.
     *       The worker stage-in script is unchanged: it always pulls from
     *       {@code remoteTaskDir/inputs/} on MinIO.</li>
     * </ul>
     *
     * <p>This avoids the "mixing S3 backends" failure where s5cmd was sent
     * a cross-endpoint S3→S3 copy command with the MinIO endpoint URL but
     * an AWS source bucket (e.g. {@code s3://ngi-igenomes/igenomes/…}),
     * causing MinIO to reject the request because it knows nothing about
     * the source bucket.</p>
     */
    protected void uploadInputFiles() {
        Map<String, Path> inputFiles = null
        try {
            inputFiles = (Map<String, Path>) task.getInputFilesMap()
        }
        catch (Exception e) {
            log.debug "[NOMAD] nf-nomad-s5cmd: unable to retrieve task input files map (${e.message}); skipping input upload"
            return
        }
        if( !inputFiles ) return

        String inputsBase = remoteTaskDir() + 'inputs/'
        for( entry in inputFiles.entrySet() ) {
            String stageName = entry.key
            Path source = entry.value
            if( source == null ) continue

            // Detect S3 sources via URI scheme (NOT toString() — an nf-amazon
            // S3Path stringifies to `/bucket/key`, indistinguishable from a
            // local POSIX path).
            String scheme = null
            try { scheme = source.toUri()?.getScheme() } catch (Exception ignored) { /* leave null */ }
            boolean isS3 = (scheme == 's3' || scheme == 's3a')

            if( isS3 && !isWorkDirBucket(source) ) {
                // External bucket (e.g. s3://ngi-igenomes): route via
                // cross-endpoint two-step to avoid MinIO rejecting the copy.
                uploadExternalS3Input(source, stageName, inputsBase)
                continue
            }

            if( isS3 ) {
                // Same-bucket S3 input: route via a local-disk round-trip
                // (GET then PUT) instead of a direct server-side S3→S3 copy.
                // See uploadSameBucketS3InputViaLocal for the IncompleteBody
                // root cause this avoids.
                uploadSameBucketS3InputViaLocal(source, toS3Uri(source), stageName, inputsBase)
                continue
            }

            // Local (non-S3) input: existing single-step direct upload.
            String src = source.toAbsolutePath().toString()

            // Directories need s5cmd's recursive form; single files the plain form.
            // The worker stage-in strategy mirrors this — see S5cmdFileCopyStrategy.
            String cmd
            if( isDirectoryInput(source) ) {
                String dst = inputsBase + stageName + '/'
                cmd = cmdBuilder.buildCopyDir(src, dst, true)
            }
            else {
                String dst = inputsBase + stageName
                cmd = cmdBuilder.buildCopy(src, dst, true)
            }
            runShell(prefixWithEnvExports(cmd))
        }
    }

    /**
     * Stage a <b>same-bucket</b> S3 input (already on the cluster's MinIO,
     * within {@code workDir.bucket}) into the task's {@code inputs/} dir via a
     * local-disk round-trip — a GET (s3→local) followed by a PUT (local→s3) —
     * instead of a direct server-side S3→S3 copy.
     *
     * <h3>Why not a direct S3→S3 copy?</h3>
     * <p>The earlier same-bucket path issued {@code cmdBuilder.buildCopy/
     * buildCopyDir(src=s3, dst=s3)}, which s5cmd executes as a server-side
     * {@code CopyObject} on MinIO. For <b>large</b> objects (genome FASTAs,
     * index tarballs) this MinIO rejects the request with
     * {@code IncompleteBody} (HTTP 400). Plain GET (s3→local) and PUT
     * (local→s3) are reliable; only the server-side COPY fails. Routing the
     * same-bucket case through local disk — mirroring
     * {@link #uploadExternalS3Input} — sidesteps the failing COPY entirely.
     * (ISSUE-nf-nomad-s5cmd-large-object-staging.)</p>
     *
     * <p>Unlike the external path, the download uses the <b>configured cluster
     * endpoint + credentials</b> (built via {@code cmdBuilder} so it carries
     * {@code --endpoint-url}/flags, and wrapped in {@link #prefixWithEnvExports}
     * so AWS creds are exported) — this is the cluster's own MinIO, NOT a
     * public bucket, so there is no {@code --no-sign-request}.</p>
     *
     * <p><b>Known cost:</b> large reference DBs are downloaded in full to the
     * head's temp dir before re-upload. The future efficiency fix is a
     * host-volume for reference DBs (out of scope here); correctness first.</p>
     */
    protected void uploadSameBucketS3InputViaLocal(Path source, String srcUri, String stageName, String inputsBase) {
        log.debug "[NOMAD] nf-nomad-s5cmd: same-bucket S3 input '${stageName}' (${srcUri}) — local round-trip (GET then PUT) to avoid server-side S3→S3 COPY"
        boolean isDir = isDirectoryInput(source)

        if( isDir ) {
            // Directory: download the s3 prefix → local temp dir, then upload
            // the local temp dir → inputs/<stageName>/.
            Path tmpDir = Files.createTempDirectory('s5cmd-same-')
            try {
                String download = cmdBuilder.buildCopyDir(srcUri, tmpDir.toString(), false)
                runShell(prefixWithEnvExports(download))
                String upload = cmdBuilder.buildCopyDir(tmpDir.toString(), inputsBase + stageName + '/', true)
                runShell(prefixWithEnvExports(upload))
            } finally {
                tmpDir.toFile().deleteDir()
            }
        } else {
            // Single file: download s3 → temp, then upload temp → inputs/<stageName>.
            Path tmp = Files.createTempFile('s5cmd-same-', '-' + stageName)
            try {
                String download = cmdBuilder.buildCopy(srcUri, tmp.toString(), false)
                runShell(prefixWithEnvExports(download))
                String upload = cmdBuilder.buildCopy(tmp.toString(), inputsBase + stageName, true)
                runShell(prefixWithEnvExports(upload))
            } finally {
                Files.deleteIfExists(tmp)
            }
        }
    }

    /**
     * Whether the given input path is a directory. Wraps
     * {@link Files#isDirectory} so tests can deterministically simulate an S3
     * directory input without a live S3 filesystem provider.
     */
    protected boolean isDirectoryInput(Path source) {
        return Files.isDirectory(source)
    }

    /**
     * Two-step cross-endpoint transfer for inputs from external S3 buckets
     * (not the cluster's MinIO). Sequence:
     * <ol>
     *   <li>Download from the external endpoint using a bare s5cmd invocation
     *       (no {@code --endpoint-url}; defaults to AWS S3). Public buckets
     *       additionally get {@code --no-sign-request}.</li>
     *   <li>Upload the local temp file to the task's MinIO inputs dir using
     *       the configured cluster endpoint + credentials.</li>
     * </ol>
     *
     * <p>Large reference files (genome FASTAs, index tarballs) are
     * downloaded in full to the head node before re-upload. This is slower
     * than an in-cluster S3→S3 copy but correct, and is only triggered for
     * external-bucket inputs. A future optimisation could use presigned
     * URLs or a multipart hand-off, but correctness first.</p>
     */
    protected void uploadExternalS3Input(Path source, String stageName, String inputsBase) {
        String srcUri = toS3Uri(source)
        log.debug "[NOMAD] nf-nomad-s5cmd: external S3 input '${stageName}' (${srcUri}) — cross-endpoint download then upload"
        boolean isDir = false
        try { isDir = Files.isDirectory(source) } catch (Exception ignored) {}

        // Use the configured binary name (defaults to 's5cmd' from PATH, but respects
        // s5cmd { binary = '/explicit/path/...' } operator overrides and the
        // fallback probe in NomadS5cmdPlugin.validateS5cmdBinary().
        String bin = s5cmdConfig.binary ?: 's5cmd'

        if( isDir ) {
            // Directory: download to local temp dir, then upload recursively to MinIO.
            Path tmpDir = Files.createTempDirectory('s5cmd-ext-')
            try {
                String download = "${bin} --no-verify-ssl --no-sign-request cp '${srcUri}/*' '${tmpDir}/'"
                int rc = runShellQuiet(download)
                if( rc != 0 ) {
                    // Retry without --no-sign-request (private external bucket)
                    log.debug "[NOMAD] nf-nomad-s5cmd: public download failed (rc=${rc}); retrying with credentials for '${srcUri}'"
                    runShell("${bin} --no-verify-ssl cp '${srcUri}/*' '${tmpDir}/'")
                }
                String upload = cmdBuilder.buildCopyDir(tmpDir.toString(), inputsBase + stageName + '/', true)
                runShell(prefixWithEnvExports(upload))
            } finally {
                tmpDir.toFile().deleteDir()
            }
        } else {
            // Single file: download to temp, upload to MinIO.
            Path tmp = Files.createTempFile('s5cmd-ext-', '-' + stageName)
            try {
                String download = "${bin} --no-verify-ssl --no-sign-request cp '${srcUri}' '${tmp}'"
                int rc = runShellQuiet(download)
                if( rc != 0 ) {
                    log.debug "[NOMAD] nf-nomad-s5cmd: public download failed (rc=${rc}); retrying with credentials for '${srcUri}'"
                    runShell("${bin} --no-verify-ssl cp '${srcUri}' '${tmp}'")
                }
                String upload = cmdBuilder.buildCopy(tmp.toString(), inputsBase + stageName, true)
                runShell(prefixWithEnvExports(upload))
            } finally {
                Files.deleteIfExists(tmp)
            }
        }
    }

    /**
     * Returns true when the given S3 source path lives within the configured
     * {@code workDir.bucket} — i.e. on the same MinIO endpoint. External
     * buckets (e.g. {@code s3://ngi-igenomes}) return false and are routed
     * through {@link #uploadExternalS3Input}.
     */
    protected boolean isWorkDirBucket(Path source) {
        if( !workDir?.bucket ) return false
        try {
            String uri = toS3Uri(source)
            // Normalise workDir.bucket to trailing-slash form for prefix comparison.
            String base = workDir.bucket.endsWith('/') ? workDir.bucket : (workDir.bucket + '/')
            return uri.startsWith(base)
        } catch (Exception ignored) {
            return false
        }
    }

    protected Map<String, String> buildTransferEnv() {
        Map<String, String> env = new LinkedHashMap<>()
        env.put('NXF_S5CMD_REMOTE_WORKDIR', remoteTaskDir())
        env.put('NXF_S5CMD_REMOTE_EXITCODE', remoteExitFile())
        // Session-level bin/ root, e.g. s3://bucket/prefix/_nxf-bin/<sessionId>/
        // The worker bootstrap pulls .nxf-bin/* from here (instead of per-task)
        // so a pipeline with N tasks ships bin/ once, not N times.
        env.put('NXF_S5CMD_SESSION_BIN_DIR', sessionBinRoot(resolveSessionId()))
        // S3 connection envs (so the worker's s5cmd has the same creds/endpoint as the operator)
        String exports = cmdBuilder.envExports()
        if( exports ) {
            for( String line : exports.split('\n') ) {
                if( !line.startsWith('export ') ) continue
                int eq = line.indexOf('=')
                if( eq < 0 ) continue
                String key = line.substring('export '.length(), eq)
                String raw = line.substring(eq + 1)
                if( raw.startsWith("'") && raw.endsWith("'") )
                    raw = raw.substring(1, raw.length() - 1).replace("'\\''", "'")
                env.put(key, raw)
            }
        }
        return env
    }

    /**
     * Bootstrap script the worker runs as its main task. Inline-quoted with
     * Nomad-template-safe `$$` escapes wherever a literal `$` should reach
     * bash (Nomad templating expands single `$` first).
     *
     * <p>The worker's inline s5cmd invocations use {@code s5cmdCall}, which
     * is the full {@code s5cmd [global-flags]} string captured at script
     * generation time.  This is necessary because the global flags
     * (in particular {@code --no-verify-ssl} for private-CA S3 endpoints such
     * as a self-hosted MinIO / RustFS) are not expressible via env vars and
     * must appear on every s5cmd command line.</p>
     */
    protected String bootstrapScript() {
        String binary = s5cmdConfig.binary ?: 's5cmd'
        // Full invocation including global flags (--endpoint-url, --no-verify-ssl,
        // --log, -r, -numworkers). Generated once here so the bootstrap script
        // doesn't need to reconstruct them.  The endpoint is also available via
        // S3_ENDPOINT_URL env (Nomad injects it), but --no-verify-ssl has no
        // env-var equivalent, so we must embed it explicitly.
        String s5cmdCall = cmdBuilder.global()
        // Nomad-escape rules (learned the hard way):
        //   • `\${VAR}` / `\${VAR:-default}` from bash → write `$${VAR}` here
        //     (Nomad's HCL escape: `$$` → `$` ONLY when followed by `{`)
        //   • `$VAR` / `$(cmd)` / `$?` / `$$` from bash → write a SINGLE `$`
        //     here (Nomad passes these through unchanged; bash handles them)
        //
        // Earlier versions used `\$\$(date)` and `\$\$PATH` which Nomad does
        // NOT escape (no `{` after `$$`), so bash saw `$$` as PID (1 in the
        // container) followed by literal `(date)` / `PATH` — which broke
        // logging, PATH augmentation, and command substitution everywhere.
        return """\
set -uo pipefail

export PATH="/nxf-work/bin:\$\${PATH:-/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin}"
TASK_DIR="${TASK_DIR_VAR}"
mkdir -p "\$TASK_DIR" && cd "\$TASK_DIR"

# Step log: every important action appends here. Pushed back unconditionally
# by the EXIT trap below, so we always have visibility on worker behaviour.
NF_DBG=.nxf-debug.log
: > "\$NF_DBG"
log() { printf '[nf-nomad-s5cmd %s] %s\\n' "\$(date -u +%H:%M:%S)" "\$*" >> "\$NF_DBG" ; }

push_debug_then_exit() {
  rc=\$?
  log "EXIT trap fired with rc=\$rc"
  log "TASK_DIR contents:"
  ls -la . >> "\$NF_DBG" 2>&1 || true
  # Push outputs first with a plain recursive copy, then write + push .exitcode
  # strictly LAST. Because the marker is written only AFTER the output push, a
  # preemption mid-push leaves no remote .exitcode, so synchronizeCompletion()
  # returns null and the task is correctly retried.
  #
  # Do NOT add an s5cmd exclude-filter to this recursive push: with a
  # non-wildcard directory source, an exclude-filter suppresses the recursive
  # upload, so the task's outputs never reach S3 and downstream tasks 404 on
  # stage-in (the 0.1.4 inter-task staging regression). Writing the marker after
  # the recursive push needs no exclusion at all.
  #
  # rc-safety: s5cmd exits rc=0 even when individual files fail to upload
  # (per-file errors — e.g. IncompleteBody on a dropped versions.yml — go to
  # stderr only). Trusting rc here would stamp the user task's SUCCESS code
  # over an INCOMPLETE output set and Nextflow would 404 on the declared
  # output. So we capture the push output, grep it for ERROR/IncompleteBody,
  # and on ANY partial failure we override the recorded exit code to non-zero
  # so Nextflow sees a FAILED task and retries — rather than silently
  # proceeding with incomplete outputs. Detection only: the copy mechanism,
  # endpoint and retries are unchanged (ISSUE-nf-nomad-s5cmd-large-object-staging).
  _push_log="\$(mktemp)"
  ${s5cmdCall} cp ./ "\$\${NXF_S5CMD_REMOTE_WORKDIR}" > "\$_push_log" 2>&1 ; _push_rc=\$?
  cat "\$_push_log" >> "\$NF_DBG"
  if [ "\$_push_rc" -ne 0 ] || grep -Eq "ERROR|IncompleteBody" "\$_push_log" ; then
    log "push-back of outputs to S3 FAILED (rc=\$_push_rc or per-file ERROR/IncompleteBody on stderr) — marking task failed"
    # Override any success code the user task reported: outputs are incomplete,
    # so the task must be treated as failed (non-zero) and retried.
    if [ -z "\$_exit_code" ] || [ "\$_exit_code" = "0" ]; then
      _exit_code=1
    fi
  fi
  rm -f "\$_push_log"
  if [ -n "\$_exit_code" ]; then
    printf '%s' "\$_exit_code" > .exitcode
    ${s5cmdCall} cp .exitcode "\$\${NXF_S5CMD_REMOTE_WORKDIR}.exitcode" >> "\$NF_DBG" 2>&1 \
      || log "FINAL .exitcode push to S3 FAILED"
  else
    log "no task exit code captured (killed before completion) — leaving remote .exitcode absent so the task is retried"
  fi
  exit \$rc
}
# Initialised so the EXIT trap can test it safely under `set -u` even when the
# task is killed before the exit code is captured below.
_exit_code=""
trap push_debug_then_exit EXIT

log "bootstrap start; pwd=\$PWD; hostname=\$\${HOSTNAME:-?}"
# Structured identity log — picked up by Promtail + label-promotion rules to
# feed Loki, so queries like `{nf_session="<id>"}` find every task's lines.
# Empty fields print as the literal `-` so the line shape stays parseable.
echo "[nf-task] start session=\$\${NF_SESSION_ID:--} session_name=\$\${NF_SESSION_NAME:--} head_job=\$\${NF_HEAD_JOB_ID:--} head_alloc=\$\${NF_HEAD_ALLOC_ID:--} process=\$\${NF_PROCESS_NAME:--} task=\$\${NF_TASK_HASH:--}" >&2
log "PATH=\$PATH"
log "s5cmd at /nxf-work/bin/s5cmd:"
ls -la /nxf-work/bin/s5cmd >> "\$NF_DBG" 2>&1 || log "  /nxf-work/bin/s5cmd MISSING"
log "s5cmd version:"
${s5cmdCall} version >> "\$NF_DBG" 2>&1 || log "  s5cmd version FAILED"
log "NXF_S5CMD_REMOTE_WORKDIR=\$\${NXF_S5CMD_REMOTE_WORKDIR:-<unset>}"

log "step 1: pull .command.* from S3"
${s5cmdCall} cp "\$\${NXF_S5CMD_REMOTE_WORKDIR}.command.*" ./ >> "\$NF_DBG" 2>&1
rc=\$?; log "  cp .command.* rc=\$rc"

log "step 2: pull pipeline bin/ from session-level dir (\$\${NXF_S5CMD_SESSION_BIN_DIR:-<unset>})"
mkdir -p .nxf-bin
${s5cmdCall} cp "\$\${NXF_S5CMD_SESSION_BIN_DIR}*" .nxf-bin/ >> "\$NF_DBG" 2>&1
rc=\$?; log "  cp .nxf-bin/* rc=\$rc (non-zero is OK if pipeline has no bin/)"

for bd in .nxf-bin/*/ ; do
  if [ -d "\$bd" ]; then
    chmod -R +x "\$bd" 2>/dev/null
    abs="\$(cd "\$bd" 2>/dev/null && pwd)"
    if [ -n "\$abs" ]; then
      export PATH="\$abs:\$PATH"
      log "  prepended bin dir to PATH: \$abs"
    fi
  fi
done

log "step 3: patch absolute paths in .command.run"
if [ -f .command.run ]; then
  awk '
    {
      gsub(/\\/[^[:space:]]*\\/\\.command\\.run/, ".command.run")
      gsub(/\\/[^[:space:]]*\\/\\.command\\.sh/, ".command.sh")
      gsub(/\\/[^[:space:]]*\\/\\.command\\.begin/, ".command.begin")
      gsub(/\\/[^[:space:]]*\\/\\.command\\.trace/, ".command.trace")
      gsub(/\\/[^[:space:]]*\\/\\.exitcode/, ".exitcode")
      print
    }
  ' .command.run > .command.run.patched && mv .command.run.patched .command.run
  chmod 755 .command.run 2>/dev/null
  log "  .command.run patched (size now \$(wc -c < .command.run) bytes)"
else
  log "  no .command.run, will fall back to .command.sh"
fi

if [ -f .command.run ]; then
  _task_script='.command.run'
elif [ -f .command.sh ]; then
  _task_script='.command.sh'
else
  log "FATAL: missing .command.run AND .command.sh"
  exit 127
fi

log "step 4: run \$_task_script (effective PATH: \$PATH)"
unset NXF_CHDIR 2>/dev/null
bash "\$_task_script"
_exit_code=\$?
log "  task exited with \$_exit_code"

log "step 5: task exit captured (\$_exit_code); .exitcode is written + pushed LAST by the trap"
echo "[nf-task] end session=\$\${NF_SESSION_ID:--} head_job=\$\${NF_HEAD_JOB_ID:--} process=\$\${NF_PROCESS_NAME:--} task=\$\${NF_TASK_HASH:--} exit=\$_exit_code" >&2
log "bootstrap done — trap will push back"
exit "\$_exit_code"
""".stripIndent()
    }

    protected Integer awaitRemoteExitCode() {
        long deadline = System.currentTimeMillis() + completionTimeoutMillis
        while( System.currentTimeMillis() <= deadline ) {
            Integer exit = readRemoteExitCode()
            if( exit != null ) return exit
            sleepQuietly(1000L)
        }
        return null
    }

    protected Integer readRemoteExitCode() {
        // s5cmd cat doesn't exist; download to a temp file and read
        try {
            Path tmp = Files.createTempFile('s5cmd-exit-', '.txt')
            String cmd = cmdBuilder.buildCopy(remoteExitFile(), tmp.toString(), false)
            int rc = runShellQuiet(prefixWithEnvExports(cmd))
            if( rc != 0 ) {
                Files.deleteIfExists(tmp)
                return null
            }
            String txt = Files.readString(tmp).trim()
            Files.deleteIfExists(tmp)
            return txt ? Integer.parseInt(txt) : null
        } catch (Exception e) {
            log.debug("[NOMAD] nf-nomad-s5cmd readRemoteExitCode failed (transient ok): ${e.message}")
            return null
        }
    }

    protected void copyAllArtifacts() {
        // toS3Uri() — same normalisation as uploadCommandFiles(); see that method for rationale.
        String cmd = cmdBuilder.buildCopyDir(remoteTaskDir(), toS3Uri(task.workDir), false)
        runShell(prefixWithEnvExports(cmd))
    }

    protected void writeLocalExitCode(Integer code) {
        if( code == null ) return
        Files.writeString(task.workDir.resolve(TaskRun.CMD_EXIT), String.valueOf(code))
    }

    // ── shell helpers ─────────────────────────────────────────────────────

    /**
     * Prepend the configured AWS_* / S3_* env exports to a command line so the
     * operator-side s5cmd subprocess sees the same identity the worker would.
     * Falls back to the bare command when {@link S5cmdCommandBuilder#envExports}
     * produces nothing (e.g. profile-only configs that rely on ~/.aws).
     */
    protected String prefixWithEnvExports(String cmdline) {
        String exports = cmdBuilder.envExports()
        if( !exports?.trim() ) return cmdline
        return exports + '\n' + cmdline
    }

    protected void runShell(String cmdline) {
        Process proc = ['bash', '-c', cmdline].execute()
        StringBuffer out = new StringBuffer()
        StringBuffer err = new StringBuffer()
        proc.consumeProcessOutput(out, err)
        proc.waitFor()
        if( proc.exitValue() != 0 ) {
            throw new ProcessSubmitException(
                "[NOMAD] nf-nomad-s5cmd command failed (rc=${proc.exitValue()}): ${cmdline}\n${err}")
        }
    }

    protected int runShellQuiet(String cmdline) {
        Process proc = ['bash', '-c', cmdline].execute()
        StringBuffer out = new StringBuffer()
        StringBuffer err = new StringBuffer()
        proc.consumeProcessOutput(out, err)
        proc.waitFor()
        return proc.exitValue()
    }

    protected static void sleepQuietly(long millis) {
        try { Thread.sleep(millis) }
        catch (InterruptedException e) { Thread.currentThread().interrupt() }
    }

    // ── path helpers (mirrors RcloneNomadInterop) ─────────────────────────

    /**
     * Convert a NIO {@link Path} to a string that s5cmd (or a POSIX shell) can use.
     *
     * <p>Three cases:
     * <ul>
     *   <li><b>Local path</b> ({@code file://…} URI) — return {@code path.toString()},
     *       which gives the normal POSIX or Windows filesystem path expected by shell
     *       commands.</li>
     *   <li><b>S3 NIO (nf-amazon) path</b> — {@code toUri().toString()} returns
     *       {@code s3:///bucket/key} (empty authority; bucket in path-component) because
     *       the nf-amazon S3 filesystem registers without a host. Normalise to the
     *       canonical {@code s3://bucket/key} form that s5cmd requires.</li>
     *   <li><b>Any other cloud path</b> ({@code gs://}, {@code az://}, …) — return the
     *       URI string unchanged; those schemes are not expected here but shouldn't
     *       silently corrupt.</li>
     * </ul>
     */
    protected static String toS3Uri(Path path) {
        String uri = path.toUri().toString()
        if (uri.startsWith('file:///') || uri.startsWith('file:/') && !uri.startsWith('file://')) {
            // Local filesystem path — give the shell a plain OS path, not a file:// URI.
            return path.toString()
        }
        // s3:///bucket/key  →  s3://bucket/key
        if (uri.startsWith('s3:///')) {
            return 's3://' + uri.substring('s3:///'.length())
        }
        return uri
    }

    protected static String relativePathFromSessionWorkDir(Path taskWorkDir, Path sessionWorkDir) {
        if( taskWorkDir == null || sessionWorkDir == null ) return null
        try {
            Path taskAbs = taskWorkDir.toAbsolutePath().normalize()
            Path sessionAbs = sessionWorkDir.toAbsolutePath().normalize()
            if( !taskAbs.startsWith(sessionAbs) ) return null
            Path rel = sessionAbs.relativize(taskAbs)
            String value = rel.toString().replace('\\' as char, '/' as char)
            value = value.replaceAll('^/+', '').replaceAll('/+$', '')
            if( !value || value == '..' || value.startsWith('../') || value.contains('/../') ) return null
            return value
        } catch (Exception ignored) {
            return null
        }
    }

    protected static boolean isNextflowWorkPathLayout(String value) {
        return value && value ==~ /[0-9a-fA-F]{2}\/[0-9a-fA-F]+/
    }

    protected static Map readMap(Map source, String key) {
        if( source == null ) return Collections.emptyMap()
        Object value = source.get(key)
        return value instanceof Map ? (Map) value : Collections.emptyMap()
    }
}
