/*
 * Copyright 2026-, abc-cluster
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nextflow.s5cmd.nomad

import groovy.util.logging.Slf4j
import nextflow.exception.ProcessSubmitException
import nextflow.processor.TaskRun
import nextflow.s5cmd.config.S5cmdConfig
import nextflow.s5cmd.config.S5cmdWorkDirConfig
import nextflow.s5cmd.strategy.S5cmdCommandBuilder

import java.nio.file.Files
import java.nio.file.Path

/**
 * nf-s5cmd's distributed-workdir backend for nf-nomad. Discovered
 * reflectively by {@code nextflow.nomad.executor.NomadTaskHandler} —
 * this class deliberately does NOT declare an interface dependency on
 * nf-nomad to avoid cross-plugin compile coupling. It exposes the same
 * method shape that {@code DistributedWorkdirProvider} declares, and
 * nf-nomad wraps it via a reflective adapter.
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
 * <p>Sidecar mode (lifecycle-based prestart/poststop) and legal-transfer
 * policy enforcement are intentionally out of scope for v1 — nf-rclone's
 * implementation can serve those use cases until parity is needed.</p>
 */
@Slf4j
class S5cmdNomadInterop {

    private static final String TASK_DIR_VAR = '${NOMAD_TASK_DIR:-$PWD}/nf-s5cmd-task'

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

        Map s5cmdScope = readMap(this.sessionConfig, 's5cmd')
        this.s5cmdConfig = S5cmdConfig.fromMap(s5cmdScope as Map<String, Object>)
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
        submitEnv = buildTransferEnv()
        submitCommand = ['bash', '-c', bootstrapScript()]
    }

    List<String> getSubmitCommand() { submitCommand }
    Map<String, String> getSubmitEnv() { submitEnv }

    /** No lifecycle tasks in bootstrap mode. */
    List getLifecycleTasks() { Collections.emptyList() }

    /** Use Nextflow's default copy strategy — bootstrap script handles staging. */
    Object createCopyStrategy(boolean stagingDisabled) { null }

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
                '[NOMAD] nf-s5cmd workDir.enabled=true but `s5cmd.workDir.bucket` is missing')
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
                "[NOMAD] nf-s5cmd interop requires task workDir `${task.workDir}` to be nested under session workDir `${sessionWorkDir}`")
        }
        if( !isNextflowWorkPathLayout(fromSession) ) {
            throw new ProcessSubmitException(
                "[NOMAD] nf-s5cmd interop requires Nextflow workDir layout `NN/HASH`; found `${fromSession}`")
        }
        return fromSession
    }

    protected void uploadCommandFiles() {
        // Use s5cmd cp with a glob to upload .command.* into the remote task dir.
        String src = task.workDir.toString() + '/.command.*'
        String dst = remoteTaskDir()
        String cmd = cmdBuilder.buildCopy(src, dst, true)
        runShell(cmd)
    }

    protected Map<String, String> buildTransferEnv() {
        Map<String, String> env = new LinkedHashMap<>()
        env.put('NXF_S5CMD_REMOTE_WORKDIR', remoteTaskDir())
        env.put('NXF_S5CMD_REMOTE_EXITCODE', remoteExitFile())
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
     */
    protected String bootstrapScript() {
        String binary = s5cmdConfig.binary ?: 's5cmd'
        return """\
set -euo pipefail
# nf-s5cmd bootstrap PATH augmentation: when the cluster operator delivers
# s5cmd to every Nomad client via a host volume (e.g. abc-place-s5cmd
# sysbatch writing to /opt/nomad/scratch/bin/s5cmd, mounted at
# /nxf-work/bin/s5cmd inside the worker), we need to make sure the binary
# resolves regardless of the container image. Image-provided s5cmd wins.
export PATH="/nxf-work/bin:/opt/abc/bin:\$\${PATH:-/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin}"
TASK_DIR="${TASK_DIR_VAR}"
mkdir -p "\$TASK_DIR"
cd "\$TASK_DIR"

# Pull command files from S3
${binary} cp '\$\${NXF_S5CMD_REMOTE_WORKDIR}.command.*' ./

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
  ' .command.run > .command.run.patched
  mv .command.run.patched .command.run
  chmod 755 .command.run 2>/dev/null || true
fi

if [ -f .command.run ]; then
  _task_script='.command.run'
elif [ -f .command.sh ]; then
  _task_script='.command.sh'
else
  echo "[NOMAD] Missing .command.run/.command.sh in bootstrap task directory: \$TASK_DIR" >&2
  exit 127
fi
set +e
unset NXF_CHDIR 2>/dev/null || true
bash "\$_task_script"
_exit_code=\$?
set -e
printf '%s' "\$_exit_code" > .exitcode

# Push entire task dir back up so the operator can sync it
${binary} cp ./ "\$\${NXF_S5CMD_REMOTE_WORKDIR}"
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
            int rc = runShellQuiet(cmd)
            if( rc != 0 ) {
                Files.deleteIfExists(tmp)
                return null
            }
            String txt = Files.readString(tmp).trim()
            Files.deleteIfExists(tmp)
            return txt ? Integer.parseInt(txt) : null
        } catch (Exception e) {
            log.debug("[NOMAD] nf-s5cmd readRemoteExitCode failed (transient ok): ${e.message}")
            return null
        }
    }

    protected void copyAllArtifacts() {
        String cmd = cmdBuilder.buildCopyDir(remoteTaskDir(), task.workDir.toString(), false)
        runShell(cmd)
    }

    protected void writeLocalExitCode(Integer code) {
        if( code == null ) return
        Files.writeString(task.workDir.resolve(TaskRun.CMD_EXIT), String.valueOf(code))
    }

    // ── shell helpers ─────────────────────────────────────────────────────

    protected void runShell(String cmdline) {
        Process proc = ['bash', '-c', cmdline].execute()
        StringBuffer out = new StringBuffer()
        StringBuffer err = new StringBuffer()
        proc.consumeProcessOutput(out, err)
        proc.waitFor()
        if( proc.exitValue() != 0 ) {
            throw new ProcessSubmitException(
                "[NOMAD] nf-s5cmd command failed (rc=${proc.exitValue()}): ${cmdline}\n${err}")
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
