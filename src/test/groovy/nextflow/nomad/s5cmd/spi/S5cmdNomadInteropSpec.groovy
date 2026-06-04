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

import nextflow.exception.ProcessSubmitException
import nextflow.processor.TaskRun
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

class S5cmdNomadInteropSpec extends Specification {

    @TempDir
    Path tempDir

    // ── helpers ───────────────────────────────────────────────────────────

    /**
     * Stub subclass that intercepts every shell-out (uploadCommandFiles,
     * readRemoteExitCode, copyAllArtifacts) so tests stay hermetic. Records
     * each command line so we can assert on the exact `s5cmd cp` invocations.
     */
    private static class StubInterop extends S5cmdNomadInterop {
        final List<String> shellCalls = []
        Map<String, Integer> quietExitOverrides = [:]
        Closure<Void> onShell = null

        StubInterop(TaskRun task, Map sessionConfig, Path sessionWorkDir) {
            super(task, sessionConfig, sessionWorkDir)
        }

        @Override
        protected void runShell(String cmdline) {
            shellCalls << cmdline
            if( onShell ) onShell.call(cmdline)
        }

        @Override
        protected int runShellQuiet(String cmdline) {
            shellCalls << cmdline
            for( entry in quietExitOverrides ) {
                if( cmdline.contains(entry.key) ) return entry.value
            }
            return 0
        }
    }

    private TaskRun mockTaskAt(Path workDir) {
        Mock(TaskRun) { getWorkDir() >> workDir }
    }

    private Path makeNfTask(Path sessionDir = tempDir.resolve('session')) {
        Path workDir = sessionDir.resolve('ab').resolve('cdef1234')
        Files.createDirectories(workDir)
        Files.writeString(workDir.resolve('.command.sh'), 'echo hi\n')
        Files.writeString(workDir.resolve('.command.run'), 'bash .command.sh\n')
        return workDir
    }

    private Map enabledSession(Map overrides = [:]) {
        Map workDir = [enabled: true, bucket: 's3://nextflow-work', prefix: 'sessions/abc'] + (overrides.workDir ?: [:])
        Map s3 = [endpoint: 'http://rustfs:9900', usePathStyle: true,
                  accessKeyId: 'AK', secretAccessKey: 'SK'] + (overrides.s3 ?: [:])
        Map cp = [concurrency: 4, numWorkers: 32, retryCount: 5, logLevel: 'info'] + (overrides.cp ?: [:])
        return [s5cmd: [enabled: true, paths: ['s3://nextflow-work/'],
                        s3: s3, cp: cp, workDir: workDir]]
    }

    // ── name + flags ──────────────────────────────────────────────────────

    def 'name() returns the provider id'() {
        expect:
        new S5cmdNomadInterop(mockTaskAt(tempDir.resolve('w')), [:], tempDir).name() == 's5cmd'
    }

    def 'isEnabled is false when only s5cmd.enabled is true (workDir omitted)'() {
        given:
        def session = [s5cmd: [enabled: true, paths: ['s3://x/']]]
        def task = mockTaskAt(tempDir.resolve('a/b/c'))

        expect:
        !new S5cmdNomadInterop(task, session, tempDir).isEnabled()
    }

    def 'isEnabled is false when workDir.enabled is true but s5cmd.enabled is false'() {
        given:
        def session = [s5cmd: [enabled: false, workDir: [enabled: true, bucket: 's3://b']]]
        def task = mockTaskAt(tempDir.resolve('a/b/c'))

        expect:
        !new S5cmdNomadInterop(task, session, tempDir).isEnabled()
    }

    def 'isEnabled is true when both flags set and bucket present'() {
        given:
        def task = mockTaskAt(tempDir.resolve('a/b/c'))

        expect:
        new S5cmdNomadInterop(task, enabledSession(), tempDir).isEnabled()
    }

    def 'isExternallyStaged is always false in bootstrap mode'() {
        expect:
        !new S5cmdNomadInterop(mockTaskAt(tempDir.resolve('a/b/c')), enabledSession(), tempDir).isExternallyStaged()
    }

    def 'createCopyStrategy returns an S5cmdFileCopyStrategy that emits stage-in s5cmd cp lines'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = sessionDir.resolve('ab').resolve('cdef1234')
        Files.createDirectories(workDir)
        def interop = new S5cmdNomadInterop(mockTaskAt(workDir), enabledSession(), sessionDir)

        when:
        def strategy = interop.createCopyStrategy(false)

        then:
        strategy != null
        strategy.class.name == 'nextflow.nomad.s5cmd.strategy.S5cmdFileCopyStrategy'

        and: 'stage-in script pulls each input from the per-task remote inputs/ dir'
        def stageIn = strategy.getStageInputFilesScript([
            'reads.fq': Path.of('/local/data/reads.fq')
        ])
        stageIn.contains('s5cmd')
        stageIn.contains("'s3://nextflow-work/sessions/abc/ab/cdef1234/inputs/reads.fq'")
        stageIn.contains("'./reads.fq'")
    }

    def 'getLifecycleTasks is always empty in bootstrap mode'() {
        expect:
        new S5cmdNomadInterop(mockTaskAt(tempDir.resolve('a/b/c')), enabledSession(), tempDir).lifecycleTasks == []
    }

    // ── pre-prepare accessors ─────────────────────────────────────────────

    def 'submitCommand and submitEnv start empty before prepare()'() {
        given:
        def interop = new S5cmdNomadInterop(mockTaskAt(tempDir.resolve('a/b/c')), enabledSession(), tempDir)

        expect:
        interop.submitCommand == []
        interop.submitEnv == [:]
    }

    // ── path computations ─────────────────────────────────────────────────

    def 'remoteExitHint composes bucket + prefix + NN/HASH + .exitcode'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = sessionDir.resolve('ab').resolve('cdef1234')
        def interop = new S5cmdNomadInterop(mockTaskAt(workDir), enabledSession(), sessionDir)

        expect:
        interop.remoteExitHint == 's3://nextflow-work/sessions/abc/ab/cdef1234/.exitcode'
    }

    def 'remoteExitHint handles bucket without trailing slash and missing prefix'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = sessionDir.resolve('ab').resolve('cdef1234')
        def session = enabledSession(workDir: [enabled: true, bucket: 's3://only-bucket', prefix: null])
        def interop = new S5cmdNomadInterop(mockTaskAt(workDir), session, sessionDir)

        expect:
        interop.remoteExitHint == 's3://only-bucket/ab/cdef1234/.exitcode'
    }

    def 'taskRemotePath rejects work dirs that escape the session dir'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Files.createDirectories(sessionDir)
        Path stranded = tempDir.resolve('elsewhere').resolve('ab').resolve('cdef1234')
        Files.createDirectories(stranded)
        def interop = new StubInterop(mockTaskAt(stranded), enabledSession(), sessionDir)

        when:
        interop.prepare()

        then:
        def e = thrown(ProcessSubmitException)
        e.message.contains('to be nested under session workDir')
    }

    def 'taskRemotePath rejects layouts that are not NN/HASH'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path bad = sessionDir.resolve('not-nf-shaped')
        Files.createDirectories(bad)
        def interop = new StubInterop(mockTaskAt(bad), enabledSession(), sessionDir)

        when:
        interop.prepare()

        then:
        def e = thrown(ProcessSubmitException)
        e.message.contains('Nextflow workDir layout')
    }

    // ── prepare() — submit shape ──────────────────────────────────────────

    def 'prepare() builds submitCommand as bash -c <bootstrap script>'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        def interop = new StubInterop(mockTaskAt(workDir), enabledSession(), sessionDir)

        when:
        interop.prepare()

        then:
        interop.submitCommand.size() == 3
        interop.submitCommand[0] == 'bash'
        interop.submitCommand[1] == '-c'
        interop.submitCommand[2].startsWith('set -uo pipefail')
    }

    def 'prepare() uploads .command.* to the remote task dir via s5cmd cp'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        def interop = new StubInterop(mockTaskAt(workDir), enabledSession(), sessionDir)

        when:
        interop.prepare()

        then:
        // Exactly one shell call (the upload). The bootstrap script itself
        // doesn't run on the operator side.
        interop.shellCalls.size() == 1
        interop.shellCalls[0].contains('s5cmd')
        interop.shellCalls[0].contains('cp')
        interop.shellCalls[0].contains(workDir.toString() + '/.command.*')
        interop.shellCalls[0].contains('s3://nextflow-work/sessions/abc/ab/cdef1234/')
    }

    def 'prepare() raises a clear error when workDir.bucket is missing'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        def session = [s5cmd: [enabled: true, workDir: [enabled: true]]]   // no bucket
        def interop = new StubInterop(mockTaskAt(workDir), session, sessionDir)

        when:
        interop.prepare()

        then:
        thrown(IllegalArgumentException)         // S5cmdWorkDirConfig.validate
    }

    // ── bootstrap script shape ────────────────────────────────────────────

    def 'bootstrap script prepends /nxf-work/bin to PATH so the host-volume binary wins'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        def interop = new StubInterop(mockTaskAt(workDir), enabledSession(), sessionDir)

        when:
        interop.prepare()
        String script = interop.submitCommand[2]

        then:
        script.contains('export PATH="/nxf-work/bin:')
    }

    def 'bootstrap script pulls .command.* down then runs the task then pushes everything back'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        def interop = new StubInterop(mockTaskAt(workDir), enabledSession(), sessionDir)

        when:
        interop.prepare()
        String script = interop.submitCommand[2]

        then: 'pulls .command.* from the remote workdir — full global flags embedded'
        // global() produces: s5cmd --endpoint-url <url> --log info -r N -numworkers N
        // The key assertion is that the script contains both the binary+flags prefix AND
        // the cp argument; checking the pull pattern covers both.
        script.contains('cp "$${NXF_S5CMD_REMOTE_WORKDIR}.command.*" ./')
        script.contains('s5cmd --endpoint-url http://rustfs:9900')

        and: 'writes the local exit code marker'
        script.contains("printf '%s' \"\$_exit_code\" > .exitcode")

        and: 'pushes the entire task dir back at the end'
        script.contains('cp ./ "$${NXF_S5CMD_REMOTE_WORKDIR}"')
        // The push-back line uses the same global flags
        script.contains('s5cmd --endpoint-url http://rustfs:9900')

        and: 'falls back to .command.sh when .command.run is absent'
        script.contains("elif [ -f .command.sh ]; then")
    }

    def 'bootstrap script honours a custom binary path from s5cmd.binary'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        def session = enabledSession()
        ((Map) session.s5cmd).binary = '/custom/bin/s5cmd'
        def interop = new StubInterop(mockTaskAt(workDir), session, sessionDir)

        when:
        interop.prepare()
        String script = interop.submitCommand[2]

        then: 'custom binary appears in the bootstrap s5cmd invocation with global flags'
        // global() now starts with the custom binary path, followed by global flags
        script.contains('/custom/bin/s5cmd --endpoint-url')
        script.contains('cp "$${NXF_S5CMD_REMOTE_WORKDIR}.command.*" ./')
    }

    // ── env exports ───────────────────────────────────────────────────────

    def 'submitEnv exposes the remote workdir + exit-code paths plus AWS connection vars'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        def interop = new StubInterop(mockTaskAt(workDir), enabledSession(), sessionDir)

        when:
        interop.prepare()
        Map<String, String> env = interop.submitEnv

        then: 'workdir + exit-code markers point at the per-task S3 location'
        env['NXF_S5CMD_REMOTE_WORKDIR'] == 's3://nextflow-work/sessions/abc/ab/cdef1234/'
        env['NXF_S5CMD_REMOTE_EXITCODE'] == 's3://nextflow-work/sessions/abc/ab/cdef1234/.exitcode'

        and: 'AWS auth + region propagate so the worker s5cmd has the same identity'
        env['AWS_ACCESS_KEY_ID'] == 'AK'
        env['AWS_SECRET_ACCESS_KEY'] == 'SK'
        env['AWS_REGION'] == 'us-east-1'

        and: 'endpoint + path-style flag both surface'
        env['S3_ENDPOINT_URL'] == 'http://rustfs:9900'
        env['S3_USE_PATH_STYLE'] == 'true'
    }

    def 'submitEnv prefers profile and exports it as AWS_PROFILE'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        def session = enabledSession(s3: [profile: 'cluster-dev'])
        def interop = new StubInterop(mockTaskAt(workDir), session, sessionDir)

        when:
        interop.prepare()

        then:
        interop.submitEnv['AWS_PROFILE'] == 'cluster-dev'
    }

    // ── session-level bin dedupe ──────────────────────────────────────────

    def 'sessionBinRoot uses workDir root + _nxf-bin + sessionId so multiple sessions cannot collide'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        def interop = new StubInterop(mockTaskAt(workDir), enabledSession(), sessionDir)

        expect: 'two distinct session ids produce distinct roots'
        interop.sessionBinRoot('sess-A') == 's3://nextflow-work/sessions/abc/_nxf-bin/sess-A/'
        interop.sessionBinRoot('sess-B') == 's3://nextflow-work/sessions/abc/_nxf-bin/sess-B/'
        interop.sessionBinDirUrl('sess-A', 0) == 's3://nextflow-work/sessions/abc/_nxf-bin/sess-A/0/'
    }

    def 'submitEnv carries NXF_S5CMD_SESSION_BIN_DIR pointing at the session-level root'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        def interop = new StubInterop(mockTaskAt(workDir), enabledSession(), sessionDir)

        when:
        interop.prepare()

        then:
        interop.submitEnv['NXF_S5CMD_SESSION_BIN_DIR'].startsWith('s3://nextflow-work/sessions/abc/_nxf-bin/')
        interop.submitEnv['NXF_S5CMD_SESSION_BIN_DIR'].endsWith('/')
    }

    // ── synchronizeCompletion ─────────────────────────────────────────────

    def 'synchronizeCompletion returns the remote exit code and writes it locally'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        def interop = new StubInterop(mockTaskAt(workDir), enabledSession(), sessionDir) {
            @Override
            protected Integer readRemoteExitCode() {
                return 42
            }
        }

        when:
        Integer exit = interop.synchronizeCompletion()

        then:
        exit == 42
        Files.readString(workDir.resolve('.exitcode')).trim() == '42'

        and: 'copyAllArtifacts shelled out the recursive cp back'
        interop.shellCalls.any { it.contains('cp') && it.contains(workDir.toString()) }
    }

    def 'synchronizeCompletion returns null when the remote exitcode never appears within the timeout'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        def session = enabledSession(workDir: [enabled: true, bucket: 's3://nextflow-work',
                                               prefix: 'sessions/abc', completionTimeout: '100ms'])
        def interop = new StubInterop(mockTaskAt(workDir), session, sessionDir) {
            @Override
            protected Integer readRemoteExitCode() { null }      // never appears
        }

        when:
        Integer exit = interop.synchronizeCompletion()

        then:
        exit == null
        // Local exit-code marker should NOT be written when remote was missing
        !Files.exists(workDir.resolve('.exitcode'))
    }

    def 'synchronizeCompletion is a no-op when the provider is disabled'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        def session = [s5cmd: [enabled: false]]
        def interop = new StubInterop(mockTaskAt(workDir), session, sessionDir)

        expect:
        interop.synchronizeCompletion() == null
        interop.shellCalls == []
    }
}
