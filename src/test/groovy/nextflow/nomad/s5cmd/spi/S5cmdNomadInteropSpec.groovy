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
        // Source URIs (s3://… or local path strings) that should be reported as
        // directories. Lets a faked S3 input behave as a dir without a live S3
        // filesystem provider behind Files.isDirectory().
        Set<String> dirSources = [] as Set

        StubInterop(TaskRun task, Map sessionConfig, Path sessionWorkDir) {
            super(task, sessionConfig, sessionWorkDir)
        }

        @Override
        protected void runShell(String cmdline) {
            shellCalls << cmdline
            if( onShell ) onShell.call(cmdline)
        }

        // The artifact pull goes through the verifying variant (it must detect a
        // per-file failure reported with rc=0). Recorded identically so the
        // existing shellCalls assertions still describe what was executed;
        // verification behaviour itself is covered in S5cmdRcSafetySpec.
        @Override
        protected void runShellVerified(String cmdline) {
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

        @Override
        protected boolean isDirectoryInput(java.nio.file.Path source) {
            String key
            try { key = source.toUri()?.toString() } catch (Exception ignored) { key = null }
            if( key != null && dirSources.any { key.contains(it) } ) return true
            try { return source.toString() in dirSources ? true : super.isDirectoryInput(source) }
            catch (Exception ignored) { return false }
        }
    }

    /**
     * A faked S3 input Path. Only {@code toUri()} (→ s3://… scheme) is needed by
     * the production routing; directory-ness is controlled separately via
     * {@code StubInterop.dirSources}, since there is no live S3 filesystem here.
     */
    private Path s3Path(String s3Uri) {
        // nf-amazon S3 paths stringify to /bucket/key and toUri() to s3://bucket/key.
        URI uri = URI.create(s3Uri)
        return Spy(Path) {
            toUri() >> uri
            toString() >> s3Uri.replaceFirst('^s3://', '/')
        }
    }

    def 'resolveBinDirs reads getBinDirs from the TaskProcessor, not the Session'() {
        // getBinDirs() -> List<Path> is defined on TaskProcessor. Session has
        // only the singular getBinDir(). Calling the plural on Session throws
        // on every task and silently degrades to the project bin dir alone,
        // so module bin dirs never get staged.
        given:
        Path projectBin = tempDir.resolve('project/bin')
        Path moduleBin = tempDir.resolve('modules/foo/resources/usr/bin')
        Files.createDirectories(projectBin)
        Files.createDirectories(moduleBin)

        def session = Mock(nextflow.Session) {
            // Present, and deliberately different, so a fallback to the
            // singular is visible in the assertion below.
            getBinDir() >> projectBin
        }
        def processor = Mock(nextflow.processor.TaskProcessor) {
            getSession() >> session
            getBinDirs() >> [projectBin, moduleBin]
        }
        def task = Mock(TaskRun) {
            getWorkDir() >> tempDir.resolve('ab/cdef1234')
            getProcessor() >> processor
        }
        def interop = new S5cmdNomadInterop(task, enabledSession(), tempDir)

        when:
        def dirs = interop.resolveBinDirs()

        then:
        dirs == [projectBin, moduleBin]
    }

    def 'resolveBinDirs falls back to the project bin dir when getBinDirs is unavailable'() {
        given:
        Path projectBin = tempDir.resolve('project2/bin')
        Files.createDirectories(projectBin)

        def session = Mock(nextflow.Session) {
            getBinDir() >> projectBin
        }
        def processor = Mock(nextflow.processor.TaskProcessor) {
            getSession() >> session
            getBinDirs() >> { throw new MissingMethodException('getBinDirs', Object, [] as Object[]) }
        }
        def task = Mock(TaskRun) {
            getWorkDir() >> tempDir.resolve('ab/cdef1234')
            getProcessor() >> processor
        }
        def interop = new S5cmdNomadInterop(task, enabledSession(), tempDir)

        when:
        def dirs = interop.resolveBinDirs()

        then:
        dirs == [projectBin]
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

        and: 'pushes outputs first (plain recursive cp), then .exitcode strictly last'
        // .exitcode is written + pushed only AFTER the recursive output push, so its
        // remote presence implies the outputs were already staged; a preemption
        // mid-push leaves no remote .exitcode and the task is retried.
        // NB: a PLAIN `cp ./` is used deliberately — `s5cmd cp --exclude ".exitcode" ./`
        // with a non-wildcard dir source suppresses the recursive upload, which is
        // the 0.1.4 inter-task staging regression (downstream tasks 404 on stage-in).
        script.contains('cp ./ "$${NXF_S5CMD_REMOTE_WORKDIR}"')
        !script.contains('--exclude')   // regression guard: no exclude-filter on the recursive push
        script.contains('cp .exitcode "$${NXF_S5CMD_REMOTE_WORKDIR}.exitcode"')
        // the outputs-first push must appear before the .exitcode push
        script.indexOf('cp ./ "$${NXF_S5CMD_REMOTE_WORKDIR}"') < script.indexOf('cp .exitcode "$${NXF_S5CMD_REMOTE_WORKDIR}.exitcode"')
        // The push-back lines use the same global flags
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

    // ── input staging: same-bucket S3 inputs avoid server-side S3→S3 copy ──
    //
    // Regression guard for ISSUE-nf-nomad-s5cmd-large-object-staging: a direct
    // server-side S3→S3 copy of a large same-bucket input fails on MinIO with
    // IncompleteBody (HTTP 400). The fix routes same-bucket inputs through a
    // local-disk round-trip (GET then PUT). The defining invariant: NO single
    // emitted command may have BOTH an s3:// source AND an s3:// destination.

    private TaskRun mockTaskWithInputs(Path workDir, Map<String, Path> inputs) {
        Mock(TaskRun) {
            getWorkDir() >> workDir
            getInputFilesMap() >> inputs
        }
    }

    /** Commands whose source AND destination are both s3:// (the forbidden direct copy). */
    private static List<String> s3ToS3Copies(List<String> calls) {
        calls.findAll { c ->
            def m = (c =~ /cp\b.*'(s3:\/\/[^']+)'\s+'(s3:\/\/[^']+)'/)
            m.find()
        }
    }

    def 'same-bucket S3 directory input round-trips via local disk (GET then PUT), never a direct S3→S3 copy'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        String srcUri = 's3://nextflow-work/refs/genome.idx'
        def task = mockTaskWithInputs(workDir, ['genome': s3Path(srcUri)])
        def interop = new StubInterop(task, enabledSession(), sessionDir)
        interop.dirSources << srcUri

        when:
        interop.prepare()

        then: 'no command has both an s3:// source AND an s3:// destination'
        s3ToS3Copies(interop.shellCalls).isEmpty()

        and: 'there is a download leg: from the source s3 prefix → a local temp dir'
        def download = interop.shellCalls.find { it.contains("'${srcUri}/*'") && !it.contains('inputs/') }
        download != null
        // local destination (not an s3:// dst)
        !(download =~ /'${java.util.regex.Pattern.quote(srcUri)}\/\*'\s+'s3:\/\//).find()

        and: 'there is an upload leg: from the local temp dir → the per-task inputs/ dir'
        def upload = interop.shellCalls.find {
            it.contains("'s3://nextflow-work/sessions/abc/ab/cdef1234/inputs/genome/'") && it.contains('cp')
        }
        upload != null
        // the upload SOURCE is local (no s3:// before the inputs/ destination)
        !(upload =~ /'s3:\/\/[^']+'\s+'s3:\/\/nextflow-work\/sessions\/abc/).find()

        and: 'download precedes upload'
        interop.shellCalls.indexOf(download) < interop.shellCalls.indexOf(upload)
    }

    def 'same-bucket S3 file input round-trips via local disk (GET then PUT), never a direct S3→S3 copy'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        String srcUri = 's3://nextflow-work/refs/sample.bam'
        def task = mockTaskWithInputs(workDir, ['sample.bam': s3Path(srcUri)])
        def interop = new StubInterop(task, enabledSession(), sessionDir)
        // not added to dirSources → treated as a single file

        when:
        interop.prepare()

        then: 'no direct S3→S3 copy'
        s3ToS3Copies(interop.shellCalls).isEmpty()

        and: 'download leg pulls the source object → a local temp file'
        def download = interop.shellCalls.find { it.contains("'${srcUri}'") && !it.contains('inputs/') }
        download != null

        and: 'upload leg pushes the local temp file → inputs/<stageName>'
        def upload = interop.shellCalls.find {
            it.contains("'s3://nextflow-work/sessions/abc/ab/cdef1234/inputs/sample.bam'")
        }
        upload != null
        !(upload =~ /'s3:\/\/[^']+'\s+'s3:\/\/nextflow-work\/sessions\/abc/).find()

        and: 'download precedes upload'
        interop.shellCalls.indexOf(download) < interop.shellCalls.indexOf(upload)
    }

    def 'same-bucket S3 file with a nested stage name does not throw "Invalid prefix or suffix" (FASTQC regression)'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        String srcUri = 's3://nextflow-work/refs/SAMPLE_1.fastq.gz'
        // Nested stage name (carries a '/') — e.g. FASTQC stages its reads under a
        // subdirectory. The separator must NOT leak into the createTempFile suffix,
        // or Java throws java.lang.IllegalArgumentException: Invalid prefix or suffix.
        def task = mockTaskWithInputs(workDir, ['fastqc/SAMPLE_1.fastq.gz': s3Path(srcUri)])
        def interop = new StubInterop(task, enabledSession(), sessionDir)

        when:
        interop.prepare()

        then: 'temp-file creation no longer throws, and no direct S3→S3 copy is emitted'
        noExceptionThrown()
        s3ToS3Copies(interop.shellCalls).isEmpty()

        and: 'download leg pulls the source object → a local temp file'
        def download = interop.shellCalls.find { it.contains("'${srcUri}'") && !it.contains('inputs/') }
        download != null

        and: 'upload leg pushes the local temp file → inputs/<nested stageName>'
        def upload = interop.shellCalls.find {
            it.contains("'s3://nextflow-work/sessions/abc/ab/cdef1234/inputs/fastqc/SAMPLE_1.fastq.gz'")
        }
        upload != null

        and: 'download precedes upload'
        interop.shellCalls.indexOf(download) < interop.shellCalls.indexOf(upload)
    }

    def 'tempSuffixFor strips path separators so createTempFile never sees a nested name'() {
        expect:
        S5cmdNomadInterop.tempSuffixFor(input) == expected

        where:
        input                      | expected
        'sample.bam'               | '-sample.bam'
        'fastqc/SAMPLE_1.fastq.gz' | '-SAMPLE_1.fastq.gz'
        'a/b/c/read.fq.gz'         | '-read.fq.gz'
        ''                         | '.tmp'
        null                       | '.tmp'
    }

    def 'local file input still uses a single direct upload (no round-trip)'() {
        given:
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = makeNfTask(sessionDir)
        Path localInput = tempDir.resolve('reads.fq')
        Files.writeString(localInput, 'data\n')
        def task = mockTaskWithInputs(workDir, ['reads.fq': localInput])
        def interop = new StubInterop(task, enabledSession(), sessionDir)

        when:
        interop.prepare()

        then: 'exactly one input-staging command: a single PUT local→inputs/'
        def inputCmds = interop.shellCalls.findAll { it.contains('inputs/reads.fq') }
        inputCmds.size() == 1
        inputCmds[0].contains(localInput.toAbsolutePath().toString())
        inputCmds[0].contains("'s3://nextflow-work/sessions/abc/ab/cdef1234/inputs/reads.fq'")

        and: 'no s3:// source appears for a purely local input'
        s3ToS3Copies(interop.shellCalls).isEmpty()
    }
}
