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
package nextflow.nomad.s5cmd

import nextflow.nomad.s5cmd.config.S5cmdConfig
import nextflow.nomad.s5cmd.spi.S5cmdNomadInterop
import nextflow.nomad.s5cmd.strategy.S5cmdCommandBuilder
import nextflow.nomad.s5cmd.strategy.S5cmdFileCopyStrategy
import nextflow.processor.TaskRun
import spock.lang.PendingFeature
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

/**
 * rc-safety of s5cmd transfers — characterization + desired-behavior suite.
 *
 * <h2>The bug this suite anchors</h2>
 * <p>s5cmd exits {@code rc=0} even on a PARTIAL failure: per-file errors
 * (e.g. {@code IncompleteBody} on a large object, a dropped file in a
 * recursive push) are reported on stderr ONLY, while the process still
 * returns 0. The plugin trusts rc, so two real failures go undetected:</p>
 *
 * <ol>
 *   <li><b>STAGE-IN</b>: a per-task {@code path} input copy fails with a
 *       per-file error but s5cmd returns rc=0 → the task runs without its
 *       input → a downstream 404.</li>
 *   <li><b>PUSH-BACK</b>: the bootstrap EXIT-trap recursive
 *       {@code s5cmd cp ./ "$NXF_S5CMD_REMOTE_WORKDIR"} silently drops files
 *       (e.g. {@code versions.yml}, a transient {@code _tmp/} dir) but
 *       returns rc=0 → Nextflow 404s on the declared output.</li>
 * </ol>
 *
 * <h2>Two kinds of test in this file</h2>
 * <ul>
 *   <li><b>CHARACTERIZATION</b> — GREEN now. Lock in the current shape of the
 *       emitted bash so the upcoming rc-safety fix can't silently regress the
 *       cp form, flags, quoting, or the EXIT-trap structure.</li>
 *   <li><b>DESIRED-BEHAVIOR</b> — RED now, marked {@link PendingFeature}.
 *       Assert that the emitted stage-in and push-back bash VERIFIES
 *       COMPLETENESS (captures s5cmd stderr and exits non-zero on
 *       {@code ERROR}/{@code IncompleteBody}, and/or verifies expected objects
 *       exist) rather than trusting rc. {@code @PendingFeature} keeps the build
 *       GREEN while unimplemented, and FAILS the build the moment the fix lands
 *       and the assertion unexpectedly passes — the TDD signal we want.</li>
 * </ul>
 *
 * Tracking: ISSUE-nf-nomad-s5cmd-large-object-staging.
 */
class S5cmdRcSafetySpec extends Specification {

    static final String PENDING_REASON =
        'rc-safety fix pending — see ISSUE-nf-nomad-s5cmd-large-object-staging'

    @TempDir
    Path tempDir

    // ── shared fixtures ───────────────────────────────────────────────────

    private S5cmdConfig cfg() {
        return S5cmdConfig.fromMap(
            s3: [endpoint: 'http://rustfs:9900', usePathStyle: true,
                 accessKeyId: 'AK', secretAccessKey: 'SK'],
            cp: [concurrency: 4, numWorkers: 32, retryCount: 5, logLevel: 'info'])
    }

    private S5cmdCommandBuilder builder() {
        return new S5cmdCommandBuilder(cfg())
    }

    private S5cmdFileCopyStrategy strategy(
            String remoteTaskUrl = 's3://nextflow-work/sessions/abc/ab/cdef1234/') {
        return new S5cmdFileCopyStrategy(
                cfg(), tempDir, tempDir, 'copy', 'copy', remoteTaskUrl)
    }

    private Map enabledSession() {
        return [s5cmd: [
            enabled: true,
            binary : '/nxf-work/bin/s5cmd',
            paths  : ['s3://nextflow-work/'],
            s3     : [endpoint: 'http://rustfs:9900', usePathStyle: true,
                      accessKeyId: 'AK', secretAccessKey: 'SK'],
            cp     : [concurrency: 4, numWorkers: 32, retryCount: 5, logLevel: 'info'],
            workDir: [enabled: true, bucket: 's3://nextflow-work', prefix: 'sessions/abc'],
        ]]
    }

    private S5cmdNomadInterop interopAndPrepare() {
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = sessionDir.resolve('ab').resolve('cdef1234')
        Files.createDirectories(workDir)
        def task = Mock(TaskRun) { getWorkDir() >> workDir }
        return new S5cmdNomadInterop(task, enabledSession(), sessionDir)
    }

    /** The rendered bootstrap script the worker runs. */
    private String bootstrapScript() {
        return interopAndPrepare().bootstrapScript()
    }

    // ══════════════════════════════════════════════════════════════════════
    // CHARACTERIZATION — GREEN now. Pin the *current* emitted shape so the
    // upcoming fix preserves it instead of rewriting it from scratch.
    // ══════════════════════════════════════════════════════════════════════

    // ── stage-in cp form (buildCopy / buildCopyDir) ───────────────────────

    def 'CHARACTERIZATION: buildCopy emits a single `cp ... <src> <dst>` with both args single-quoted'() {
        when:
        def cmd = builder().buildCopy('s3://b/inputs/reads.fq', './reads.fq')

        then: 'current form is one cp call, src then dst, each single-quoted, no rc/stderr check'
        cmd ==~ /.*\bcp\b.*'s3:\/\/b\/inputs\/reads\.fq'\s+'\.\/reads\.fq'\s*$/
        cmd.contains("'s3://b/inputs/reads.fq' './reads.fq'")
        // No completeness machinery exists yet on this raw builder output.
        !cmd.contains('||')
        !cmd.contains('grep')
        !cmd.contains('2>')
    }

    def 'CHARACTERIZATION: buildCopyDir uses the glob src + trailing-slash dst form'() {
        when:
        def cmd = builder().buildCopyDir('s3://b/inputs/index', './index')

        then:
        cmd.contains("'s3://b/inputs/index/*' './index/'")
    }

    def 'CHARACTERIZATION: getStageInputFilesScript chains one cp per input with no per-line rc check'() {
        given:
        def s = strategy()
        def f1 = tempDir.resolve('reads.fq'); Files.writeString(f1, 'x')
        def dir = tempDir.resolve('index'); Files.createDirectory(dir)

        when:
        String script = s.getStageInputFilesScript(['reads.fq': f1, 'index': dir])

        then: 'a file input → plain cp into ./<name>'
        script.contains("'s3://nextflow-work/sessions/abc/ab/cdef1234/inputs/reads.fq'")
        script.contains("'./reads.fq'")

        and: 'a directory input → recursive glob form into ./<name>/'
        script.contains("'s3://nextflow-work/sessions/abc/ab/cdef1234/inputs/index/*'")
        script.contains("'./index/'")

        and: 'current behaviour trusts rc — none of the cp lines guard on stderr/ERROR'
        // executable cp lines only — skip the leading `# ...stage-in (s5cmd cp...)` comment
        def cpLines = script.readLines().findAll {
            it.contains(' cp ') && !it.trim().startsWith('#')
        }
        cpLines.size() == 2
        cpLines.every { !it.contains('||') && !it.contains('grep') }
    }

    // ── bootstrap push-back (EXIT trap) ───────────────────────────────────

    def 'CHARACTERIZATION: push-back uses a plain recursive `cp ./` to the remote workdir (no --exclude)'() {
        when:
        String script = bootstrapScript()

        then: 'the recursive output push is `s5cmd ... cp ./ "$${NXF_S5CMD_REMOTE_WORKDIR}"`'
        script.contains('cp ./ "$${NXF_S5CMD_REMOTE_WORKDIR}"')
        // Regression guard carried over from the 0.1.5 inter-task staging fix:
        // an --exclude filter on a non-wildcard dir source suppresses the upload.
        !script.contains('--exclude')
    }

    def 'CHARACTERIZATION: push-back is wired into the EXIT trap, redirects to the debug log, writes .exitcode LAST'() {
        when:
        String script = bootstrapScript()

        then: 'the trap structure that the fix must preserve'
        script.contains('trap push_debug_then_exit EXIT')
        script.contains('push_debug_then_exit() {')
        // outputs pushed first, .exitcode written + pushed strictly after
        int outPush = script.indexOf('cp ./ "$${NXF_S5CMD_REMOTE_WORKDIR}"')
        int exitPush = script.indexOf('cp .exitcode "$${NXF_S5CMD_REMOTE_WORKDIR}.exitcode"')
        outPush > 0
        exitPush > outPush

        and: 'today the only failure signal is a log line via `|| log` — NOT a non-zero exit'
        // The current push-back swallows partial failures: `|| log "...FAILED"`
        // logs but does not propagate a non-zero rc out of the trap.
        script.contains('|| log "push-back of outputs to S3 FAILED"')
    }

    // ══════════════════════════════════════════════════════════════════════
    // DESIRED-BEHAVIOR — RED now (@PendingFeature). Will pass once the
    // rc-safety fix makes the emitted bash verify completeness.
    // ══════════════════════════════════════════════════════════════════════

    @PendingFeature(reason = PENDING_REASON)
    def 'DESIRED: stage-in captures s5cmd stderr and fails on a per-file ERROR / IncompleteBody'() {
        given:
        def s = strategy()
        def f1 = tempDir.resolve('reads.fq'); Files.writeString(f1, 'x')

        when:
        String script = s.getStageInputFilesScript(['reads.fq': f1])

        then: 'stage-in must not trust rc — it captures stderr and greps for failure tokens'
        // The emitted bash should redirect/capture s5cmd stderr ...
        (script.contains('2>') || script.contains('stderr'))
        // ... and treat a per-file error as fatal (exit non-zero), e.g. by grepping.
        script.toLowerCase().contains('error') || script.contains('IncompleteBody')
        (script.contains('grep') || script.contains('exit 1') || script.contains('exit "'))
    }

    @PendingFeature(reason = PENDING_REASON)
    def 'DESIRED: stage-in verifies each expected input object actually arrived locally'() {
        given:
        def s = strategy()
        def f1 = tempDir.resolve('reads.fq'); Files.writeString(f1, 'x')

        when:
        String script = s.getStageInputFilesScript(['reads.fq': f1])

        then: 'after the cp, the script asserts the staged file exists (so a silent drop fails the task)'
        // e.g. `[ -s './reads.fq' ] || { echo ...; exit 1; }`  or  `test -e`
        (script.contains("[ -s './reads.fq'") || script.contains("[ -e './reads.fq'")
            || script.contains("test -s './reads.fq'") || script.contains("test -e './reads.fq'"))
    }

    @PendingFeature(reason = PENDING_REASON)
    def 'DESIRED: push-back fails the task (non-zero) on a partial recursive-copy failure instead of only logging'() {
        when:
        String script = bootstrapScript()

        then: 'a dropped file in the recursive push must surface as a non-zero rc out of the trap'
        // The current `|| log "...FAILED"` must be replaced/augmented so that a
        // partial failure sets a non-zero exit (the marker push must then be
        // skipped, leaving no remote .exitcode → the task is retried).
        !script.contains('|| log "push-back of outputs to S3 FAILED"') ||
            (script.contains('IncompleteBody') || script.toLowerCase().contains('grep'))
        // Completeness must be checked on the push: capture stderr and detect ERROR.
        (script.contains('grep') && (script.contains('ERROR') || script.contains('IncompleteBody')))
    }

    @PendingFeature(reason = PENDING_REASON)
    def 'DESIRED: push-back does not silently swallow a missing declared output such as versions.yml'() {
        when:
        String script = bootstrapScript()

        then: 'the push-back path verifies output completeness — captures stderr and fails on errors'
        // Concretely: the recursive `cp ./` is followed by a completeness check
        // (stderr captured + grepped for ERROR/IncompleteBody, or a remote
        // object-existence verification) so a dropped versions.yml fails the task.
        script.contains('cp ./ "$${NXF_S5CMD_REMOTE_WORKDIR}"')
        (script.contains('2>') &&
            (script.contains('ERROR') || script.contains('IncompleteBody')) &&
            script.contains('grep'))
    }
}
