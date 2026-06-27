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
 *   <li><b>CHARACTERIZATION</b> — lock the stable shape of the emitted bash
 *       (cp form, flags, quoting, one-cp-per-input, EXIT-trap wiring, no
 *       {@code --exclude}) so future changes can't silently regress it.</li>
 *   <li><b>DESIRED-BEHAVIOR</b> — assert the rc-safety contract now
 *       implemented: stage-in and push-back VERIFY COMPLETENESS (capture
 *       s5cmd output, fail on {@code ERROR}/{@code IncompleteBody}, verify the
 *       expected object exists) rather than trusting rc. These were authored
 *       test-first as {@code @PendingFeature} and flipped to plain features
 *       once the detection fix landed.</li>
 * </ul>
 *
 * <p>The fix is DETECTION ONLY — the copy mechanism, endpoint and retry
 * behaviour are unchanged. Tracking: ISSUE-nf-nomad-s5cmd-large-object-staging.
 */
class S5cmdRcSafetySpec extends Specification {

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

    def 'CHARACTERIZATION: getStageInputFilesScript chains exactly one cp per input, preserving quoting + form'() {
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

        and: 'still exactly one s5cmd cp per input (rc-safety guards wrap each cp, they do not duplicate it)'
        // executable cp lines only — skip comments and the stderr-capture/grep guard lines.
        def cpLines = script.readLines().findAll {
            it.contains(' cp ') && it.contains('s5cmd') && !it.trim().startsWith('#')
        }
        cpLines.size() == 2
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

        and: 'rc-safety: the masking `|| log "...FAILED"` form is gone'
        // The fix replaced the swallow-and-log push with a captured-output
        // completeness check that overrides the exit code on partial failure.
        !script.contains('|| log "push-back of outputs to S3 FAILED"')
    }

    // ══════════════════════════════════════════════════════════════════════
    // DESIRED-BEHAVIOR — RED now (@PendingFeature). Will pass once the
    // rc-safety fix makes the emitted bash verify completeness.
    // ══════════════════════════════════════════════════════════════════════

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

    def 'DESIRED: stage-in verifies each expected input object actually arrived locally'() {
        given:
        def s = strategy()
        def f1 = tempDir.resolve('reads.fq'); Files.writeString(f1, 'x')

        when:
        String script = s.getStageInputFilesScript(['reads.fq': f1])

        then: 'after the cp, the script asserts the staged file exists (so a silent drop fails the task)'
        // The emitted guard tests the local target with [ ... -e './reads.fq' ]
        // (negated form: `if [ ! -e './reads.fq' ]; then ... exit 1`).
        (script.contains("-e './reads.fq'") || script.contains("-s './reads.fq'")
            || script.contains("test -e './reads.fq'") || script.contains("test -s './reads.fq'"))
        // and the failure path aborts the chained stage-in
        script.contains('exit 1')
    }

    def 'DESIRED: push-back fails the task (non-zero) on a partial recursive-copy failure instead of only logging'() {
        when:
        String script = bootstrapScript()

        then: 'the swallow-and-log masking is gone'
        !script.contains('|| log "push-back of outputs to S3 FAILED"')

        and: 'completeness is checked on the push: capture output + grep for ERROR/IncompleteBody'
        script.contains('grep') && (script.contains('ERROR') || script.contains('IncompleteBody'))

        and: 'a partial failure overrides a success code to non-zero so Nextflow retries'
        // On detection, _exit_code is forced non-zero before .exitcode is written.
        script.contains('_exit_code=1')
    }

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
