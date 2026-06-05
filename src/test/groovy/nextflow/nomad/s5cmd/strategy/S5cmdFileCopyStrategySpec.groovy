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
package nextflow.nomad.s5cmd.strategy

import nextflow.nomad.s5cmd.config.S5cmdConfig
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

class S5cmdFileCopyStrategySpec extends Specification {

    @TempDir
    Path tempDir

    private S5cmdConfig configWith(List<String> publishPaths = [], List<String> paths = []) {
        return S5cmdConfig.fromMap(
            paths       : paths,
            publishPaths: publishPaths,
            s3: [endpoint: 'http://rustfs:9900', usePathStyle: true,
                 accessKeyId: 'AK', secretAccessKey: 'SK'],
            cp: [concurrency: 4, numWorkers: 32, retryCount: 5, logLevel: 'info'],
        )
    }

    private S5cmdFileCopyStrategy makeStrategy(List<String> publishPaths = [],
                                                String remoteTaskUrl = 's3://nextflow-work/sessions/abc/ab/cdef1234/') {
        return new S5cmdFileCopyStrategy(
                configWith(publishPaths),
                tempDir,         // workDir (unused for these tests)
                tempDir,         // targetDir (unused)
                'copy',          // stageInMode
                'copy',          // stageOutMode
                remoteTaskUrl)
    }

    // ── stage-in (existing behaviour, locked-down) ────────────────────────

    def 'getStageInputFilesScript emits one s5cmd cp per input, into per-task remote inputs/'() {
        given:
        def s = makeStrategy([])    // paths empty — stage-in still works (different code path)
        def file = tempDir.resolve('reads.fq')
        Files.writeString(file, 'data\n')

        when:
        String script = s.getStageInputFilesScript(['reads.fq': file])

        then:
        script.contains('s5cmd')
        script.contains("'s3://nextflow-work/sessions/abc/ab/cdef1234/inputs/reads.fq'")
        script.contains("'./reads.fq'")
    }

    def 'getStageInputFilesScript uses recursive form for directory inputs'() {
        given:
        def s = makeStrategy([])
        def dir = tempDir.resolve('index')
        Files.createDirectory(dir)

        when:
        String script = s.getStageInputFilesScript(['index': dir])

        then:
        // recursive form: src ends with /*  and dst ends with /
        script.contains("'s3://nextflow-work/sessions/abc/ab/cdef1234/inputs/index/*'")
        script.contains("'./index/'")
    }

    // ── publishDir via s5cmd (opt-in via publishPaths) ────────────────────

    def 'copyFile defers to default strategy when publishPaths is empty (default — Nextflow handles it)'() {
        given: 'publishPaths NOT set — even an s3:// target falls through'
        def s = makeStrategy([])
        URI uri = URI.create('s3://my-results/run42/multiqc.html')
        Path target = Spy(Path) {
            toUri() >> uri
        }

        when:
        String cmd = s.copyFile('/local/work/multiqc.html', target)

        then: 'no s5cmd — Nextflow native nf-amazon handles publishDir'
        !cmd.contains('s5cmd')
    }

    def 'copyFile emits s5cmd cp when publishPaths matches the target'() {
        given:
        def s = makeStrategy(['s3://my-results/'])    // explicit opt-in
        URI uri = URI.create('s3://my-results/run42/multiqc.html')
        Path target = Spy(Path) {
            toUri() >> uri
        }

        when:
        String cmd = s.copyFile('/local/work/multiqc.html', target)

        then:
        cmd.contains('s5cmd')
        cmd.contains('cp')
        cmd.contains("'s3://my-results/run42/multiqc.html'")
        cmd.contains("'/local/work/multiqc.html'")
    }

    def 'publishPaths and paths are independent — input-staging prefix does NOT enable publishDir interception'() {
        given: 'paths (input staging) is set, but publishPaths is empty'
        def cfg = configWith([], ['s3://my-inputs/'])
        def s = new S5cmdFileCopyStrategy(
                cfg, tempDir, tempDir, 'copy', 'copy',
                's3://nextflow-work/sessions/abc/ab/cdef1234/')
        URI uri = URI.create('s3://my-inputs/results/output.txt')
        Path target = Spy(Path) { toUri() >> uri }

        when:
        String cmd = s.copyFile('/local/output.txt', target)

        then: 'no s5cmd — paths governs input, not publish'
        !cmd.contains('s5cmd')
    }

    def 'copyFile falls back when target s3:// does not match publishPaths'() {
        given:
        def s = makeStrategy(['s3://my-results/'])    // only my-results/ is matched
        URI uri = URI.create('s3://other-bucket/elsewhere/data.bam')
        Path target = Spy(Path) {
            toUri() >> uri
        }

        when:
        String cmd = s.copyFile('/local/work/data.bam', target)

        then:
        !cmd.contains('s5cmd')
    }

    def 'copyFile falls back for non-s3 targets even when publishPaths is set'() {
        given:
        def s = makeStrategy(['s3://my-results/'])
        Path local = tempDir.resolve('local-publish/output.txt')

        when:
        String cmd = s.copyFile('/local/work/output.txt', local)

        then:
        !cmd.contains('s5cmd')
    }

    def 'copyFile uses recursive form when source is a directory and publishPath matches'() {
        given:
        def s = makeStrategy(['s3://my-results/'])
        Path srcDir = tempDir.resolve('quant-output')
        Files.createDirectory(srcDir)
        URI uri = URI.create('s3://my-results/run42/quant')
        Path target = Spy(Path) {
            toUri() >> uri
        }

        when:
        String cmd = s.copyFile(srcDir.toString(), target)

        then:
        cmd.contains('s5cmd')
        cmd.contains("'${srcDir}/*'")
        cmd.contains("'s3://my-results/run42/quant/'")
    }

    // ── stage-out is delegated to bootstrap (no per-file unstage) ─────────

    def 'getUnstageOutputFilesScript returns only comments when no inputs were staged'() {
        given:
        def s = makeStrategy([])

        when: 'no getStageInputFilesScript call → nothing recorded to clean up'
        String script = s.getUnstageOutputFilesScript(['out.txt'], tempDir)

        then:
        script.contains('# nf-nomad-s5cmd:')
        script.contains('bootstrap')
        // No executable lines — only comments. (The comment text mentions
        // 's5cmd cp', so assert every non-blank line starts with '#'.)
        script.readLines().findAll { it.trim() }.every { it.trim().startsWith('#') }
    }

    // ── staged-input cleanup (cleanupLocal, default on) ───────────────────

    def 'getUnstageOutputFilesScript frees staged inputs after stage-in (inputs only)'() {
        given:
        def s = makeStrategy([])
        def f1 = tempDir.resolve('reads.fq'); Files.writeString(f1, 'x')
        def f2 = tempDir.resolve('ref.fa');   Files.writeString(f2, 'y')
        s.getStageInputFilesScript(['reads.fq': f1, 'ref.fa': f2])   // records staged names

        when:
        String script = s.getUnstageOutputFilesScript(['result.bam'], tempDir)

        then: 'both inputs removed; the output never removed'
        script.contains("rm -rf -- './reads.fq'")
        script.contains("rm -rf -- './ref.fa'")
        !script.contains("rm -rf -- './result.bam'")
    }

    def 'getUnstageOutputFilesScript keeps an input that is also a declared output (in-place)'() {
        given:
        def s = makeStrategy([])
        def f = tempDir.resolve('inplace.txt'); Files.writeString(f, 'x')
        s.getStageInputFilesScript(['inplace.txt': f])

        when: 'inplace.txt is BOTH an input and an output'
        String script = s.getUnstageOutputFilesScript(['inplace.txt'], tempDir)

        then: 'it must NOT be removed (would corrupt the output)'
        !script.contains("rm -rf -- './inplace.txt'")
    }

    def 'getUnstageOutputFilesScript covers directory inputs with rm -rf'() {
        given:
        def s = makeStrategy([])
        def dir = tempDir.resolve('index'); Files.createDirectory(dir)
        s.getStageInputFilesScript(['index': dir])

        when:
        String script = s.getUnstageOutputFilesScript([], tempDir)

        then:
        script.contains("rm -rf -- './index'")
    }

    def 'getUnstageOutputFilesScript skips cleanup when workDir.cleanupLocal = false'() {
        given:
        def cfg = S5cmdConfig.fromMap(
            s3: [endpoint: 'http://rustfs:9900', usePathStyle: true, accessKeyId: 'AK', secretAccessKey: 'SK'],
            cp: [concurrency: 4, numWorkers: 32, retryCount: 5, logLevel: 'info'],
            workDir: [cleanupLocal: false],
        )
        def s = new S5cmdFileCopyStrategy(cfg, tempDir, tempDir, 'copy', 'copy',
                's3://nextflow-work/sessions/abc/ab/cdef1234/')
        def f = tempDir.resolve('reads.fq'); Files.writeString(f, 'x')
        s.getStageInputFilesScript(['reads.fq': f])

        when:
        String script = s.getUnstageOutputFilesScript([], tempDir)

        then: 'no rm — only comments'
        !script.contains('rm -rf')
        script.readLines().findAll { it.trim() }.every { it.trim().startsWith('#') }
    }
}
