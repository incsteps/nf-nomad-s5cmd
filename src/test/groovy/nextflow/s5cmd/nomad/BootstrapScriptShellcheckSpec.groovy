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

import nextflow.processor.TaskRun
import spock.lang.IgnoreIf
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

/**
 * Runs shellcheck against the rendered worker bootstrap script. Catches
 * the class of bug we hit in v1: writing {@code $$(date)} thinking the
 * Groovy {@code $$} would Nomad-escape to {@code $}, when in fact Nomad
 * only escapes {@code $${...}} and bash interpreted {@code $$} as PID.
 *
 * Skipped when shellcheck isn't on PATH (CI sets it up; local devs may not
 * have it installed).
 */
@IgnoreIf({ !shellcheckAvailable() })
class BootstrapScriptShellcheckSpec extends Specification {

    @TempDir
    Path tempDir

    static boolean shellcheckAvailable() {
        try {
            def proc = ['shellcheck', '--version'].execute()
            proc.waitFor()
            return proc.exitValue() == 0
        } catch (Throwable ignored) {
            return false
        }
    }

    def 'rendered bootstrap script passes shellcheck (no errors)'() {
        given: 'a real S5cmdNomadInterop in a session that exercises bin/ + inputs paths'
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = sessionDir.resolve('ab').resolve('cdef1234')
        Files.createDirectories(workDir)
        def task = Mock(TaskRun) { getWorkDir() >> workDir }
        def session = [
            s5cmd: [
                enabled: true,
                binary : '/nxf-work/bin/s5cmd',
                s3     : [endpoint: 'http://rustfs:9900', usePathStyle: true,
                          accessKeyId: 'AK', secretAccessKey: 'SK'],
                cp     : [concurrency: 4, numWorkers: 32, retryCount: 5, logLevel: 'info'],
                workDir: [enabled: true, bucket: 's3://nextflow-work', prefix: 'sessions/abc'],
            ]
        ]
        def interop = new S5cmdNomadInterop(task, session, sessionDir)

        and: 'the rendered script with Nomad-escape applied (so what bash actually sees)'
        String script = interop.bootstrapScript()
        // Apply Nomad's HCL escape rule: $$ becomes $ ONLY when followed by {.
        // This mimics what Nomad's job-spec parser does to args before they
        // reach the docker driver.
        String bashView = applyNomadEscape(script)

        and: 'a temp file shellcheck can read'
        Path scriptFile = tempDir.resolve('bootstrap.sh')
        Files.writeString(scriptFile, '#!/bin/bash\n' + bashView)

        when: 'shellcheck runs against it'
        def errors = runShellcheck(scriptFile)

        then:
        errors.isEmpty() ?: throwWithDetails(errors, bashView)
    }

    /**
     * Apply Nomad's HCL interpolation escape: `$$` → `$` ONLY when the next
     * char is `{`. Bare `$$` (e.g. `$$PATH`, `$$(cmd)`) is left as-is. This
     * mirrors how Nomad processes the args field of a docker task.
     */
    static String applyNomadEscape(String script) {
        StringBuilder sb = new StringBuilder(script.length())
        int i = 0
        while( i < script.length() ) {
            if( i + 2 < script.length()
                && script.charAt(i) == ('$' as char)
                && script.charAt(i + 1) == ('$' as char)
                && script.charAt(i + 2) == ('{' as char) ) {
                sb.append('$')        // $${ → ${
                i += 2
            } else {
                sb.append(script.charAt(i))
                i++
            }
        }
        return sb.toString()
    }

    /**
     * Run shellcheck on the script. Returns a list of error strings; empty
     * means clean. We're intentionally strict: any error fails the test.
     * Specific noisy warnings (e.g. SC2086 on s5cmd globs we know are safe)
     * can be excluded via -e.
     */
    static List<String> runShellcheck(Path script) {
        // -S error  → only treat error-level findings as failures (warnings/infos still printed)
        // -e SC2086 → unquoted variables in our globs are intentional (s5cmd glob args)
        // -e SC2155 → declare-and-assign in `abs="$(cmd)"` is fine; cmd subst doesn't fail loudly anyway
        def cmd = ['shellcheck', '-S', 'error', '-e', 'SC2086,SC2155', script.toString()]
        def proc = cmd.execute()
        StringBuilder out = new StringBuilder()
        StringBuilder err = new StringBuilder()
        proc.consumeProcessOutput(out, err)
        proc.waitFor()
        if( proc.exitValue() == 0 ) return []
        // shellcheck returns 1 when issues found; collect the report
        String report = out.toString() + err.toString()
        return report.readLines().findAll { it.trim() }
    }

    static boolean throwWithDetails(List<String> errors, String renderedScript) {
        def msg = new StringBuilder()
        msg.append("shellcheck reported ${errors.size()} line(s) of issues:\n")
        errors.each { msg.append("  ").append(it).append('\n') }
        msg.append("\n--- rendered script (Nomad-escape applied, what bash sees) ---\n")
        msg.append(renderedScript)
        throw new AssertionError(msg.toString())
    }
}
