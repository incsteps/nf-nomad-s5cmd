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

/**
 * Head-side s5cmd calls run with the credential exports prepended. A failure
 * used to echo the whole cmdline into the exception, which put
 * AWS_SECRET_ACCESS_KEY into Nextflow's error output and the head log.
 */
class S5cmdSecretRedactionSpec extends Specification {

    @TempDir
    Path tempDir

    private S5cmdNomadInterop interop(String secret) {
        Path sessionDir = tempDir.resolve('sess')
        Path workDir = sessionDir.resolve('ab').resolve('cdef1234')
        Files.createDirectories(workDir)
        def task = Mock(TaskRun) { getWorkDir() >> workDir }
        def session = [s5cmd: [
            enabled: true, binary: '/nxf-work/bin/s5cmd',
            s3     : [endpoint: 'http://rustfs:9900', accessKeyId: 'AKID-visible', secretAccessKey: secret],
            workDir: [enabled: true, bucket: 's3://nextflow-work', prefix: 'sessions/abc'],
        ]]
        return new S5cmdNomadInterop(task, session, sessionDir)
    }

    def 'a failed command does not echo the secret into the exception'() {
        given:
        def i = interop(secret)

        when: 'a command that fails runs through the real export-prefixing path'
        i.runShell(i.prefixWithEnvExports('false'))

        then:
        def e = thrown(ProcessSubmitException)
        !e.message.contains(secret)
        !e.message.contains(secretFragment)
        e.message.contains('AWS_SECRET_ACCESS_KEY=<redacted>')
        // The key id stays: it says which credential failed, and is not secret.
        e.message.contains('AKID-visible')

        where:
        secret                              | secretFragment
        'sk-0123456789abcdef'               | 'sk-0123'
        "it's-a-secret-with-a-quote"        | "a-secret-with"
    }

    def 'redaction leaves everything else intact'() {
        expect:
        S5cmdNomadInterop.redactSecrets("""export AWS_ACCESS_KEY_ID='AK'
export AWS_SECRET_ACCESS_KEY='SK'
export AWS_SESSION_TOKEN='TOK'
/nxf-work/bin/s5cmd cp a s3://b/c""") == """export AWS_ACCESS_KEY_ID='AK'
export AWS_SECRET_ACCESS_KEY=<redacted>
export AWS_SESSION_TOKEN=<redacted>
/nxf-work/bin/s5cmd cp a s3://b/c"""
    }
}
