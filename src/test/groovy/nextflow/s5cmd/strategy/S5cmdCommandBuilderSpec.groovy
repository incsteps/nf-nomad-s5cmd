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

package nextflow.s5cmd.strategy

import nextflow.s5cmd.config.S5cmdConfig
import spock.lang.Specification

class S5cmdCommandBuilderSpec extends Specification {

    def 'buildCopy renders endpoint + log + retries + numworkers + -c with quoted args'() {
        given:
        def cfg = S5cmdConfig.fromMap(
            s3: [endpoint: 'http://rustfs.aither:9900'],
            cp: [concurrency: 4, numWorkers: 128, retryCount: 7, logLevel: 'info'])
        def builder = new S5cmdCommandBuilder(cfg)

        when:
        def cmd = builder.buildCopy('s3://b/key one.fq', '/tmp/out file.fq')

        then:
        cmd == "s5cmd --endpoint-url http://rustfs.aither:9900 --log info -r 7 -numworkers 128 cp -c 4 's3://b/key one.fq' '/tmp/out file.fq'"
    }

    def 'buildCopy emits --no-verify-ssl when useTLS=false'() {
        given:
        def cfg = S5cmdConfig.fromMap(
            s3: [endpoint: 'https://self-signed.aither', useTLS: false])
        def builder = new S5cmdCommandBuilder(cfg)

        when:
        def cmd = builder.buildCopy('a', 'b')

        then:
        cmd.contains('--no-verify-ssl')
    }

    def 'buildCopy includes -p partSize when configured'() {
        given:
        def builder = new S5cmdCommandBuilder(S5cmdConfig.fromMap(cp: [partSize: 50]))

        when:
        def cmd = builder.buildCopy('a', 'b')

        then:
        cmd.contains('cp -c 5 -p 50 ')
    }

    def 'buildCopy with uploadDirection=true uses uploadConcurrency'() {
        given:
        def cfg = S5cmdConfig.fromMap(cp: [concurrency: 5, uploadConcurrency: 12])
        def builder = new S5cmdCommandBuilder(cfg)

        when:
        def downloadCmd = builder.buildCopy('s3://b/key', '/tmp/out')          // download (default)
        def uploadCmd = builder.buildCopy('/tmp/data', 's3://b/key', true)     // upload

        then:
        downloadCmd.contains('cp -c 5 ')
        uploadCmd.contains('cp -c 12 ')
    }

    def 'buildCopyDir appends a glob and trailing slash'() {
        given:
        def builder = new S5cmdCommandBuilder(S5cmdConfig.fromMap([:]))

        when:
        def cmd = builder.buildCopyDir('s3://b/prefix', '/local/work')

        then:
        cmd.contains("'s3://b/prefix/*' '/local/work/'")
    }

    def 'extraFlags are appended verbatim to every cp call'() {
        given:
        def cfg = S5cmdConfig.fromMap(
            cp: [concurrency: 1, extraFlags: ['--storage-class', 'STANDARD_IA']])
        def builder = new S5cmdCommandBuilder(cfg)

        when:
        def cmd = builder.buildCopy('a', 'b')

        then:
        cmd.contains('cp -c 1 --storage-class STANDARD_IA')
    }

    def 'envExports omits unset s3 fields and includes path-style when true'() {
        given:
        def cfg = S5cmdConfig.fromMap(s3: [
            accessKeyId    : "AK_with_'quote'",
            secretAccessKey: 'SK',
            endpoint       : 'http://rustfs.aither:9900',
            region         : 'us-east-1',
            usePathStyle   : true,
        ])
        def builder = new S5cmdCommandBuilder(cfg)

        when:
        def script = builder.envExports()

        then:
        script.contains("export AWS_ACCESS_KEY_ID='AK_with_'\\''quote'\\''")
        script.contains("export AWS_SECRET_ACCESS_KEY='SK'")
        script.contains("export AWS_REGION='us-east-1'")
        script.contains("export S3_ENDPOINT_URL='http://rustfs.aither:9900'")
        script.contains('export S3_USE_PATH_STYLE=true')
    }

    def 'envExports prefers profile over key/secret when both are set'() {
        given:
        def cfg = S5cmdConfig.fromMap(s3: [
            profile: 'cluster-dev',
            accessKeyId: 'AK',
            secretAccessKey: 'SK',
        ])
        def builder = new S5cmdCommandBuilder(cfg)

        when:
        def script = builder.envExports()

        then:
        script.contains("export AWS_PROFILE='cluster-dev'")
        // Both AWS_ACCESS_KEY_ID and AWS_PROFILE present → AWS SDK lets profile win
        // (we still export both for transparency; that's the AWS SDK contract).
        script.contains("export AWS_ACCESS_KEY_ID='AK'")
    }

    def 'envExports skips fields the operator left null and emits only AWS_REGION default'() {
        given:
        def builder = new S5cmdCommandBuilder(S5cmdConfig.fromMap([:]))

        when:
        def script = builder.envExports()

        then:
        // Only AWS_REGION (default us-east-1) is set
        script == "export AWS_REGION='us-east-1'"
    }

    def 'single-quote escaping survives quotes inside source paths'() {
        given:
        def builder = new S5cmdCommandBuilder(S5cmdConfig.fromMap([:]))

        when:
        def cmd = builder.buildCopy("s3://b/it's-fine.txt", '/tmp/dst')

        then:
        cmd.contains("'s3://b/it'\\''s-fine.txt'")
    }
}
