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
package nextflow.s5cmd.config

import nextflow.util.Duration
import spock.lang.Specification
import spock.lang.Unroll

class S5cmdWorkDirConfigSpec extends Specification {

    def 'defaults: disabled, no bucket, 60s timeout'() {
        when:
        def cfg = S5cmdWorkDirConfig.fromMap([:])

        then:
        !cfg.enabled
        cfg.bucket == null
        cfg.prefix == null
        cfg.completionTimeout == Duration.of('60s')
    }

    def 'fromMap with null returns a default-config instance'() {
        expect:
        !S5cmdWorkDirConfig.fromMap(null).enabled
    }

    def 'fromMap parses every key + accepts duration as either Duration or string'() {
        when:
        def cfg = S5cmdWorkDirConfig.fromMap(
            enabled          : true,
            bucket           : 's3://nextflow-work',
            prefix           : 'sessions/abc/',
            completionTimeout: '5m',
        )

        then:
        cfg.enabled
        cfg.bucket == 's3://nextflow-work'
        cfg.prefix == 'sessions/abc/'
        cfg.completionTimeout == Duration.of('5m')
    }

    def 'fromMap trims surrounding whitespace from bucket/prefix and treats empty as null'() {
        when:
        def cfg = S5cmdWorkDirConfig.fromMap(bucket: '  s3://b  ', prefix: '   ')

        then:
        cfg.bucket == 's3://b'
        cfg.prefix == null
    }

    @Unroll
    def 'rootUrl composition: bucket=#bucket prefix=#prefix → #expected'() {
        given:
        def cfg = new S5cmdWorkDirConfig(bucket: bucket, prefix: prefix)

        expect:
        cfg.rootUrl() == expected

        where:
        bucket                      | prefix                | expected
        's3://b'                    | null                  | 's3://b/'
        's3://b'                    | ''                    | 's3://b/'
        's3://b/'                   | null                  | 's3://b/'
        's3://b'                    | 'sessions/abc'        | 's3://b/sessions/abc/'
        's3://b/'                   | 'sessions/abc/'       | 's3://b/sessions/abc/'
        's3://b'                    | '/sessions/abc'       | 's3://b/sessions/abc/'
        's3://nextflow-work/runs/'  | 'r-2026-05-01/'       | 's3://nextflow-work/runs/r-2026-05-01/'
    }

    def 'rootUrl returns null when bucket is unset'() {
        expect:
        new S5cmdWorkDirConfig().rootUrl() == null
    }

    def 'validate is a no-op when disabled'() {
        when:
        new S5cmdWorkDirConfig(enabled: false).validate()

        then:
        noExceptionThrown()
    }

    def 'validate requires a bucket when enabled'() {
        when:
        new S5cmdWorkDirConfig(enabled: true).validate()

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('workDir.bucket')
    }

    def 'validate rejects a non-S3 bucket URL'() {
        when:
        new S5cmdWorkDirConfig(enabled: true, bucket: '/local/path').validate()

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('s3://')
    }

    def 'validate accepts both s3:// and s3a:// schemes'() {
        when:
        new S5cmdWorkDirConfig(enabled: true, bucket: scheme + 'b').validate()

        then:
        noExceptionThrown()

        where:
        scheme << ['s3://', 's3a://']
    }
}
