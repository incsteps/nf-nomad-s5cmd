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

import spock.lang.Specification

class S5cmdS3ConfigSpec extends Specification {

    def 'fromMap defaults: only region is preset; everything else is null/false'() {
        when:
        def cfg = S5cmdS3Config.fromMap([:])

        then:
        cfg.region == 'us-east-1'
        cfg.endpoint == null
        cfg.accessKeyId == null
        cfg.secretAccessKey == null
        cfg.profile == null
        !cfg.usePathStyle
        cfg.useTLS                   // default true (most providers want TLS verification on)
    }

    def 'fromMap returns defaults when given a null map'() {
        when:
        def cfg = S5cmdS3Config.fromMap(null)

        then:
        cfg.region == 'us-east-1'
    }

    def 'fromMap parses every supported key'() {
        when:
        def cfg = S5cmdS3Config.fromMap(
            endpoint       : 'https://s3.us-west-2.amazonaws.com',
            region         : 'us-west-2',
            accessKeyId    : 'AKIA...',
            secretAccessKey: 'secret',
            usePathStyle   : true,
            useTLS         : false,
            profile        : 'work',
        )

        then:
        cfg.endpoint == 'https://s3.us-west-2.amazonaws.com'
        cfg.region == 'us-west-2'
        cfg.accessKeyId == 'AKIA...'
        cfg.secretAccessKey == 'secret'
        cfg.usePathStyle
        !cfg.useTLS
        cfg.profile == 'work'
    }

    def 'validate rejects an endpoint missing scheme'() {
        when:
        S5cmdS3Config.fromMap(endpoint: 'rustfs.aither:9900').validate()

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('must include scheme')
        e.message.contains('rustfs.aither:9900')
    }

    def 'validate accepts an http:// endpoint'() {
        when:
        S5cmdS3Config.fromMap(endpoint: 'http://localhost:9000').validate()

        then:
        noExceptionThrown()
    }

    def 'validate accepts an https:// endpoint'() {
        when:
        S5cmdS3Config.fromMap(endpoint: 'https://s3.us-west-2.amazonaws.com').validate()

        then:
        noExceptionThrown()
    }

    def 'validate is a no-op when endpoint is unset (relies on AWS default chain)'() {
        when:
        S5cmdS3Config.fromMap([:]).validate()

        then:
        noExceptionThrown()
    }

    def 'validate is happy when profile + key/secret coexist (warning only, profile wins at runtime)'() {
        when:
        S5cmdS3Config.fromMap(profile: 'work', accessKeyId: 'AK', secretAccessKey: 'SK').validate()

        then:
        noExceptionThrown()
    }
}
