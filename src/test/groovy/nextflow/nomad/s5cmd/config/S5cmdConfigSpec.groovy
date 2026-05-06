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

package nextflow.nomad.s5cmd.config

import spock.lang.Specification

class S5cmdConfigSpec extends Specification {

    def 'fromMap returns defaults when given an empty map'() {
        when:
        def cfg = S5cmdConfig.fromMap([:])

        then:
        cfg.enabled
        cfg.binary == 's5cmd'
        cfg.paths.isEmpty()
        cfg.s3.region == 'us-east-1'
        !cfg.s3.usePathStyle
        cfg.s3.useTLS
        cfg.cp.concurrency == 5
        cfg.cp.numWorkers == 256
        cfg.cp.retryCount == 10
        cfg.cp.partSize == 0
    }

    def 'fromMap parses a full nested s5cmd { s3 {} cp {} } block'() {
        given:
        Map raw = [
            enabled: false,
            binary : '/usr/local/bin/s5cmd',
            paths  : ['s3://my-bucket/', 's3://shared/inputs/'],
            s3     : [
                endpoint       : 'http://rustfs.aither:9900',
                region         : 'eu-west-1',
                accessKeyId    : 'AK',
                secretAccessKey: 'SK',
                usePathStyle   : true,
                useTLS         : false,
                profile        : 'cluster-dev',
            ],
            cp: [
                concurrency      : 10,
                uploadConcurrency: 3,
                numWorkers       : 64,
                retryCount       : 3,
                partSize         : 50,
                logLevel         : 'DEBUG',
                extraFlags       : ['--storage-class', 'STANDARD_IA'],
            ],
        ]

        when:
        def cfg = S5cmdConfig.fromMap(raw)

        then:
        !cfg.enabled
        cfg.binary == '/usr/local/bin/s5cmd'
        cfg.paths == ['s3://my-bucket/', 's3://shared/inputs/']

        and: 's3 sub-block'
        cfg.s3.endpoint == 'http://rustfs.aither:9900'
        cfg.s3.region == 'eu-west-1'
        cfg.s3.accessKeyId == 'AK'
        cfg.s3.secretAccessKey == 'SK'
        cfg.s3.usePathStyle
        !cfg.s3.useTLS
        cfg.s3.profile == 'cluster-dev'

        and: 'cp sub-block'
        cfg.cp.concurrency == 10
        cfg.cp.uploadConcurrency == 3
        cfg.cp.effectiveUploadConcurrency() == 3
        cfg.cp.numWorkers == 64
        cfg.cp.retryCount == 3
        cfg.cp.partSize == 50
        cfg.cp.logLevel == 'debug'
        cfg.cp.extraFlags == ['--storage-class', 'STANDARD_IA']
    }

    def 'paths accepts a scalar single string for ergonomics'() {
        when:
        def cfg = S5cmdConfig.fromMap(paths: 's3://only-bucket/')

        then:
        cfg.paths == ['s3://only-bucket/']
    }

    def 'matches() honours s3:// prefixes'() {
        given:
        def cfg = S5cmdConfig.fromMap(paths: ['s3://hot-bucket/', 's3://shared/inputs/'])

        expect:
        cfg.matches('s3://hot-bucket/foo/bar.fq')
        cfg.matches('s3://shared/inputs/sample-A.fq')
        !cfg.matches('s3://other-bucket/x')
        !cfg.matches('/local/path/file')
        !cfg.matches('s3://shared/outputs/x')   // wrong prefix
        !cfg.matches(null)
    }

    def 'matches() returns false when plugin disabled even with matching path'() {
        given:
        def cfg = S5cmdConfig.fromMap(enabled: false, paths: ['s3://x/'])

        expect:
        !cfg.matches('s3://x/file')
    }

    def 'effectiveUploadConcurrency() defaults to download concurrency when not set'() {
        given:
        def cfg = S5cmdConfig.fromMap(cp: [concurrency: 7])

        expect:
        cfg.cp.uploadConcurrency == -1     // sentinel
        cfg.cp.effectiveUploadConcurrency() == 7
    }

    def 'validate() rejects an endpoint without scheme'() {
        when:
        S5cmdConfig.fromMap(s3: [endpoint: 'rustfs.aither:9900']).validate()

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('must include scheme')
    }

    def 'validate() rejects a path entry that is not s3:// or s3a://'() {
        when:
        S5cmdConfig.fromMap(paths: ['s3://ok/', '/local/oops']).validate()

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('must begin with s3:// or s3a://')
    }

    def 'validate() accepts a well-formed config'() {
        when:
        S5cmdConfig.fromMap(
            paths: ['s3://nextflow-work/', 's3a://other/'],
            s3: [endpoint: 'http://rustfs.aither:9900', usePathStyle: true],
            cp: [concurrency: 4, numWorkers: 32, retryCount: 5, partSize: 16],
        ).validate()

        then:
        noExceptionThrown()
    }

    def 'validate() rejects bad cp.concurrency / numWorkers / partSize values'() {
        when:
        S5cmdConfig.fromMap(cp: [concurrency: 0]).validate()

        then:
        thrown(IllegalArgumentException)

        when:
        S5cmdConfig.fromMap(cp: [numWorkers: -1]).validate()

        then:
        thrown(IllegalArgumentException)

        when:
        S5cmdConfig.fromMap(cp: [partSize: -5]).validate()

        then:
        thrown(IllegalArgumentException)
    }
}
