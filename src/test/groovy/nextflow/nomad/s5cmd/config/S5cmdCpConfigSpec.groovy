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
import spock.lang.Unroll

class S5cmdCpConfigSpec extends Specification {

    def 'defaults track s5cmd / Nextflow conventions'() {
        when:
        def cfg = S5cmdCpConfig.fromMap([:])

        then:
        cfg.concurrency == 5
        cfg.uploadConcurrency == -1               // sentinel meaning "fall back to concurrency"
        cfg.numWorkers == 256
        cfg.retryCount == 10
        cfg.partSize == 0                          // 0 = let s5cmd pick
        cfg.logLevel == 'INFO'
        cfg.extraFlags == []
    }

    def 'fromMap with null returns a default-config instance'() {
        expect:
        S5cmdCpConfig.fromMap(null).concurrency == 5
    }

    def 'fromMap parses every numeric + scalar key (logLevel is lowercased)'() {
        when:
        def cfg = S5cmdCpConfig.fromMap(
            concurrency      : 8,
            uploadConcurrency: 12,
            numWorkers       : 64,
            retryCount       : 3,
            partSize         : 32,
            logLevel         : 'DEBUG',
            extraFlags       : ['--storage-class', 'STANDARD_IA'],
        )

        then:
        cfg.concurrency == 8
        cfg.uploadConcurrency == 12
        cfg.numWorkers == 64
        cfg.retryCount == 3
        cfg.partSize == 32
        cfg.logLevel == 'debug'
        cfg.extraFlags == ['--storage-class', 'STANDARD_IA']
    }

    def 'extraFlags drops null/empty entries'() {
        when:
        def cfg = S5cmdCpConfig.fromMap(extraFlags: ['--keep', null, '', '--also-keep'])

        then:
        cfg.extraFlags == ['--keep', '--also-keep']
    }

    @Unroll
    def 'effectiveUploadConcurrency: download=#download upload=#upload → #expected'() {
        given:
        def cfg = new S5cmdCpConfig(concurrency: download, uploadConcurrency: upload)

        expect:
        cfg.effectiveUploadConcurrency() == expected

        where:
        download | upload | expected
        5        | -1     | 5            // sentinel → fall back
        5        | 0      | 5            // 0 also treated as "unset"
        5        | 12     | 12           // explicit override
        7        | 7      | 7            // explicit equal value
    }

    @Unroll
    def 'validate rejects invalid #field=#value'() {
        when:
        def cfg = new S5cmdCpConfig()
        cfg[field] = value
        cfg.validate()

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains(field)

        where:
        field         | value
        'concurrency' | 0
        'concurrency' | -1
        'numWorkers'  | 0
        'numWorkers'  | -3
        'retryCount'  | -1
        'partSize'    | -2
    }

    def 'validate accepts a fully-defaulted config'() {
        when:
        new S5cmdCpConfig().validate()

        then:
        noExceptionThrown()
    }
}
