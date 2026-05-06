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

class S5cmdConfigLocatorSpec extends Specification {

    def setup() {
        S5cmdConfigLocator.resetForTests()
    }

    def 'locate prefers nomad.s5cmd over top-level s5cmd'() {
        given:
        Map<String, Object> sessionConfig = [
                nomad : [s5cmd: [enabled: true, binary: 'new']],
                s5cmd : [enabled: true, binary: 'old'],
        ]

        when:
        Map result = S5cmdConfigLocator.locate(sessionConfig)

        then:
        result == [enabled: true, binary: 'new']
    }

    def 'locate falls back to top-level s5cmd when nomad.s5cmd is absent'() {
        given:
        Map<String, Object> sessionConfig = [
                s5cmd: [enabled: true, paths: ['s3://x']],
        ]

        when:
        Map result = S5cmdConfigLocator.locate(sessionConfig)

        then:
        result == [enabled: true, paths: ['s3://x']]
    }

    def 'locate returns null when neither scope is present'() {
        expect:
        S5cmdConfigLocator.locate([:]) == null
        S5cmdConfigLocator.locate(null) == null
    }

    def 'locate returns null when scope is non-Map'() {
        expect:
        S5cmdConfigLocator.locate([nomad: [s5cmd: 'string-not-map']]) == null
        S5cmdConfigLocator.locate([s5cmd: 42]) == null
    }

    def 'nomad scope without s5cmd nested still falls through to legacy'() {
        given:
        Map<String, Object> sessionConfig = [
                nomad : [client: [address: 'http://nomad:4646']],
                s5cmd : [enabled: true],
        ]

        when:
        Map result = S5cmdConfigLocator.locate(sessionConfig)

        then:
        result == [enabled: true]
    }
}
