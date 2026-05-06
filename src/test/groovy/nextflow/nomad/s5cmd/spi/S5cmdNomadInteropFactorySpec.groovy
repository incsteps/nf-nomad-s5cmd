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

import nextflow.nomad.s5cmd.config.S5cmdConfigLocator
import spock.lang.Specification

/**
 * Verifies the SPI factory's isEnabled gate honours both the new
 * {@code nomad.s5cmd { ... }} scope and the legacy top-level
 * {@code s5cmd { ... }} fallback.
 */
class S5cmdNomadInteropFactorySpec extends Specification {

    def setup() {
        S5cmdConfigLocator.resetForTests()
    }

    private static Map workdirEnabledScope() {
        return [enabled: true, workDir: [enabled: true, bucket: 's3://nf-work']]
    }

    def 'isEnabled returns true when nomad.s5cmd has workDir.enabled and a bucket'() {
        given:
        Map sessionConfig = [nomad: [s5cmd: workdirEnabledScope()]]

        expect:
        new S5cmdNomadInteropFactory().isEnabled(sessionConfig)
    }

    def 'isEnabled returns true via legacy top-level s5cmd fallback'() {
        given:
        Map sessionConfig = [s5cmd: workdirEnabledScope()]

        expect:
        new S5cmdNomadInteropFactory().isEnabled(sessionConfig)
    }

    def 'isEnabled prefers nomad.s5cmd over legacy when both present'() {
        given:
        // legacy says workDir disabled; new says enabled — new should win
        Map sessionConfig = [
                nomad: [s5cmd: workdirEnabledScope()],
                s5cmd: [enabled: true, workDir: [enabled: false, bucket: 's3://x']],
        ]

        expect:
        new S5cmdNomadInteropFactory().isEnabled(sessionConfig)
    }

    def 'isEnabled returns false when neither scope is present'() {
        expect:
        !new S5cmdNomadInteropFactory().isEnabled([:])
        !new S5cmdNomadInteropFactory().isEnabled([nomad: [client: [address: 'x']]])
    }

    def 'isEnabled returns false when workDir.bucket is missing'() {
        given:
        Map sessionConfig = [nomad: [s5cmd: [enabled: true, workDir: [enabled: true]]]]

        expect:
        !new S5cmdNomadInteropFactory().isEnabled(sessionConfig)
    }

    def 'isEnabled returns false when s5cmd.enabled is explicitly false'() {
        given:
        Map sessionConfig = [nomad: [s5cmd: [
                enabled: false,
                workDir: [enabled: true, bucket: 's3://nf-work'],
        ]]]

        expect:
        !new S5cmdNomadInteropFactory().isEnabled(sessionConfig)
    }
}
