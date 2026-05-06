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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Single source of truth for "where do we find the s5cmd config block in
 * the active Nextflow session config?"
 *
 * <p>Canonical scope (since the rename to {@code nf-nomad-s5cmd}):</p>
 * <pre>
 * nomad {
 *     s5cmd { ... }
 * }
 * </pre>
 *
 * <p>Legacy scope (still accepted with a deprecation warning):</p>
 * <pre>
 * s5cmd { ... }
 * </pre>
 *
 * <p>The legacy path will be removed once existing pipelines have migrated.
 * Both read sites — the trace observer and the SPI factory — go through
 * this helper so the deprecation message is logged once per scope hit, not
 * twice.</p>
 */
@Slf4j
@CompileStatic
class S5cmdConfigLocator {

    static final String SCOPE_NEW_PARENT = 'nomad'
    static final String SCOPE_KEY = 's5cmd'

    private static final Object DEPRECATION_LOCK = new Object()
    private static volatile boolean deprecationWarned = false

    /**
     * Resolve the s5cmd config map from a session config. Prefers
     * {@code nomad.s5cmd} (new) and falls back to top-level {@code s5cmd}
     * (legacy, with a one-shot deprecation warning). Returns {@code null}
     * when neither scope is present or non-Map.
     */
    static Map<String, Object> locate(Map<String, Object> sessionConfig) {
        if( sessionConfig == null ) return null

        // New canonical location: nomad.s5cmd
        final nomadScope = sessionConfig.get(SCOPE_NEW_PARENT)
        if( nomadScope instanceof Map ) {
            final nested = ((Map<String, Object>) nomadScope).get(SCOPE_KEY)
            if( nested instanceof Map ) {
                return (Map<String, Object>) nested
            }
        }

        // Legacy fallback: top-level s5cmd
        final legacy = sessionConfig.get(SCOPE_KEY)
        if( legacy instanceof Map ) {
            warnDeprecatedScopeOnce()
            return (Map<String, Object>) legacy
        }

        return null
    }

    private static void warnDeprecatedScopeOnce() {
        if( deprecationWarned ) return
        synchronized( DEPRECATION_LOCK ) {
            if( deprecationWarned ) return
            log.warn(
                'nf-nomad-s5cmd: top-level `s5cmd { ... }` scope is deprecated; ' +
                'move it under `nomad { s5cmd { ... } }`. The old scope still works ' +
                'for now but will be removed in a future release.'
            )
            deprecationWarned = true
        }
    }

    /** Test-only — reset the one-shot deprecation latch between specs. */
    static void resetForTests() {
        synchronized( DEPRECATION_LOCK ) {
            deprecationWarned = false
        }
    }
}
