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
 * Tunables for `s5cmd cp`. Each field maps directly to an s5cmd flag —
 * see https://github.com/peak/s5cmd?tab=readme-ov-file#flags.
 *
 * Naming follows s5cmd's own conventions (concurrency = -c, partSize = -p,
 * numWorkers = -numworkers) so debugging from logs back to flags is direct.
 *
 * <pre>
 * cp {{
 *   concurrency       = 5      // -c   parts per object (download)
 *   uploadConcurrency = 5      // -c   override for uploads (defaults to concurrency)
 *   numWorkers        = 256    // -numworkers global pool
 *   retryCount        = 10     // -r
 *   partSize          = 50     // -p   in MiB; 0 = s5cmd default
 *   logLevel          = 'info' // --log (trace|debug|info|error; always lowercase)
 *   extraFlags        = ['--dry-run']
 * }}
 * </pre>
 */
@Slf4j
@CompileStatic
class S5cmdCpConfig {

    private static final Set<String> KNOWN_KEYS =
        ['concurrency','uploadConcurrency','numWorkers','retryCount','partSize','logLevel','extraFlags'] as Set

    /** Per-object download parallelism (s5cmd cp -c). Default 5. */
    int concurrency = 5

    /**
     * Per-object upload parallelism. Defaults to {@link #concurrency} —
     * override when downloads and uploads should differ (e.g. asymmetric
     * bandwidth). Maps to s5cmd cp -c on the upload command line.
     */
    int uploadConcurrency = -1

    /** Global worker pool size (s5cmd -numworkers). Default 256. */
    int numWorkers = 256

    /** Number of retries per transfer (s5cmd -r). Default 10. */
    int retryCount = 10

    /**
     * Multipart chunk size in MiB (s5cmd cp -p). 0 (default) lets s5cmd
     * pick. Larger sizes = fewer parts but more memory per worker.
     */
    int partSize = 0

    /** Log level: trace|debug|info|warn|error. Maps to s5cmd --log. Default info (lowercase). */
    String logLevel = 'info'

    /**
     * Extra raw flags appended verbatim to every `s5cmd cp` invocation.
     * Use for one-off knobs not modeled here (e.g. --storage-class STANDARD_IA,
     * --acl bucket-owner-full-control, --request-payer).
     */
    List<String> extraFlags = []

    /** Effective upload concurrency: explicit override OR concurrency. */
    int effectiveUploadConcurrency() {
        return uploadConcurrency > 0 ? uploadConcurrency : concurrency
    }

    static S5cmdCpConfig fromMap(Map<String, Object> map) {
        def cfg = new S5cmdCpConfig()
        if( map == null ) return cfg

        if( map.containsKey('concurrency') )       cfg.concurrency = ((Number) map.concurrency).intValue()
        if( map.containsKey('uploadConcurrency') ) cfg.uploadConcurrency = ((Number) map.uploadConcurrency).intValue()
        if( map.containsKey('numWorkers') )        cfg.numWorkers = ((Number) map.numWorkers).intValue()
        if( map.containsKey('retryCount') )        cfg.retryCount = ((Number) map.retryCount).intValue()
        if( map.containsKey('partSize') )          cfg.partSize = ((Number) map.partSize).intValue()
        if( map.containsKey('logLevel') )          cfg.logLevel = ((String) map.logLevel).toLowerCase()
        if( map.containsKey('extraFlags') && map.extraFlags instanceof List )
            cfg.extraFlags = ((List) map.extraFlags).collect { it?.toString() }.findAll { it } as List<String>

        def unknown = map.keySet().findAll { !(KNOWN_KEYS.contains((String) it)) }
        if( unknown ) {
            log.warn("nf-s5cmd: unknown cp{} key(s): ${unknown} — supported: ${KNOWN_KEYS.toList().sort()}")
        }
        return cfg
    }

    void validate() {
        if( concurrency <= 0 ) {
            throw new IllegalArgumentException("nf-s5cmd: cp.concurrency must be > 0, got ${concurrency}")
        }
        if( numWorkers <= 0 ) {
            throw new IllegalArgumentException("nf-s5cmd: cp.numWorkers must be > 0, got ${numWorkers}")
        }
        if( retryCount < 0 ) {
            throw new IllegalArgumentException("nf-s5cmd: cp.retryCount must be >= 0, got ${retryCount}")
        }
        if( partSize < 0 ) {
            throw new IllegalArgumentException("nf-s5cmd: cp.partSize must be >= 0 (0 = s5cmd default), got ${partSize}")
        }
    }
}
