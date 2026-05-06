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
 * Typed view of the {@code s5cmd { ... }} block in nextflow.config.
 *
 * The config is intentionally narrowly scoped: s5cmd only talks to S3
 * (and S3-API-compatible endpoints — MinIO, rustfs, Ceph RGW, Wasabi).
 * No SFTP, no encryption, no path-mapping-as-pseudo-remote. Anything
 * outside that scope is out of scope for this plugin.
 *
 * <pre>
 * s5cmd {
 *   enabled = true
 *   binary  = '/usr/local/bin/s5cmd'
 *
 *   // S3 prefixes nf-s5cmd will service. Anything not matching a prefix
 *   // here falls through to Nextflow's default staging.
 *   paths = ['s3://my-bucket/', 's3://other-bucket/inputs/']
 *
 *   // Connection (every field except endpoint is optional and falls
 *   // back to AWS_* env vars + ~/.aws/credentials when omitted).
 *   s3 {
 *     endpoint        = 'http://rustfs.aither:9900'
 *     region          = 'us-east-1'
 *     accessKeyId     = '${env.AWS_ACCESS_KEY_ID}'
 *     secretAccessKey = '${env.AWS_SECRET_ACCESS_KEY}'
 *     usePathStyle    = true              // required for MinIO/rustfs
 *     useTLS          = false             // skip TLS verification (dev only)
 *     profile         = null              // ~/.aws/credentials profile name
 *   }
 *
 *   // Transfer tunables (per `s5cmd cp`).
 *   cp {
 *     concurrency      = 5                // -c   parts per object
 *     numWorkers       = 256              // -numworkers global pool
 *     retryCount       = 10               // -r
 *     partSize         = 50               // -p   in MiB; 0 = s5cmd default
 *     uploadConcurrency = 5               // upload-only -c override
 *     logLevel         = 'INFO'           // --log
 *     extraFlags       = []               // raw flags appended verbatim
 *   }
 * }
 * </pre>
 */
@Slf4j
@CompileStatic
class S5cmdConfig {

    private static final Set<String> KNOWN_KEYS = ['enabled','binary','paths','publishPaths','s3','cp','workDir'] as Set

    /** Master switch — when false, the plugin loads but stays inactive. */
    boolean enabled = true

    /** Absolute path to the s5cmd binary. Default: PATH lookup. */
    String binary = 's5cmd'

    /**
     * Ordered list of S3 URL prefixes nf-s5cmd handles for INPUT staging.
     * A path matches when it startsWith() any entry. Defaults to empty
     * (plugin loads but matches nothing — useful for opt-in pipelines).
     */
    List<String> paths = []

    /**
     * Ordered list of S3 URL prefixes nf-s5cmd handles for OUTPUT publishing
     * via {@code publishDir}. Separate from {@link #paths} on purpose:
     * input staging and output publishing are independent decisions, and
     * intercepting publishDir means co-existing with Nextflow's nf-amazon
     * S3 client (which still does mkdir / list calls). Default empty —
     * Nextflow's native publishDir-via-nf-amazon handles all s3:// targets.
     *
     * Set this only when you want s5cmd's worker-pool throughput for the
     * actual file copies. The {@code aws { client { ... } }} config still
     * needs to point nf-amazon at the same S3 endpoint so its mkdir/list
     * calls succeed before our cp fires.
     */
    List<String> publishPaths = []

    /** S3 connection parameters. */
    S5cmdS3Config s3 = new S5cmdS3Config()

    /** Per-cp transfer tunables. */
    S5cmdCpConfig cp = new S5cmdCpConfig()

    /**
     * Distributed-workdir config — when {@code workDir.enabled = true}, nf-nomad
     * routes task scratch through this s5cmd backend instead of relying on a
     * shared filesystem. Discovered reflectively by nf-nomad; nf-s5cmd itself
     * does not need to know about Nomad to populate this block.
     */
    S5cmdWorkDirConfig workDir = new S5cmdWorkDirConfig()

    /**
     * Construct from the raw map Nextflow's config parser produces. Unknown
     * top-level keys are warned (helps catch typos like `transfers` vs `cp`).
     */
    static S5cmdConfig fromMap(Map<String, Object> map) {
        if( map == null ) return new S5cmdConfig()
        def cfg = new S5cmdConfig()

        if( map.containsKey('enabled') ) cfg.enabled = (boolean) map.enabled
        if( map.containsKey('binary') )  cfg.binary  = (String) map.binary

        if( map.containsKey('paths') ) {
            cfg.paths = coercePathsList(map.paths)
        }
        if( map.containsKey('publishPaths') ) {
            cfg.publishPaths = coercePathsList(map.publishPaths)
        }
        if( map.containsKey('s3') && map.s3 instanceof Map ) {
            cfg.s3 = S5cmdS3Config.fromMap((Map<String, Object>) map.s3)
        }
        if( map.containsKey('cp') && map.cp instanceof Map ) {
            cfg.cp = S5cmdCpConfig.fromMap((Map<String, Object>) map.cp)
        }
        if( map.containsKey('workDir') && map.workDir instanceof Map ) {
            cfg.workDir = S5cmdWorkDirConfig.fromMap((Map<String, Object>) map.workDir)
        }

        def unknown = map.keySet().findAll { !(KNOWN_KEYS.contains((String) it)) }
        if( unknown ) {
            log.warn("nf-s5cmd: unknown top-level config key(s): ${unknown} — supported: ${KNOWN_KEYS.toList().sort()}")
        }
        return cfg
    }

    /**
     * Validate cross-field invariants. Raises IllegalArgumentException on
     * malformed input; returns silently when ok or when {@code enabled=false}.
     */
    void validate() {
        if( !enabled ) return
        s3.validate()
        cp.validate()
        workDir.validate()
        for( String p : paths ) {
            if( !p ) continue
            if( !p.startsWith('s3://') && !p.startsWith('s3a://') ) {
                throw new IllegalArgumentException(
                    "nf-s5cmd: paths entries must begin with s3:// or s3a://; got '${p}'")
            }
        }
        for( String p : publishPaths ) {
            if( !p ) continue
            if( !p.startsWith('s3://') && !p.startsWith('s3a://') ) {
                throw new IllegalArgumentException(
                    "nf-s5cmd: publishPaths entries must begin with s3:// or s3a://; got '${p}'")
            }
        }
    }

    /**
     * Returns true when the given path is a publishDir target nf-s5cmd
     * should intercept (matches a configured {@link #publishPaths} prefix).
     * Distinct from {@link #matches} which governs input staging — output
     * publishing is opt-in via a separate prefix list.
     */
    boolean matchesPublish(String path) {
        if( !enabled || !path ) return false
        for( String prefix : publishPaths ) {
            if( prefix && path.startsWith(prefix) ) return true
        }
        return false
    }

    /**
     * Returns true when the given path begins with any configured S3 prefix
     * AND the plugin is enabled. Used by the file copy strategy to decide
     * whether to delegate to s5cmd or fall through to Nextflow defaults.
     */
    boolean matches(String path) {
        if( !enabled || !path ) return false
        for( String prefix : paths ) {
            if( prefix && path.startsWith(prefix) ) return true
        }
        return false
    }

    /** Accept either {@code paths = ['s3://x','s3://y']} or scalar {@code paths = 's3://x'}. */
    @SuppressWarnings('unchecked')
    private static List<String> coercePathsList(Object value) {
        if( value == null ) return []
        if( value instanceof List ) {
            return ((List) value).collect { it?.toString() }.findAll { it } as List<String>
        }
        return [value.toString()]
    }
}
