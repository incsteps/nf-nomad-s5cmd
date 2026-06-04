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
import nextflow.util.Duration

/**
 * Configuration for using s5cmd as the distributed-workdir backend on
 * Nomad workers (consumed by the {@code S5cmdNomadInterop} provider in
 * nf-nomad). Controls where on the S3 endpoint task scratch goes.
 *
 * <pre>
 * s5cmd {
 *   workDir {
 *     enabled    = true
 *     bucket     = 's3://nextflow-work'
 *     prefix     = 'sessions/<run-id>/'           // appended after bucket
 *     completionTimeout = '60s'                   // max wait for remote .exitcode
 *   }
 * }
 * </pre>
 */
@CompileStatic
class S5cmdWorkDirConfig {

    /** Master switch — when false, nf-nomad falls back to other providers / shared FS. */
    boolean enabled = false

    /** S3 URL of the bucket where session work-dirs live (e.g. {@code s3://nextflow-work}). */
    String bucket

    /** Optional prefix prepended to the per-task path inside the bucket. */
    String prefix

    /** How long to poll the remote {@code .exitcode} marker before giving up. */
    Duration completionTimeout = Duration.of('60s')

    /**
     * Delete staged INPUT files from the local task dir after the task finishes,
     * before the bootstrap pushes results to S3. Frees node disk immediately and
     * avoids re-uploading inputs (the head already placed them on S3). Outputs and
     * the {@code -resume} cache are never touched — only inputs that are not also
     * declared as outputs are removed. Default on.
     */
    boolean cleanupLocal = true

    /**
     * After the workflow completes, delete the per-task {@code inputs/} dirs from
     * the S3 work dir (the head-uploaded staged inputs — dead once the run ends,
     * since {@code -resume} reuses task outputs, never inputs). Reclaims S3 space.
     * Default on.
     */
    boolean cleanupRemoteInputs = true

    static S5cmdWorkDirConfig fromMap(Map<String, Object> map) {
        def cfg = new S5cmdWorkDirConfig()
        if( map == null ) return cfg
        if( map.containsKey('enabled') ) cfg.enabled = (boolean) map.enabled
        if( map.containsKey('bucket') )  cfg.bucket  = ((String) map.bucket)?.trim() ?: null
        if( map.containsKey('prefix') )  cfg.prefix  = ((String) map.prefix)?.trim() ?: null
        if( map.containsKey('cleanupLocal') ) cfg.cleanupLocal = (boolean) map.cleanupLocal
        if( map.containsKey('cleanupRemoteInputs') ) cfg.cleanupRemoteInputs = (boolean) map.cleanupRemoteInputs
        if( map.containsKey('completionTimeout') ) {
            cfg.completionTimeout = Duration.of(map.completionTimeout.toString())
        }
        return cfg
    }

    /** Validate cross-field invariants. Raises IllegalArgumentException on malformed input. */
    void validate() {
        if( !enabled ) return
        if( !bucket ) {
            throw new IllegalArgumentException(
                'nf-s5cmd: workDir.enabled=true requires workDir.bucket (e.g. s3://my-bucket)')
        }
        if( !(bucket.startsWith('s3://') || bucket.startsWith('s3a://')) ) {
            throw new IllegalArgumentException(
                "nf-s5cmd: workDir.bucket must be an s3:// URL; got '${bucket}'")
        }
    }

    /** Returns the canonical {@code s3://bucket/prefix/} root (always trailing-slashed). */
    String rootUrl() {
        if( !bucket ) return null
        String b = bucket.endsWith('/') ? bucket : (bucket + '/')
        if( !prefix ) return b
        String p = prefix.startsWith('/') ? prefix.substring(1) : prefix
        if( !p.endsWith('/') ) p = p + '/'
        return b + p
    }
}
