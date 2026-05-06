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
 * S3 connection parameters for nf-s5cmd. Every field is optional and
 * falls back to the conventional AWS chain (env vars, ~/.aws/credentials,
 * IMDS) when omitted — meaning a pipeline running on EC2/EKS with an
 * instance-profile gets nf-s5cmd "for free" with an empty s3 {} block.
 *
 * For non-AWS endpoints (MinIO, rustfs, Ceph RGW, Wasabi) you typically
 * need: endpoint + region + accessKeyId + secretAccessKey + usePathStyle=true.
 */
@Slf4j
@CompileStatic
class S5cmdS3Config {

    private static final Set<String> KNOWN_KEYS =
        ['endpoint','region','accessKeyId','secretAccessKey','usePathStyle','useTLS','profile'] as Set

    /**
     * S3 endpoint URL. Required for non-AWS providers (MinIO, rustfs, etc).
     * Must include scheme (http:// or https://). Maps to s5cmd's
     * `--endpoint-url` flag and the S3_ENDPOINT_URL env var.
     */
    String endpoint

    /**
     * S3 region (default us-east-1). Maps to AWS_REGION. Most non-AWS
     * providers ignore region but s5cmd still requires it set.
     */
    String region = 'us-east-1'

    /** S3 access key id (AWS_ACCESS_KEY_ID). */
    String accessKeyId

    /** S3 secret access key (AWS_SECRET_ACCESS_KEY). */
    String secretAccessKey

    /**
     * Path-style addressing (bucket in URL path) instead of virtual-host
     * style. REQUIRED for MinIO, rustfs, and most self-hosted providers.
     * Maps to s5cmd's behaviour when S3_USE_PATH_STYLE is set.
     */
    boolean usePathStyle = false

    /**
     * When false, skips TLS certificate verification. Maps to s5cmd's
     * `--no-verify-ssl`. Use only for development against self-signed
     * endpoints.
     */
    boolean useTLS = true

    /**
     * Named profile from ~/.aws/credentials. Mutually exclusive with
     * accessKeyId/secretAccessKey — when set, those are ignored.
     */
    String profile

    static S5cmdS3Config fromMap(Map<String, Object> map) {
        def cfg = new S5cmdS3Config()
        if( map == null ) return cfg

        if( map.containsKey('endpoint') )        cfg.endpoint = (String) map.endpoint
        if( map.containsKey('region') )          cfg.region = (String) map.region
        if( map.containsKey('accessKeyId') )     cfg.accessKeyId = (String) map.accessKeyId
        if( map.containsKey('secretAccessKey') ) cfg.secretAccessKey = (String) map.secretAccessKey
        if( map.containsKey('usePathStyle') )    cfg.usePathStyle = (boolean) map.usePathStyle
        if( map.containsKey('useTLS') )          cfg.useTLS = (boolean) map.useTLS
        if( map.containsKey('profile') )         cfg.profile = (String) map.profile

        def unknown = map.keySet().findAll { !(KNOWN_KEYS.contains((String) it)) }
        if( unknown ) {
            log.warn("nf-s5cmd: unknown s3{} key(s): ${unknown} — supported: ${KNOWN_KEYS.toList().sort()}")
        }
        return cfg
    }

    void validate() {
        if( endpoint && !endpoint.startsWith('http://') && !endpoint.startsWith('https://') ) {
            throw new IllegalArgumentException(
                "nf-s5cmd: s3.endpoint must include scheme (http:// or https://); got '${endpoint}'")
        }
        if( profile && (accessKeyId || secretAccessKey) ) {
            log.warn("nf-s5cmd: s3.profile and s3.accessKeyId/secretAccessKey are mutually exclusive — profile will win")
        }
    }
}
