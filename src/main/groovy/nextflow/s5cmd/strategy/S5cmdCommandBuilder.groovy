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

package nextflow.s5cmd.strategy

import groovy.transform.CompileStatic
import nextflow.s5cmd.config.S5cmdConfig
import nextflow.s5cmd.config.S5cmdCpConfig
import nextflow.s5cmd.config.S5cmdS3Config

/**
 * Pure-functional builder for the bash command line that invokes s5cmd
 * with the right global flags + cp-specific tunables. No I/O, no Nextflow
 * task state — just configuration → string. Easy to unit-test.
 *
 * Form for {@code cp}:
 * <pre>
 *   s5cmd \
 *     [--endpoint-url http://...] [--no-verify-ssl] \
 *     --log info \
 *     -r 10 -numworkers 256 \
 *     cp -c 5 [-p 50] [extra flags...] '<src>' '<dst>'
 * </pre>
 *
 * Connection params (endpoint, region, creds, path-style) are exported as
 * env vars before invocation via {@link #envExports()} so they apply to
 * every cp call within the same task without repeating flags.
 */
@CompileStatic
class S5cmdCommandBuilder {

    private final S5cmdConfig config

    S5cmdCommandBuilder(S5cmdConfig config) {
        this.config = config ?: new S5cmdConfig()
    }

    /**
     * Build a `cp` command for a single src→dst transfer. Either side
     * may be local (`/path/...`) or S3 (`s3://bucket/key`).
     *
     * @param uploadDirection true when the dst is S3 (i.e. an upload —
     *                        uses uploadConcurrency); false otherwise.
     */
    String buildCopy(String src, String dst, boolean uploadDirection = false) {
        return "${global()} cp ${perCopy(uploadDirection)} ${quote(src)} ${quote(dst)}"
    }

    /**
     * Build a `cp` command that transfers a directory recursively. s5cmd
     * uses glob form (e.g. `s3://bucket/prefix/*`) for prefix transfers.
     */
    String buildCopyDir(String srcDir, String dstDir, boolean uploadDirection = false) {
        String src = srcDir.endsWith('/') ? "${srcDir}*" : "${srcDir}/*"
        String dst = dstDir.endsWith('/') ? dstDir : "${dstDir}/"
        return "${global()} cp ${perCopy(uploadDirection)} ${quote(src)} ${quote(dst)}"
    }

    /**
     * Bash snippet that exports AWS_* / S3_* env vars from the config.
     * Emitted ONCE per task before any cp call. Fields left null on the
     * config skip their export, so an existing env var (from the docker
     * image, Nomad template, etc.) wins.
     */
    String envExports() {
        S5cmdS3Config s = config.s3
        List<String> lines = []
        if( s.profile )         lines.add(("export AWS_PROFILE='" + shellEscape(s.profile) + "'") as String)
        if( s.accessKeyId )     lines.add(("export AWS_ACCESS_KEY_ID='" + shellEscape(s.accessKeyId) + "'") as String)
        if( s.secretAccessKey ) lines.add(("export AWS_SECRET_ACCESS_KEY='" + shellEscape(s.secretAccessKey) + "'") as String)
        if( s.region )          lines.add(("export AWS_REGION='" + shellEscape(s.region) + "'") as String)
        if( s.endpoint )        lines.add(("export S3_ENDPOINT_URL='" + shellEscape(s.endpoint) + "'") as String)
        if( s.usePathStyle )    lines.add('export S3_USE_PATH_STYLE=true')
        return lines.join('\n')
    }

    // ── internals ─────────────────────────────────────────────────────────

    /** Global flags: binary path + endpoint + tls + log + retries + workers. */
    private String global() {
        S5cmdCpConfig cp = config.cp
        S5cmdS3Config s = config.s3
        List<String> parts = [config.binary]

        // Endpoint can also come from S3_ENDPOINT_URL env (set in envExports).
        // Including it on the command line is more visible in .command.run logs.
        if( s.endpoint )       parts.add("--endpoint-url " + s.endpoint)
        if( !s.useTLS )        parts.add("--no-verify-ssl")
        if( cp.logLevel )      parts.add("--log " + cp.logLevel)
        if( cp.retryCount > 0 ) parts.add("-r " + cp.retryCount)
        if( cp.numWorkers > 0 ) parts.add("-numworkers " + cp.numWorkers)
        return parts.join(' ')
    }

    /** Per-cp flags: concurrency + part-size + extras. */
    private String perCopy(boolean uploadDirection) {
        S5cmdCpConfig cp = config.cp
        List<String> parts = []
        int c = uploadDirection ? cp.effectiveUploadConcurrency() : cp.concurrency
        if( c > 0 ) parts.add("-c " + c)
        if( cp.partSize > 0 ) parts.add("-p " + cp.partSize)
        if( cp.extraFlags ) parts.addAll(cp.extraFlags)
        return parts.join(' ')
    }

    private static String quote(String s) {
        return "'" + s.replace("'", "'\\''") + "'"
    }

    private static String shellEscape(String s) {
        return s.replace("'", "'\\''")
    }
}
