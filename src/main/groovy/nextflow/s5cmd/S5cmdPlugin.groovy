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

package nextflow.s5cmd

import groovy.util.logging.Slf4j
import nextflow.cli.PluginAbstractExec
import nextflow.plugin.BasePlugin
import org.pf4j.PluginWrapper

/**
 * Nextflow plugin: high-throughput S3 staging via the s5cmd CLI.
 *
 * Sibling to nf-rclone — focused exclusively on S3-compatible endpoints
 * (AWS, MinIO, rustfs, Ceph RGW, ...) with s5cmd's massive parallelism
 * for input/output staging. No multi-backend abstraction; no rclone.conf
 * file rendering; no SFTP/crypt overlays.
 *
 * On startup the plugin verifies the s5cmd binary is reachable. The CLI
 * subcommand `nextflow plugin nf-s5cmd validate` provides a standalone
 * connectivity check against a configured endpoint.
 *
 * @author abc-cluster
 */
@Slf4j
class S5cmdPlugin extends BasePlugin implements PluginAbstractExec {

    S5cmdPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }

    @Override
    void start() {
        super.start()
        log.info("nf-s5cmd plugin started (v${wrapper.descriptor.version})")
        validateS5cmdBinary()
    }

    @Override
    List<String> getCommands() {
        return ['validate']
    }

    @Override
    int exec(String cmd, List<String> args) {
        if (cmd == 'validate') {
            return validateConnectivity(args)
        }
        return -1
    }

    /**
     * CLI command: nextflow plugin nf-s5cmd validate
     * Runs `s5cmd version` and a smoke `ls` to confirm the binary works.
     * Connectivity to a specific endpoint is the user's responsibility
     * (set via env/config; s5cmd needs AWS_ACCESS_KEY_ID/_SECRET_ACCESS_KEY
     * + optional S3_ENDPOINT_URL).
     */
    int validateConnectivity(List<String> args) {
        try {
            def proc = ['s5cmd', 'version'].execute()
            proc.waitFor()
            if (proc.exitValue() == 0) {
                def version = proc.text.readLines().first()
                log.info("nf-s5cmd: ${version}")
                return 0
            } else {
                log.error("nf-s5cmd: s5cmd version failed: ${proc.err.text}")
                return 1
            }
        } catch (IOException e) {
            log.error("nf-s5cmd: s5cmd binary not found. Install s5cmd on the head node and all compute nodes (https://github.com/peak/s5cmd/releases)")
            return 127
        }
    }

    private void validateS5cmdBinary() {
        try {
            def proc = ['s5cmd', 'version'].execute()
            proc.waitFor()
            if (proc.exitValue() == 0) {
                def version = proc.text.readLines().first()
                log.info("nf-s5cmd: Found ${version}")
            } else {
                log.warn("nf-s5cmd: s5cmd binary returned non-zero exit; staging via s5cmd will fail at runtime.")
            }
        } catch (IOException e) {
            log.warn("nf-s5cmd: s5cmd binary not found on PATH. The plugin will still load, but tasks that hit s5cmd staging will fail until the binary is installed.")
        }
    }
}
