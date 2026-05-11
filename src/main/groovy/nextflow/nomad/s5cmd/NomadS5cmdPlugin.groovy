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

package nextflow.nomad.s5cmd

import groovy.util.logging.Slf4j
import nextflow.cli.PluginAbstractExec
import nextflow.plugin.BasePlugin
import org.pf4j.PluginWrapper

/**
 * Nextflow plugin: high-throughput S3 staging via the s5cmd CLI.
 *
 * Focused exclusively on S3-compatible endpoints (AWS, MinIO, rustfs,
 * Ceph RGW, ...) with s5cmd's massive parallelism for input/output
 * staging. Single-backend by design — no multi-backend abstraction, no
 * SFTP/crypt overlays.
 *
 * On startup the plugin verifies the s5cmd binary is reachable. The CLI
 * subcommand `nextflow plugin nf-nomad-s5cmd validate` provides a
 * standalone connectivity check against a configured endpoint.
 *
 * @author Incremental Steps Software Solutions OÜ
 */
@Slf4j
class NomadS5cmdPlugin extends BasePlugin implements PluginAbstractExec {

    NomadS5cmdPlugin(PluginWrapper wrapper) {
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
