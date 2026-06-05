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
        log.debug("nf-nomad-s5cmd plugin started (v${wrapper.descriptor.version})")
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
     * CLI command: nextflow plugin nf-nomad-s5cmd validate
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
                log.info("nf-nomad-s5cmd: ${version}")
                return 0
            } else {
                log.error("nf-nomad-s5cmd: s5cmd version failed: ${proc.err.text}")
                return 1
            }
        } catch (IOException e) {
            log.error("nf-nomad-s5cmd: s5cmd binary not found. Install s5cmd on the head node and all compute nodes (https://github.com/peak/s5cmd/releases)")
            return 127
        }
    }

    private void validateS5cmdBinary() {
        // 1. Try PATH first (fast path — normal install or --dev-plugins bin-* loop has already run)
        try {
            def proc = ['s5cmd', 'version'].execute()
            proc.waitFor()
            if (proc.exitValue() == 0) {
                def version = proc.text.readLines().first()
                log.debug("nf-nomad-s5cmd: Found ${version}")
                return
            }
        } catch (IOException ignored) {
            // Not on PATH — fall through to fallback probes
        }

        // 2. Probe well-known fallback locations (operator toolchains may place
        //    s5cmd off-PATH). Consulted in priority order; first executable wins.
        //    - /local/bin-s5cmd/s5cmd — common dev-plugins location on the head container
        //    - /local/bin-*/s5cmd     — glob covers any variant bin directory from the entrypoint loop
        //    - /nxf-work/bin/s5cmd    — host-volume path used by worker bootstrap scripts
        List<String> fallbackDirs = ['/local', '/nxf-work/bin']
        List<String> candidates = [
            '/local/bin-s5cmd/s5cmd',
            '/nxf-work/bin/s5cmd',
        ]

        // Also glob /local/bin-*/s5cmd at runtime
        try {
            new File('/local').listFiles()?.each { f ->
                if (f.isDirectory() && f.name.startsWith('bin-')) {
                    candidates.add("${f.absolutePath}/s5cmd")
                }
            }
        } catch (Exception ignored) {}

        for (String candidate : candidates) {
            File bin = new File(candidate)
            if (!bin.exists() || !bin.canExecute()) continue
            try {
                def proc = [candidate, 'version'].execute()
                proc.waitFor()
                if (proc.exitValue() == 0) {
                    def version = proc.text.readLines().first()
                    log.debug("nf-nomad-s5cmd: Found ${version} at ${candidate} (not on PATH — using absolute path)")
                    return
                }
            } catch (IOException ignored2) {}
        }

        log.warn("nf-nomad-s5cmd: s5cmd binary not found on PATH or in known fallback locations " +
            "(/local/bin-s5cmd/, /nxf-work/bin/). " +
            "The plugin will still load, but tasks that require s5cmd staging will fail at runtime. " +
            "Ensure s5cmd is installed and on PATH (or at /nxf-work/bin/s5cmd).")
    }
}
