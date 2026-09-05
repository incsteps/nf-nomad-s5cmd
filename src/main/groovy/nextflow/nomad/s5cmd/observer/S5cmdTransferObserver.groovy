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

package nextflow.nomad.s5cmd.observer

import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.nomad.s5cmd.config.S5cmdConfig
import nextflow.nomad.s5cmd.config.S5cmdConfigLocator
import nextflow.nomad.s5cmd.strategy.S5cmdCommandBuilder
import nextflow.trace.TraceObserverV2

/**
 * Session-level observer that materialises the {@link S5cmdConfig} from
 * the active {@code s5cmd { ... }} scope and surfaces a startup banner
 * so operators see the plugin is active and what endpoint it'll talk to.
 *
 * Future work: wire this observer to inject a {@code S5cmdFileCopyStrategy}
 * into the session's executor configuration. For now it's a no-op past
 * the banner — intentional: the whole staging path is still being designed.
 */
@Slf4j
class S5cmdTransferObserver implements TraceObserverV2 {

    private S5cmdConfig config

    @Override
    void onFlowCreate(Session session) {
        Map<String, Object> s5cmdScope = S5cmdConfigLocator.locate(
                (Map<String, Object>) session?.config)
        if( s5cmdScope == null ) {
            log.debug('nf-nomad-s5cmd: no `nomad.s5cmd { ... }` scope in nextflow.config — plugin inactive')
            return
        }
        try {
            config = S5cmdConfig.fromMap(s5cmdScope)
            config.validate()
        } catch (Exception e) {
            log.error("nf-nomad-s5cmd: invalid configuration — ${e.message}")
            return
        }
        if( !config.enabled ) {
            log.info('nf-nomad-s5cmd: plugin disabled (enabled=false)')
            return
        }
        final endpoint = config.s3.endpoint ?: '<aws-default>'
        log.info("nf-nomad-s5cmd: active; endpoint=${endpoint} region=${config.s3.region} pathStyle=${config.s3.usePathStyle} cp.concurrency=${config.cp.concurrency} numWorkers=${config.cp.numWorkers} prefixes=${config.paths ?: '(none)'}")
    }

    @Override
    void onFlowComplete() {
        if( !config?.enabled ) return
        log.debug('nf-nomad-s5cmd: session complete')
        warnIfLegacySweepRequested()
    }

    /**
     * The staged {@code inputs/} dirs are reclaimed per task, by each task's own
     * EXIT trap, against that task's own {@code NXF_S5CMD_REMOTE_WORKDIR}
     * (see {@code S5cmdNomadInterop.bootstrapScript}). Controlled by
     * {@code nomad.s5cmd.workDir.cleanupRemoteInputs} — set it to {@code false}
     * to keep every task's inputs for debugging.
     *
     * <p>This method used to run, at end of run:</p>
     *
     * <pre>s5cmd rm 's3://&lt;bucket&gt;/&lt;prefix&gt;/*&#47;inputs/*'</pre>
     *
     * <p>That wildcard was scoped to the configured work-dir root, which is NOT
     * session-scoped: every run sharing a bucket+prefix writes task dirs under it.
     * A run reaching {@code onFlowComplete} therefore deleted the staged inputs of
     * any pipeline still executing against the same prefix, whose tasks then failed
     * stage-in with a missing input — and the wildcard also erased the forensic
     * state needed to diagnose that. It additionally held every task's inputs for
     * the whole run instead of freeing them as tasks finished.</p>
     *
     * <p>Superseded by the per-task reclaim; kept only as an explicit, warned
     * opt-in for anyone who relied on the old sweep.</p>
     */
    protected void warnIfLegacySweepRequested() {
        if( !config.workDir.enabled ) return
        if( !config.workDir.legacyEndOfRunInputSweep ) return
        String root = config.workDir.rootUrl()
        if( !root ) return
        log.warn("nf-nomad-s5cmd: legacyEndOfRunInputSweep=true — deleting ${root}*/inputs/* . " +
                 'This wildcard is NOT scoped to this run and will delete the staged inputs of any ' +
                 'other pipeline using the same bucket+prefix. Prefer the default per-task reclaim.')
        sweepRemoteInputs(root)
    }

    /** Best-effort legacy wildcard sweep — never fails the run. */
    protected void sweepRemoteInputs(String root) {
        try {
            S5cmdCommandBuilder b = new S5cmdCommandBuilder(config)
            String cmd = b.buildRemove(root + '*/inputs/*')
            String exports = b.envExports()
            String full = exports ? (exports + '\n' + cmd) : cmd
            Process p = ['bash', '-c', full].execute()
            StringBuffer out = new StringBuffer()
            StringBuffer err = new StringBuffer()
            p.consumeProcessOutput(out, err)
            int rc = p.waitFor()
            if( rc == 0 )
                log.debug("nf-nomad-s5cmd: swept staged inputs/ from ${root}")
            else
                log.warn("nf-nomad-s5cmd: input sweep rc=${rc} (non-fatal; e.g. no inputs to remove): ${err.toString().trim()}")
        }
        catch( Exception e ) {
            log.warn("nf-nomad-s5cmd: input sweep failed (non-fatal): ${e.message}")
        }
    }
}
