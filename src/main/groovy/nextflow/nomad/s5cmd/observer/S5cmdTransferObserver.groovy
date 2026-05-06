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
        if( config?.enabled ) {
            log.info('nf-s5cmd: session complete')
        }
    }
}
