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

package nextflow.s5cmd.observer

import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.s5cmd.config.S5cmdConfig
import nextflow.trace.TraceObserverV2

/**
 * Session-level observer that materialises the {@link S5cmdConfig} from
 * the active {@code s5cmd { ... }} scope and surfaces a startup banner
 * so operators see the plugin is active and what endpoint it'll talk to.
 *
 * Future work: wire this observer to inject a {@code S5cmdFileCopyStrategy}
 * into the session's executor configuration (mirroring nf-rclone's
 * RcloneTransferObserver flow). For now it's a no-op past the banner —
 * intentional: the whole staging path is still being designed.
 */
@Slf4j
class S5cmdTransferObserver implements TraceObserverV2 {

    private S5cmdConfig config

    @Override
    void onFlowCreate(Session session) {
        def s5cmdScope = session?.config?.get('s5cmd')
        if( !(s5cmdScope instanceof Map) ) {
            log.debug('nf-s5cmd: no `s5cmd { ... }` scope in nextflow.config — plugin inactive')
            return
        }
        try {
            config = S5cmdConfig.fromMap(s5cmdScope as Map<String, Object>)
            config.validate()
        } catch (Exception e) {
            log.error("nf-s5cmd: invalid configuration — ${e.message}")
            return
        }
        if( !config.enabled ) {
            log.info('nf-s5cmd: plugin disabled (enabled=false)')
            return
        }
        final endpoint = config.s3.endpoint ?: '<aws-default>'
        log.info("nf-s5cmd: active; endpoint=${endpoint} region=${config.s3.region} pathStyle=${config.s3.usePathStyle} cp.concurrency=${config.cp.concurrency} numWorkers=${config.cp.numWorkers} prefixes=${config.paths ?: '(none)'}")
    }

    @Override
    void onFlowComplete() {
        if( config?.enabled ) {
            log.info('nf-s5cmd: session complete')
        }
    }
}
