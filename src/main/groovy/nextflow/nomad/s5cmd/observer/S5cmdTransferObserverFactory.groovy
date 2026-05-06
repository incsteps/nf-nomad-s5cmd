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
import nextflow.trace.TraceObserverFactoryV2
import nextflow.trace.TraceObserverV2
import org.pf4j.Extension

/**
 * Registers a {@link S5cmdTransferObserver} on every Nextflow session.
 *
 * Required by the io.nextflow.nextflow-plugin Gradle plugin: every
 * Nextflow plugin must declare at least one extension point. This is
 * the lightest possible one — observer factories are invoked exactly
 * once per session and let us emit a startup log line that surfaces
 * the active s5cmd config to the operator.
 */
@Slf4j
@Extension
class S5cmdTransferObserverFactory implements TraceObserverFactoryV2 {

    @Override
    Collection<TraceObserverV2> create(Session session) {
        return [new S5cmdTransferObserver()]
    }
}
