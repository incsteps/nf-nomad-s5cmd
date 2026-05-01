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
