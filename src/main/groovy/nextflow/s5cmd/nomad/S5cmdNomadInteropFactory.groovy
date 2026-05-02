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
package nextflow.s5cmd.nomad

import groovy.transform.CompileStatic
import nextflow.nomad.executor.spi.DistributedWorkdirProvider
import nextflow.nomad.executor.spi.DistributedWorkdirProviderFactory
import nextflow.processor.TaskRun
import org.pf4j.Extension

import java.nio.file.Path

/**
 * Registers nf-s5cmd's {@link S5cmdNomadInterop} with nf-nomad as a
 * distributed-workdir backend. Discovered at runtime by nf-nomad via
 * {@code Plugins.getExtensions(DistributedWorkdirProviderFactory.class)}.
 *
 * <p>Active when both {@code s5cmd.enabled = true} (default) and
 * {@code s5cmd.workDir.enabled = true} appear in the Nextflow session
 * config — and {@code s5cmd.workDir.bucket} is set.</p>
 */
@CompileStatic
@Extension
class S5cmdNomadInteropFactory implements DistributedWorkdirProviderFactory {

    @Override
    String name() { 's5cmd' }

    @Override
    boolean isEnabled(Map sessionConfig) {
        Map s5cmdScope = readMap(sessionConfig, 's5cmd')
        if( !toBool(s5cmdScope.containsKey('enabled') ? s5cmdScope.get('enabled') : true) ) {
            return false
        }
        Map workScope = readMap(s5cmdScope, 'workDir')
        return toBool(workScope.get('enabled')) && (workScope.get('bucket') as String)
    }

    @Override
    DistributedWorkdirProvider create(TaskRun task, Map sessionConfig, Path sessionWorkDir) {
        return new S5cmdNomadInterop(task, sessionConfig, sessionWorkDir)
    }

    private static Map readMap(Map source, String key) {
        if( source == null ) return Collections.emptyMap()
        Object value = source.get(key)
        return value instanceof Map ? (Map) value : Collections.emptyMap()
    }

    private static boolean toBool(Object value) {
        if( value == null ) return false
        if( value instanceof Boolean ) return (Boolean) value
        return Boolean.valueOf(value.toString())
    }
}
