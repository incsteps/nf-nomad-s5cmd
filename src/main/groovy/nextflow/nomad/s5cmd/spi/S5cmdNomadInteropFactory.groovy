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
package nextflow.nomad.s5cmd.spi

import groovy.transform.CompileStatic
import nextflow.nomad.executor.spi.DistributedWorkdirProvider
import nextflow.nomad.executor.spi.DistributedWorkdirProviderFactory
import nextflow.nomad.s5cmd.config.S5cmdConfigLocator
import nextflow.processor.TaskRun
import org.pf4j.Extension

import java.nio.file.Path

/**
 * Registers nf-nomad-s5cmd's {@link S5cmdNomadInterop} with nf-nomad as a
 * distributed-workdir backend. Discovered at runtime by nf-nomad via
 * {@code Plugins.getExtensions(DistributedWorkdirProviderFactory.class)}.
 *
 * <p>Active when both {@code nomad.s5cmd.enabled = true} (default) and
 * {@code nomad.s5cmd.workDir.enabled = true} appear in the Nextflow session
 * config — and {@code nomad.s5cmd.workDir.bucket} is set. Legacy top-level
 * {@code s5cmd { ... }} scope is still accepted with a deprecation warning;
 * see {@link S5cmdConfigLocator}.</p>
 */
@CompileStatic
@Extension
class S5cmdNomadInteropFactory implements DistributedWorkdirProviderFactory {

    @Override
    String name() { 's5cmd' }

    @Override
    boolean isEnabled(Map sessionConfig) {
        Map<String, Object> s5cmdScope = S5cmdConfigLocator.locate(
                (Map<String, Object>) sessionConfig)
        if( s5cmdScope == null ) return false
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
