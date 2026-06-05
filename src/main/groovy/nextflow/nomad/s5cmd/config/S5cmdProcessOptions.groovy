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

package nextflow.nomad.s5cmd.config

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.processor.TaskRun

/**
 * Resolves per-process s5cmd overrides supplied via the {@code nomadOptions}
 * directive on a Nextflow process:
 *
 * <pre>
 * process {
 *     withName: 'BIG_DOWNLOAD' {
 *         nomadOptions = [
 *             s5cmd: [
 *                 cp: [concurrency: 20, numWorkers: 512]
 *             ]
 *         ]
 *     }
 * }
 * </pre>
 *
 * The override is deep-merged onto the session-level config (typically
 * {@code nomad.s5cmd { ... }} resolved by {@link S5cmdConfigLocator}). Only
 * keys explicitly present in the override are applied; everything else
 * inherits from the session base. Sub-maps {@code s3}, {@code cp}, and
 * {@code workDir} are merged element-wise; lists ({@code paths},
 * {@code publishPaths}, {@code cp.extraFlags}) are replaced wholesale —
 * lists in Nextflow config are conventionally complete declarations, not
 * partial updates.
 *
 * <p>Returns the merged map. nf-nomad-s5cmd then constructs a fresh
 * {@link S5cmdConfig} from that map — no other code path needs to know
 * about the per-task overlay.</p>
 */
@Slf4j
@CompileStatic
class S5cmdProcessOptions {

    private static final String NOMAD_OPTIONS_KEY = 'nomadOptions'
    private static final String S5CMD_KEY = 's5cmd'
    private static final Set<String> SUBMAP_KEYS = ['s3', 'cp', 'workDir'] as Set

    /**
     * Merge per-task overrides (from {@code task.config.nomadOptions.s5cmd})
     * onto the session-level base. Returns a new map; never mutates inputs.
     */
    static Map<String, Object> mergePerTask(Map<String, Object> base, TaskRun task) {
        Map<String, Object> overrides = readPerTaskOverrides(task)
        if( overrides == null || overrides.isEmpty() ) {
            return base ?: Collections.<String, Object>emptyMap()
        }
        return deepMerge(base ?: Collections.<String, Object>emptyMap(), overrides)
    }

    /**
     * Extract {@code nomadOptions.s5cmd} from a task's process config. Returns
     * {@code null} when the directive is absent or doesn't carry an s5cmd
     * sub-key. Tolerates a missing config entirely (returns null), so this is
     * safe to call on tasks that never opted in.
     */
    static Map<String, Object> readPerTaskOverrides(TaskRun task) {
        try {
            def taskConfig = task?.config
            if( taskConfig == null ) return null
            Object directive = taskConfig.get(NOMAD_OPTIONS_KEY)
            if( !(directive instanceof Map) ) return null
            Object s5cmdNode = ((Map) directive).get(S5CMD_KEY)
            if( !(s5cmdNode instanceof Map) ) return null
            return (Map<String, Object>) s5cmdNode
        }
        catch (Throwable t) {
            log.debug("nf-nomad-s5cmd: could not read nomadOptions.s5cmd from task config: ${t.message}")
            return null
        }
    }

    /**
     * Deep-merge {@code overrides} onto {@code base}. Sub-maps with one of
     * {@link #SUBMAP_KEYS} are merged element-wise; everything else
     * (including lists) is replaced. Inputs are not mutated.
     */
    static Map<String, Object> deepMerge(Map<String, Object> base, Map<String, Object> overrides) {
        Map<String, Object> result = new LinkedHashMap<>(base)
        for( Map.Entry<String, Object> entry : overrides.entrySet() ) {
            String key = entry.key
            Object overrideVal = entry.value
            if( key in SUBMAP_KEYS && overrideVal instanceof Map ) {
                Object baseVal = result.get(key)
                Map<String, Object> baseSub = (baseVal instanceof Map)
                        ? (Map<String, Object>) baseVal
                        : Collections.<String, Object>emptyMap()
                Map<String, Object> mergedSub = new LinkedHashMap<>(baseSub)
                mergedSub.putAll((Map<String, Object>) overrideVal)
                result.put(key, mergedSub)
            } else {
                result.put(key, overrideVal)
            }
        }
        return result
    }
}
