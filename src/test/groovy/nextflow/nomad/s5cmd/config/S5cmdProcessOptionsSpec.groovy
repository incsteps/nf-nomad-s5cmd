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

import nextflow.processor.TaskConfig
import nextflow.processor.TaskRun
import spock.lang.Specification

class S5cmdProcessOptionsSpec extends Specification {

    /** Build a minimal TaskRun whose `config.nomadOptions = nomadOpts`. */
    private TaskRun taskWithNomadOptions(Map nomadOpts) {
        def task = Mock(TaskRun)
        def cfg = new TaskConfig([nomadOptions: nomadOpts])
        task.getConfig() >> cfg
        return task
    }

    def 'readPerTaskOverrides returns null when nomadOptions is absent'() {
        expect:
        S5cmdProcessOptions.readPerTaskOverrides(null) == null
        S5cmdProcessOptions.readPerTaskOverrides(taskWithNomadOptions(null)) == null
    }

    def 'readPerTaskOverrides returns null when nomadOptions has no s5cmd key'() {
        expect:
        S5cmdProcessOptions.readPerTaskOverrides(
                taskWithNomadOptions([something: 'else'])
        ) == null
    }

    def 'readPerTaskOverrides extracts the s5cmd sub-map'() {
        given:
        def task = taskWithNomadOptions([s5cmd: [cp: [concurrency: 20]]])

        expect:
        S5cmdProcessOptions.readPerTaskOverrides(task) == [cp: [concurrency: 20]]
    }

    def 'deepMerge merges sub-maps element-wise without losing base keys'() {
        given:
        Map<String, Object> base = [
                enabled: true,
                cp     : [concurrency: 5, numWorkers: 256, retryCount: 10],
                s3     : [endpoint: 'http://rustfs:9900', region: 'us-east-1'],
        ]
        Map<String, Object> override = [
                cp: [concurrency: 20, numWorkers: 512],
        ]

        when:
        Map<String, Object> merged = S5cmdProcessOptions.deepMerge(base, override)

        then:
        merged.enabled == true
        merged.cp == [concurrency: 20, numWorkers: 512, retryCount: 10]
        merged.s3 == [endpoint: 'http://rustfs:9900', region: 'us-east-1']
    }

    def 'deepMerge replaces lists wholesale (paths, extraFlags) — partial list merge would surprise users'() {
        given:
        Map<String, Object> base = [
                paths: ['s3://a/', 's3://b/'],
                cp   : [extraFlags: ['--no-verify-ssl']],
        ]
        Map<String, Object> override = [
                paths: ['s3://c/'],
                cp   : [extraFlags: []],
        ]

        when:
        Map<String, Object> merged = S5cmdProcessOptions.deepMerge(base, override)

        then:
        merged.paths == ['s3://c/']
        merged.cp.extraFlags == []
    }

    def 'mergePerTask returns base when task has no overrides'() {
        given:
        Map<String, Object> base = [enabled: true, cp: [concurrency: 5]]
        def task = taskWithNomadOptions([:])

        expect:
        S5cmdProcessOptions.mergePerTask(base, task) == base
    }

    def 'mergePerTask applies per-process s5cmd overrides on top of session base'() {
        given:
        Map<String, Object> base = [enabled: true, cp: [concurrency: 5, numWorkers: 256]]
        def task = taskWithNomadOptions([s5cmd: [cp: [concurrency: 20]]])

        when:
        Map<String, Object> merged = S5cmdProcessOptions.mergePerTask(base, task)

        then:
        merged.enabled == true
        merged.cp == [concurrency: 20, numWorkers: 256]
    }

    def 'mergePerTask can disable s5cmd per-process'() {
        given:
        Map<String, Object> base = [enabled: true]
        def task = taskWithNomadOptions([s5cmd: [enabled: false]])

        when:
        Map<String, Object> merged = S5cmdProcessOptions.mergePerTask(base, task)

        then:
        merged.enabled == false
    }

    def 'mergePerTask survives a missing task config (defensive)'() {
        given:
        Map<String, Object> base = [enabled: true]
        def task = Mock(TaskRun) { getConfig() >> { throw new RuntimeException('boom') } }

        expect:
        S5cmdProcessOptions.mergePerTask(base, task) == base
    }
}
