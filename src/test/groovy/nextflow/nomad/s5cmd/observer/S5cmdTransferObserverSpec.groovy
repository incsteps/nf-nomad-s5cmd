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

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import nextflow.Session
import org.slf4j.LoggerFactory
import spock.lang.Specification

class S5cmdTransferObserverSpec extends Specification {

    private ListAppender<ILoggingEvent> appender
    private Logger observerLogger

    def setup() {
        observerLogger = (Logger) LoggerFactory.getLogger(S5cmdTransferObserver)
        observerLogger.level = Level.DEBUG
        appender = new ListAppender<>()
        appender.start()
        observerLogger.addAppender(appender)
    }

    def cleanup() {
        observerLogger.detachAppender(appender)
    }

    private List<String> messagesAtLevel(Level lvl) {
        appender.list.findAll { it.level == lvl }*.formattedMessage
    }

    private Session sessionWith(Map cfg) {
        Mock(Session) { getConfig() >> cfg }
    }

    def 'onFlowCreate emits a single info-level banner when the plugin is enabled and configured'() {
        given:
        def session = sessionWith(s5cmd: [
            enabled: true,
            paths  : ['s3://nextflow-work/'],
            s3     : [endpoint: 'http://rustfs:9900', region: 'us-east-1', usePathStyle: true],
            cp     : [concurrency: 4, numWorkers: 32],
        ])

        when:
        new S5cmdTransferObserver().onFlowCreate(session)

        then:
        def info = messagesAtLevel(Level.INFO)
        info.size() == 1
        info[0].contains('nf-nomad-s5cmd: active')
        info[0].contains('endpoint=http://rustfs:9900')
        info[0].contains('region=us-east-1')
        info[0].contains('pathStyle=true')
        info[0].contains('cp.concurrency=4')
        info[0].contains('numWorkers=32')
    }

    def 'onFlowCreate falls back to <aws-default> when no endpoint is configured'() {
        given:
        def session = sessionWith(s5cmd: [enabled: true])

        when:
        new S5cmdTransferObserver().onFlowCreate(session)

        then:
        messagesAtLevel(Level.INFO).any { it.contains('endpoint=<aws-default>') }
    }

    def 'onFlowCreate logs disabled when enabled=false (no banner)'() {
        given:
        def session = sessionWith(s5cmd: [enabled: false])

        when:
        new S5cmdTransferObserver().onFlowCreate(session)

        then:
        def info = messagesAtLevel(Level.INFO)
        info.size() == 1
        info[0].contains('plugin disabled')
    }

    def 'onFlowCreate is a quiet no-op when there is no s5cmd { } scope'() {
        given:
        def session = sessionWith([:])

        when:
        new S5cmdTransferObserver().onFlowCreate(session)

        then:
        // No info / warn / error — only a debug line about the missing scope
        messagesAtLevel(Level.INFO) == []
        messagesAtLevel(Level.WARN) == []
        messagesAtLevel(Level.ERROR) == []
    }

    def 'onFlowCreate logs an error and stops short when the config is malformed'() {
        given:
        def session = sessionWith(s5cmd: [s3: [endpoint: 'rustfs.aither:9900']])   // missing scheme

        when:
        new S5cmdTransferObserver().onFlowCreate(session)

        then:
        def errors = messagesAtLevel(Level.ERROR)
        errors.size() == 1
        errors[0].contains('invalid configuration')
        // No banner emitted
        messagesAtLevel(Level.INFO) == []
    }

    def 'onFlowComplete logs only when the plugin is active'() {
        given:
        def observer = new S5cmdTransferObserver()
        observer.onFlowCreate(sessionWith(s5cmd: [enabled: true]))
        appender.list.clear()

        when:
        observer.onFlowComplete()

        then:
        // session-complete is debug-level so it doesn't overshadow Nextflow's
        // own stdout on the head job during normal runs
        messagesAtLevel(Level.DEBUG).any { it.contains('session complete') }
    }

    def 'onFlowComplete is silent when no flow was created'() {
        when:
        new S5cmdTransferObserver().onFlowComplete()

        then:
        appender.list.empty
    }
    // ── the end-of-run wildcard sweep is no longer the default ──────────────────
    // `s5cmd rm <root>*/inputs/*` is scoped to the configured work-dir root, which
    // is NOT session-scoped. A finishing run therefore deleted the staged inputs of
    // any pipeline still executing against the same bucket+prefix, whose tasks then
    // failed stage-in on a missing input. Reclamation is now per task.

    def 'onFlowComplete does not run the run-spanning wildcard sweep by default'() {
        given:
        def observer = new S5cmdTransferObserver()
        observer.onFlowCreate(sessionWith(s5cmd: [
            enabled: true,
            workDir: [enabled: true, bucket: 's3://nextflow-work/', prefix: 'pipelines/'],
        ]))

        when:
        observer.onFlowComplete()

        then: 'no warning about the destructive wildcard, because it never ran'
        !messagesAtLevel(Level.WARN).any { it.contains('legacyEndOfRunInputSweep') }
    }

    def 'the legacy wildcard sweep is opt-in and warns that it crosses runs'() {
        given:
        def observer = new S5cmdTransferObserver()
        observer.onFlowCreate(sessionWith(s5cmd: [
            enabled: true,
            workDir: [enabled: true, bucket: 's3://nextflow-work/', prefix: 'pipelines/',
                      legacyEndOfRunInputSweep: true],
        ]))

        when:
        observer.onFlowComplete()

        then: 'the operator is told what the wildcard will reach before it is used'
        def warns = messagesAtLevel(Level.WARN)
        warns.any { it.contains('legacyEndOfRunInputSweep=true') && it.contains('other pipeline') }
    }

}
