/*
 * Copyright 2022, Seqera Labs
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

import java.nio.file.Path
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord
import groovy.json.JsonOutput
import java.time.ZonedDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.Instant
import java.math.BigDecimal
import nextflow.file.FileHolder
import groovy.transform.CompileStatic

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.ScheduledExecutorService
import java.util.Collections

@CompileStatic
@Slf4j
class LiveTracker {
    private final Path baseOutputPath
    private final String filePrefix

    // Thread-safe event list
    private final List<Map> eventLogs = Collections.synchronizedList(new ArrayList<>())

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1)
    private final int dumpInterval

    LiveTracker(Path outputPath, int interval) {
        this.baseOutputPath = outputPath.getParent()
        this.filePrefix     = outputPath.getFileName().toString()
        this.dumpInterval   = interval
        log.info "Live Tracker initialized for directory: ${baseOutputPath}, file prefix: ${filePrefix}"
        scheduleDumpEvents()
    }

    private static def jsonify(root) {
        if( root instanceof Map ) {
            return root.collectEntries { k, v -> [k, jsonify(v)] }
        }
        else if( root instanceof Collection ) {
            return root.collect { v -> jsonify(v) }
        }
        else if( root instanceof FileHolder ) {
            return jsonify(root.storePath)
        }
        else if( root instanceof Path ) {
            return root.toUriString()
        }
        else if( root instanceof Boolean || root instanceof Number ) {
            return root
        }
        else {
            return root?.toString()
        }
    }

    void logEvent(String eventType, TaskHandler handler, TraceRecord trace, String workflowSessionId, String workflowRunName) {
        final String utcNow = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT)

        final BigDecimal durationSeconds = trace.get('duration')
                ? ((BigDecimal) trace.get('duration')).divide(new BigDecimal("1000.0"))
                : null
        final BigDecimal realtimeSeconds = trace.get('realtime')
                ? ((BigDecimal) trace.get('realtime')).divide(new BigDecimal("1000.0"))
                : null

        final String submitTime = trace.get('submit')
                ? Instant.ofEpochMilli((Long) trace.get('submit')).atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT)
                : null
        final String startTime = trace.get('start')
                ? Instant.ofEpochMilli((Long) trace.get('start')).atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT)
                : null
        final String completeTime = trace.get('complete')
                ? Instant.ofEpochMilli((Long) trace.get('complete')).atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT)
                : null

        final Map eventDetails = [
            session_id       : workflowSessionId,
            run_name         : workflowRunName,
            event_type       : eventType,
            task_id          : handler.task.id,
            task_name        : handler.task.name,
            status           : trace.get('status'),
            exit             : trace.get('exit'),
            submit           : submitTime,
            start            : startTime,
            complete         : completeTime,
            duration_seconds : durationSeconds,
            realtime_seconds : realtimeSeconds,
            attempt          : trace.get('attempt'),
            workdir          : trace.get('workdir'),
            env              : trace.get('env'),
            UTC_TIME         : utcNow,
            task             : [
                id      : handler.task.id as String,
                name    : handler.task.name,
                cached  : handler.task.cached,
                process : handler.task.processor.name,
                inputs  : handler.task.inputs.findResults { inParam, object ->
                    def inputMap = [
                        name : inParam.getName(),
                        value: jsonify(object)
                    ]
                    (inputMap.name != '$') ? inputMap : null
                },
                outputs : handler.task.outputs.findResults { outParam, object ->
                    def outputMap = [
                        name : outParam.getName(),
                        emit : outParam.getChannelEmitName(),
                        value: jsonify(object)
                    ]
                    (outputMap.name != '$') ? outputMap : null
                }
            ]
        ]
        log.info "Event logged: ${eventType} for task: ${handler.task.name}"
        eventLogs.add(eventDetails) // Thread-safe add
    }

    void renderEvents() {
        try {
            // Safely copy and clear the list
            List<Map> snapshot
            synchronized(eventLogs) {
                if(eventLogs.isEmpty()) {
                    return
                }
                snapshot = new ArrayList<>(eventLogs)
                eventLogs.clear()
            }

            final String timestamp = ZonedDateTime.now(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
            final String filename = filePrefix + "_" + timestamp + ".json"
            final Path currentOutputPath = baseOutputPath.resolve(filename)

            currentOutputPath.toFile().withWriter('UTF-8') { writer ->
                writer.write(JsonOutput.prettyPrint(JsonOutput.toJson(snapshot)))
            }
            log.info "Data written to file: ${currentOutputPath}"
        }
        catch(Exception e) {
            log.error("Failed to write events: ${e.message}", e)
        }
    }

    private void scheduleDumpEvents() {
        log.info "Scheduling dump events every ${dumpInterval} seconds"
        scheduler.scheduleAtFixedRate({
            log.info "Attempting to render events."
            renderEvents()
        }, 0, dumpInterval, TimeUnit.SECONDS)
    }

    void shutdownTracker() {
        // Graceful shutdown
        scheduler.shutdown()
        try {
            if(!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Executor did not terminate in the specified time. Forcing shutdown...")
                scheduler.shutdownNow()
            }
            renderEvents() // Final flush
        }
        catch(InterruptedException e) {
            log.error("Scheduler shutdown interrupted", e)
        }
    }
}

@CompileStatic
@Slf4j
class LiveObserver implements TraceObserver {

    private final LiveTracker liveTracker
    private String workflowSessionId
    private String workflowRunName

    LiveObserver(Path outputPath, int interval) {
        this.liveTracker = new LiveTracker(outputPath, interval)
    }

    @Override
    void onFlowCreate(Session session) {
        this.workflowSessionId = session.getUniqueId()
        this.workflowRunName   = session.getRunName()
        log.info "LiveObserver captured workflow sessionId=${workflowSessionId}, runName=${workflowRunName}"
    }

    @Override
    void onProcessSubmit(TaskHandler handler, TraceRecord trace) {
        log.info "Process Submitted: ${handler.task.name}"
        liveTracker.logEvent("Process Submitted", handler, trace, workflowSessionId, workflowRunName)
    }

    @Override
    void onProcessStart(TaskHandler handler, TraceRecord trace) {
        liveTracker.logEvent("Process Started", handler, trace, workflowSessionId, workflowRunName)
    }

    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        liveTracker.logEvent("Process Completed", handler, trace, workflowSessionId, workflowRunName)
    }

    @Override
    void onFlowComplete() {
        liveTracker.shutdownTracker()
    }

    @Override
    void onFlowError(TaskHandler handler, TraceRecord trace) {
        liveTracker.logEvent("Workflow Error", handler, trace, workflowSessionId, workflowRunName)
        liveTracker.shutdownTracker()
    }
}
