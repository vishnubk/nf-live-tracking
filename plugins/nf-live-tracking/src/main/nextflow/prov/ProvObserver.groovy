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



import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.PathMatcher
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord
import nextflow.processor.TaskProcessor
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


@CompileStatic
@Slf4j
class LiveTracker {
    private Path baseOutputPath
    private String filePrefix
    private List<Map> eventLogs = []
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1)
    private int dumpInterval = 600 // default to 600 seconds (10 minutes)

    LiveTracker(Path outputPath, int interval) {
    this.baseOutputPath = outputPath.getParent()  // Ensure this is the intended directory
    this.filePrefix = outputPath.getFileName().toString()  // The intended file prefix without '.json'
    this.dumpInterval = interval
    log.info "Live Tracker initialized for directory: ${baseOutputPath}, file prefix: ${filePrefix}"
    scheduleDumpEvents()
}



    private static def jsonify(root) {
    if ( root instanceof Map )
        root.collectEntries( (k, v) -> [k, jsonify(v)] )

    else if ( root instanceof Collection )
        root.collect( v -> jsonify(v) )

    else if ( root instanceof FileHolder )
        jsonify(root.storePath)

    else if ( root instanceof Path )
        root.toUriString()

    else if ( root instanceof Boolean || root instanceof Number )
        root

    else
        root.toString()
    }

    void logEvent(String eventType, TaskHandler handler, TraceRecord trace) {
        String utcNow = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT)

        BigDecimal durationSeconds = trace.get('duration') ? ((BigDecimal) trace.get('duration')).divide(new BigDecimal("1000.0")) : null
        BigDecimal realtimeSeconds = trace.get('realtime') ? ((BigDecimal) trace.get('realtime')).divide(new BigDecimal("1000.0")) : null

        String submitTime = trace.get('submit') ? Instant.ofEpochMilli((Long) trace.get('submit')).atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT) : null
        String startTime = trace.get('start') ? Instant.ofEpochMilli((Long) trace.get('start')).atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT) : null
        String completeTime = trace.get('complete') ? Instant.ofEpochMilli((Long) trace.get('complete')).atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT) : null

        Map eventDetails = [
            'event_type': eventType,
            'task_id': handler.task.id,
            'task_name': handler.task.name,
            'status': trace.get('status'),
            'exit': trace.get('exit'),
            'submit': submitTime,
            'start': startTime,
            'complete': completeTime,
            'duration_seconds': durationSeconds,
            'realtime_seconds': realtimeSeconds,
            'attempt': trace.get('attempt'),
            'workdir': trace.get('workdir'),
            'env': trace.get('env'),
            'UTC_TIME': utcNow,
            'task': [
                'id': handler.task.id as String,
                'name': handler.task.name,
                'cached': handler.task.cached,
                'process': handler.task.processor.name,
                //'script': handler.task.script, // This is a large string and may not be useful to dump
                'inputs': handler.task.inputs.findResults { inParam, object -> 
                    def inputMap = [ 
                        'name': inParam.getName(),
                        'value': jsonify(object) 
                    ] 
                    inputMap['name'] != '$' ? inputMap : null
                },
                'outputs': handler.task.outputs.findResults { outParam, object -> 
                    def outputMap = [
                        'name': outParam.getName(),
                        'emit': outParam.getChannelEmitName(),
                        'value': jsonify(object) 
                    ] 
                    outputMap['name'] != '$' ? outputMap : null
                }
            ]
        ]
        log.info "Event logged: ${eventType} for task: ${handler.task.name}"

        eventLogs << eventDetails
    }

    void renderEvents() {
    try {
        if (!eventLogs.isEmpty()) {
            String timestamp = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
            String filename = filePrefix + "_" + timestamp + ".json"
            Path currentOutputPath = baseOutputPath.resolve(filename)
            currentOutputPath.toFile().withWriter('UTF-8') { writer ->
                writer.write(JsonOutput.prettyPrint(JsonOutput.toJson(eventLogs)))
            }
            log.info "Data written to file: ${currentOutputPath}"
            eventLogs.clear() // Clear the list for the next interval
        }
    } catch (Exception e) {
        log.error("Failed to write events: ${e.message}", e)
    }
}


    private void scheduleDumpEvents() {
        log.info "Scheduling dump events every ${dumpInterval} seconds"
        scheduler.scheduleAtFixedRate({ ->
        log.info "Attempting to render events." 
        renderEvents()
    }, 0, dumpInterval, TimeUnit.SECONDS)
}

    void shutdownTracker() {
        scheduler.shutdownNow()
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Executor did not terminate in the specified time.");
            }
            renderEvents() // Ensure all logs are written
        } catch (InterruptedException e) {
            log.error("Scheduler shutdown interrupted", e)
        }
    }
}




@CompileStatic
@Slf4j
class LiveObserver implements TraceObserver {
    private LiveTracker liveTracker

    LiveObserver(Path outputPath, int interval) {
        liveTracker = new LiveTracker(outputPath, interval)
    }

    @Override
    void onProcessSubmit(TaskHandler handler, TraceRecord trace) {
        log.info "Process Submitted: ${handler.task.name}"
        liveTracker.logEvent("Process Submitted", handler, trace)
    }

    @Override
    void onProcessStart(TaskHandler handler, TraceRecord trace) {
        liveTracker.logEvent("Process Started", handler, trace)
    }

    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        liveTracker.logEvent("Process Completed", handler, trace)
    }

    @Override
    void onFlowComplete() {
        liveTracker.shutdownTracker()
    }

    @Override
    void onFlowError(TaskHandler handler, TraceRecord trace) {
        liveTracker.logEvent("Workflow Error", handler, trace)
        liveTracker.shutdownTracker()
    }
}
