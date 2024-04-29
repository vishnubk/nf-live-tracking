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

package nextflow.prov

import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.PathMatcher

import groovy.transform.CompileStatic
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

// /**
//  * Plugin observer of workflow events
//  *
//  * @author Bruno Grande <bruno.grande@sagebase.org>
//  * @author Ben Sherman <bentshermann@gmail.com>
//  */
@Slf4j
@CompileStatic
class ProvObserver implements TraceObserver {

    public static final List<String> VALID_FORMATS = ['bco', 'dag', 'legacy', 'live']

    private Session session

    private List<Renderer> renderers

    private List<PathMatcher> matchers

    private Set<TaskRun> tasks = []

    private Map<Path,Path> workflowOutputs = [:]

    ProvObserver(Map<String,Map> formats, List<String> patterns) {
        this.renderers = formats.collect( (name, config) -> createRenderer(name, config) )
        this.matchers = patterns.collect( pattern ->
            FileSystems.getDefault().getPathMatcher("glob:**/${pattern}")
        )
    }

    private Renderer createRenderer(String name, Map opts) {
        if( name == 'bco' )
            return new BcoRenderer(opts)

        if( name == 'dag' )
            return new DagRenderer(opts)

        if( name == 'legacy' )
            return new LegacyRenderer(opts)
        
        if( name == 'live' )
            return null
        //     return new LiveObserver(opts)

        throw new IllegalArgumentException("Invalid provenance format -- valid formats are ${VALID_FORMATS.join(', ')}")
    }

    @Override
    void onFlowCreate(Session session) {
        this.session = session
    }

    void onProcessSubmit(TaskHandler handler, TraceRecord trace){

        String exitValue = trace.get('exit') // Assuming there's a getter like this
        String statusValue = trace.get('status') // Same as above

    }
    

    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        // skip failed tasks
        final task = handler.task
        if( !task.isSuccess() )
            return
        tasks << task
    }

    @Override
    void onProcessCached(TaskHandler handler, TraceRecord trace) {
        tasks << handler.task
    }

    @Override
    void onFilePublish(Path destination, Path source) {
        boolean match = matchers.isEmpty() || matchers.any { matcher ->
            matcher.matches(destination)
        }

        if( !match )
            return

        workflowOutputs[source] = destination
    }

    @Override
    void onFlowComplete() {
        if( !session.isSuccess() )
            return

        renderers.each( renderer ->
            renderer.render(session, tasks, workflowOutputs)
        )
    }

}


@CompileStatic
class LiveTracker {
    private Path outputPath
    private List<Map> eventLogs = []

    LiveTracker(Path outputPath) {
        this.outputPath = outputPath
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
            //'script': handler.task.script,
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
    eventLogs << eventDetails
}

    void renderEvents() {
        if (!eventLogs.isEmpty()) {
            outputPath.toFile().withWriter('UTF-8') { writer ->
                writer.write(JsonOutput.prettyPrint(JsonOutput.toJson(eventLogs)))
            }
        }
    }
}
@Slf4j
@CompileStatic
class LiveObserver implements TraceObserver {
    private LiveTracker liveTracker

    LiveObserver(Path outputPath) {
    liveTracker = new LiveTracker(outputPath)
    }

    @Override
    void onProcessSubmit(TaskHandler handler, TraceRecord trace) {
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
        liveTracker.renderEvents() // Renders all collected data at the end of the workflow
    }

    @Override
    void onFlowError(TaskHandler handler, TraceRecord trace) {
        liveTracker.logEvent("Workflow Error", handler, trace)
        liveTracker.renderEvents()
    }
}
