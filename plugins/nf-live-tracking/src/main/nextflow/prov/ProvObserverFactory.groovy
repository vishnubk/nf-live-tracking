package nextflow.prov

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.trace.TraceObserverFactory
import java.nio.file.Paths
import java.util.Collections

/**
 * Factory for creating trace observers, including live event tracking.
 */
@Slf4j
@CompileStatic
class LiveObserverFactory implements TraceObserverFactory {

    @Override
    Collection<TraceObserver> create(Session session) {
        Map config = session.config
        List<TraceObserver> observers = []

        TraceObserver liveObserver = createLiveObserver(config)
        if (liveObserver != null) {
            observers.add(liveObserver)
        }

        return observers
    }

    protected TraceObserver createLiveObserver(Map config) {
        final enabled = config.navigate('live.enabled', true) as Boolean
        if (!enabled) {
            return null
        }
        
        final filePrefix = config.navigate('live.file', 'workflowTrace') as String // Using a better default prefix
        final outputPath = Paths.get(filePrefix) // Use this prefix directly without adding .json here
        final interval = config.navigate('live.interval', 600) as Integer // 600 seconds/10 minutes default
        return new LiveObserver(outputPath, interval)
    }
}


