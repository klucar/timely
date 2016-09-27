package timely.adapter.accumulo;

import com.google.inject.Inject;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;
import timely.model.Metric;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.accumulo.core.conf.AccumuloConfiguration.getMemoryInBytes;
import static org.apache.accumulo.core.conf.AccumuloConfiguration.getTimeInMillis;

/**
 *
 *
 */
public class MetricWriter {

    private static final Logger LOG = LoggerFactory.getLogger(MetricWriter.class);

    @Inject
    private Configuration configuration;

    @Inject
    private Connector connector;

    private final BatchWriterConfig bwConfig;
    private final List<BatchWriter> writers = new ArrayList<>();
    private final ThreadLocal<BatchWriter> metaWriter = new ThreadLocal<>();
    private final ThreadLocal<BatchWriter> metricsWriter = new ThreadLocal<>();

    private final String metaTable;
    private final String metricsTable;

    public MetricWriter() {
        Configuration.Accumulo accumuloConf = configuration.getAccumulo();

        bwConfig = new BatchWriterConfig();
        int numWriteThreads = configuration.getAccumulo().getWrite().getThreads();
        bwConfig.setMaxLatency(getTimeInMillis(accumuloConf.getWrite().getLatency()), TimeUnit.MILLISECONDS);
        bwConfig.setMaxMemory(getMemoryInBytes(accumuloConf.getWrite().getBufferSize()) / numWriteThreads);
        bwConfig.setMaxWriteThreads(accumuloConf.getWrite().getThreads());

        metaTable = configuration.getMetaTable();
        metricsTable = configuration.getMetricsTable();
    }

    public boolean writeMetricMutation(Metric metric) {
        try {
            getMetricsWriter().addMutation(MetricAdapter.toMutation(metric));
            return true;
        } catch (MutationsRejectedException e) {
            LOG.error("Unable to write to metrics table", e);
            try {
                try {
                    final BatchWriter w = metricsWriter.get();
                    metricsWriter.remove();
                    writers.remove(w);
                    w.close();
                } catch (MutationsRejectedException e1) {
                    LOG.error("Error closing metric writer", e1);
                }
                final BatchWriter w = connector.createBatchWriter(metricsTable, bwConfig);
                metricsWriter.set(w);
                writers.add(w);
            } catch (TableNotFoundException e1) {
                LOG.error("Unexpected error recreating meta batch writer, shutting down Timely server: {}", e1);
                System.exit(1);
            }
        }
        return false;
    }

    public boolean writeMetaMutations(Set<Mutation> muts) {
        try {
            getMetaWriter().addMutations(muts);
            return true;
        } catch (MutationsRejectedException e) {
            LOG.error("Unable to write to meta table", e);
            try {
                try {
                    final BatchWriter w = metaWriter.get();
                    metaWriter.remove();
                    writers.remove(w);
                    w.close();
                } catch (MutationsRejectedException e1) {
                    LOG.error("Error closing meta writer", e1);
                }
                final BatchWriter w = connector.createBatchWriter(metaTable, bwConfig);
                metaWriter.set(w);
                writers.add(w);
            } catch (TableNotFoundException e1) {
                LOG.error("Unexpected error recreating meta batch writer, shutting down Timely server: {}", e1);
                System.exit(1);
            }
        }
        return false;
    }

    private BatchWriter getMetaWriter() {
        if (null == metaWriter.get()) {
            try {
                BatchWriter w = connector.createBatchWriter(metaTable, bwConfig);
                metaWriter.set(w);
                writers.add(w);
            } catch (TableNotFoundException e) {
                // TODO get consensus on what to do here
                LOG.error("Error creating meta batch writer", e);
            }
        }
        return metaWriter.get();
    }

    private BatchWriter getMetricsWriter() {
        if (null == metricsWriter.get()) {
            try {
                BatchWriter w = connector.createBatchWriter(metricsTable, bwConfig);
                metricsWriter.set(w);
                writers.add(w);
            } catch (TableNotFoundException e) {
                // TODO get consensus on what to do here
                LOG.error("Error creating meta batch writer", e);
            }
        }
        return metricsWriter.get();
    }

    public void flushWriters() {
        writers.forEach(w -> {
            try {
                w.close();
            } catch (final Exception ex) {
                LOG.warn("Error shutting down batchwriter", ex);
            }

        });
    }

}
