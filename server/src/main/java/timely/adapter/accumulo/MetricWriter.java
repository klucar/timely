package timely.adapter.accumulo;

import com.google.inject.Inject;
import org.apache.accumulo.core.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;
import timely.api.model.Meta;
import timely.cache.MetaCache;
import timely.guice.provider.ConnectorProvider;
import timely.model.Metric;
import timely.model.Tag;
import timely.util.MetaKeySet;

import java.util.ArrayList;
import java.util.List;
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
    Configuration configuration;

    @Inject
    ConnectorProvider connectorProvider;

    @Inject
    TableHelper tableHelper;

    @Inject
    MetaCache metaCache;

    private final BatchWriterConfig bwConfig = new BatchWriterConfig();
    private final List<BatchWriter> writers = new ArrayList<>();
    private final ThreadLocal<BatchWriter> metaWriter = new ThreadLocal<>();
    private final ThreadLocal<BatchWriter> metricsWriter = new ThreadLocal<>();

    private String metaTable;
    private String metricsTable;

    public MetricWriter() {

    }

    public void initialize() {
        Configuration.Accumulo accumuloConf = configuration.getAccumulo();

        int numWriteThreads = configuration.getAccumulo().getWrite().getThreads();
        bwConfig.setMaxLatency(getTimeInMillis(accumuloConf.getWrite().getLatency()), TimeUnit.MILLISECONDS);
        bwConfig.setMaxMemory(getMemoryInBytes(accumuloConf.getWrite().getBufferSize()) / numWriteThreads);
        bwConfig.setMaxWriteThreads(accumuloConf.getWrite().getThreads());

        metaTable = configuration.getMetaTable();
        metricsTable = configuration.getMetricsTable();

        for (String table : new String[] { metricsTable, metaTable }) {
            try {
                tableHelper.createNamespaceFromTableName(table);
            } catch (AccumuloSecurityException e) {
                LOG.error("AccumuloSecurityException while creating namespace: {}", e.getMessage());
                throw new RuntimeException(e);
            } catch (AccumuloException e) {
                LOG.error("AccumuloException while creating namespace: {}", e.getMessage());
                throw new RuntimeException(e);
            }
            try {
                tableHelper.createTableIfNotExists(table);
            } catch (AccumuloSecurityException e) {
                LOG.error("AccumuloSecurityException while creating table: {}", e.getMessage());
                throw new RuntimeException(e);
            } catch (AccumuloException e) {
                LOG.error("AccumuloException while creating table: {}", e.getMessage());
                throw new RuntimeException(e);
            } catch (TableNotFoundException e) {
                LOG.error("TableNotFoundException while creating table: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        }

    }

    public int writeMetric(Metric metric) {
        try {
            getMetricsWriter().addMutation(MetricAdapter.toMutation(metric));
            writeMeta(metric);
            return metric.getTags().size();
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
                final BatchWriter w = connectorProvider.get().createBatchWriter(metricsTable, bwConfig);
                metricsWriter.set(w);
                writers.add(w);
            } catch (TableNotFoundException e1) {
                LOG.error("Unexpected error recreating meta batch writer, shutting down Timely server: {}", e1);
                throw new RuntimeException(e1);
            }
        }
        return 0;
    }

    public int writeMeta(Metric metric) {
        List<Meta> toCache = new ArrayList<>(metric.getTags().size());
        MetaKeySet mks = new MetaKeySet();
        for (final Tag tag : metric.getTags()) {
            Meta key = new Meta(metric.getName(), tag.getKey(), tag.getValue());
            if (!metaCache.contains(key.toString())) {
                toCache.add(key);
            }
        }
        if (!toCache.isEmpty()) {
            toCache.forEach(m -> mks.addAll(MetaAdapter.toKeys(m)));
            try {
                getMetaWriter().addMutations(mks.toMutations());
                metaCache.addAll(toCache);
            } catch (MutationsRejectedException e) {
                LOG.error("Metadata Mutations Rejected: {}", e.toString());
                mks.clear();
            }
        }
        return mks.size();
    }

    private BatchWriter getMetaWriter() {
        if (null == metaWriter.get()) {
            try {
                BatchWriter w = connectorProvider.get().createBatchWriter(metaTable, bwConfig);
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
                BatchWriter w = connectorProvider.get().createBatchWriter(metricsTable, bwConfig);
                metricsWriter.set(w);
                writers.add(w);
            } catch (TableNotFoundException e) {
                // TODO get consensus on what to do here
                LOG.error("Error creating meta batch writer", e);
            }
        }
        return metricsWriter.get();
    }

    public synchronized void flushWriters() {
        writers.forEach(w -> {
            try {
                w.close();
            } catch (final Exception ex) {
                LOG.warn("Error shutting down batchwriter", ex);
            }

        });
    }

}
