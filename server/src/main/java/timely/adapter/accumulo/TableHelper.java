package timely.adapter.accumulo;

import com.google.inject.Inject;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;
import timely.guice.ConnectorProvider;
import timely.store.MetricAgeOffFilter;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class TableHelper {

    private static final Logger LOG = LoggerFactory.getLogger(TableHelper.class);
    private static final EnumSet<IteratorUtil.IteratorScope> AGEOFF_SCOPES = EnumSet
            .allOf(IteratorUtil.IteratorScope.class);

    @Inject
    Configuration configuration;

    @Inject
    ConnectorProvider connectorProvider;


    public TableHelper() { }

    private boolean tableExists(String table) {
        final Map<String, String> tableIdMap = connectorProvider.get().tableOperations().tableIdMap();
        return tableIdMap.containsKey(table);
    }

    public void createTableIfNotExists(String table) throws AccumuloSecurityException, AccumuloException,
            TableNotFoundException {

        if (!tableExists(table)) {
            try {
                LOG.info("Creating table " + table);
                connectorProvider.get().tableOperations().create(table);
                configureTableAgeoff(table);
            } catch (final TableExistsException ex) {
                // don't care
            }
        }
    }

    public void createNamespaceFromTableName(String table) throws AccumuloSecurityException, AccumuloException {
        if (table.contains(".")) {
            final String[] parts = table.split("\\.", 2);
            final String namespace = parts[0];
            if (!connectorProvider.get().namespaceOperations().exists(namespace)) {
                try {
                    LOG.info("Creating namespace " + namespace);
                    connectorProvider.get().namespaceOperations().create(namespace);
                } catch (final NamespaceExistsException ex) {
                    // don't care
                }
            }
        }

    }

    public void configureTableAgeoff(String table) throws AccumuloSecurityException, AccumuloException,
            TableNotFoundException {
        this.removeAgeOffIterators(table);
        this.applyAgeOffIterator(table);
    }

    private void removeAgeOffIterators(String tableName) throws AccumuloSecurityException, AccumuloException,
            TableNotFoundException {
        Map<String, EnumSet<IteratorUtil.IteratorScope>> iters = connectorProvider.get().tableOperations().listIterators(tableName);
        for (String name : iters.keySet()) {
            if (name.startsWith("ageoff")) {
                connectorProvider.get().tableOperations().removeIterator(tableName, name, AGEOFF_SCOPES);
            }
        }
    }

    private void applyAgeOffIterator(String tableName) throws AccumuloSecurityException, AccumuloException,
            TableNotFoundException {
        int priority = 100;
        Map<String, String> ageOffOptions = new HashMap<>();
        for (Map.Entry<String, Integer> e : configuration.getMetricAgeOffDays().entrySet()) {
            String ageoff = Long.toString(e.getValue() * 86400000L);
            ageOffOptions.put(MetricAgeOffFilter.AGE_OFF_PREFIX + e.getKey(), ageoff);
        }
        IteratorSetting ageOffIteratorSettings = new IteratorSetting(priority, "ageoff", MetricAgeOffFilter.class,
                ageOffOptions);
        connectorProvider.get().tableOperations().attachIterator(tableName, ageOffIteratorSettings, AGEOFF_SCOPES);
    }

}
