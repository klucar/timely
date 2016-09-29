package timely.guice;

import com.google.inject.AbstractModule;
import org.apache.accumulo.core.client.Connector;
import timely.Configuration;
import timely.adapter.accumulo.MetricWriter;
import timely.adapter.accumulo.TableHelper;
import timely.store.DataStore;
import timely.store.DataStoreImpl;
import timely.validator.TimelyServer;

/**
 *
 *
 */
public class TimelyModule extends AbstractModule {

    private final Configuration configuration;

    public TimelyModule(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void configure() {
        bind(Configuration.class).toInstance(configuration);
        bind(TimelyServer.class).toProvider(new TimelyServerProvider());
        bind(Connector.class).toProvider(new ConnectorProvider());

        try {
            bind(MetricWriter.class).toConstructor(MetricWriter.class.getConstructor());
            bind(TableHelper.class).toConstructor(TableHelper.class.getConstructor());
            bind(DataStore.class).toConstructor(DataStoreImpl.class.getConstructor());
        } catch (NoSuchMethodException e) {
            addError(e);
        }

    }

}
