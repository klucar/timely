package timely.guice;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import org.apache.accumulo.core.client.Connector;
import timely.Configuration;
import timely.Server;
import timely.adapter.accumulo.MetricWriter;
import timely.adapter.accumulo.TableHelper;
import timely.cache.AuthorizationsCache;
import timely.cache.VisibilityCache;
import timely.guice.provider.ChannelClassProvider;
import timely.guice.provider.ConnectorProvider;
import timely.guice.provider.EventLoopGroupProvider;
import timely.guice.provider.ServerSocketChannelClassProvider;
import timely.store.DataStore;
import timely.store.DataStoreImpl;
import timely.cache.MetaCache;
import timely.validator.TimelyServer;

/**
 * Set up injectable objects
 *
 */
public class TimelyModule extends AbstractModule {

    protected final Configuration configuration;

    protected MetaCache metaCache;
    protected AuthorizationsCache authsCache;
    protected VisibilityCache visCache;

    public TimelyModule(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void configure() {
        bind(Configuration.class).toInstance(configuration);
        bind(EventLoopGroup.class).toProvider(new EventLoopGroupProvider());

        TypeLiteral channelType = new TypeLiteral<Class<? extends Channel>>() {
        };
        bind(channelType).toProvider(new ChannelClassProvider());
        TypeLiteral serverSocketChannelType = new TypeLiteral<Class<? extends ServerSocketChannel>>() {
        };
        bind(serverSocketChannelType).toProvider(new ServerSocketChannelClassProvider());

        bind(Connector.class).toProvider(new ConnectorProvider());
        bindServer();
        bindCaches();
        try {
            bind(MetricWriter.class).toConstructor(MetricWriter.class.getConstructor());
            bind(TableHelper.class).toConstructor(TableHelper.class.getConstructor());
            bind(DataStore.class).toConstructor(DataStoreImpl.class.getConstructor());
        } catch (NoSuchMethodException e) {
            addError(e);
        }

    }

    protected void bindServer() {
        bind(TimelyServer.class).toInstance(new Server());
    }

    protected void bindCaches() {
        metaCache = new MetaCache();
        metaCache.initialize(configuration);

        authsCache = new AuthorizationsCache();
        authsCache.initialize(configuration);

        visCache = new VisibilityCache();
        visCache.initialize(configuration);

        bind(MetaCache.class).toInstance(metaCache);
        bind(AuthorizationsCache.class).toInstance(authsCache);
        bind(VisibilityCache.class).toInstance(visCache);

    }
}
