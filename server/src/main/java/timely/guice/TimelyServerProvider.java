package timely.guice;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import timely.Configuration;
import timely.Server;
import timely.StandaloneServer;
import timely.validator.TimelyServer;

/**
 *
 *
 */
public class TimelyServerProvider implements Provider<TimelyServer> {

    @Inject
    private Configuration configuration;

    private TimelyServer standaloneServer = null;
    private TimelyServer fullServer = null;
    private Injector injector = null;


    public TimelyServerProvider(){
    }

    @Override
    public TimelyServer get() {
        if( null == injector){
            injector = Guice.createInjector(new TimelyModule(configuration));
        }
        if (configuration.getAccumulo().isStandalone()) {
            if( null == standaloneServer ) {
                standaloneServer = new StandaloneServer();
            }
            injector.injectMembers(standaloneServer);
            return standaloneServer;
        } else {
            if( null == fullServer ) {
                fullServer = new Server();
            }
            injector.injectMembers(fullServer);
            return fullServer;
        }
    }
}
