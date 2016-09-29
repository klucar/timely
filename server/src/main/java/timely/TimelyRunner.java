package timely;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import timely.guice.TimelyModule;
import timely.guice.TimelyServerProvider;
import timely.validator.TimelyServer;

import java.util.concurrent.CountDownLatch;

/**
 *
 * Generic Runner for TimelyServer implementations
 *
 */
public class TimelyRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TimelyRunner.class);
    private static final CountDownLatch LATCH = new CountDownLatch(1);
    private static ConfigurableApplicationContext applicationContext;

    @Inject
    private TimelyServerProvider serverProvider;

    private TimelyServer server;

    public static void main(String[] args) throws Exception {

        // Process args with spring boot
        Configuration conf = initializeConfiguration(args);

        Injector injector = Guice.createInjector(new TimelyModule(conf));

        TimelyRunner runner = new TimelyRunner();
        injector.injectMembers(runner);
        runner.runServer();
    }

    private void runServer() {
        server = serverProvider.get();
        server.setup();
        try {
            server.run();
            shutdownHook();
            LATCH.await();
            if (applicationContext != null) {
                LOG.info("Closing applicationContext");
                applicationContext.close();
            }
        } catch (final Exception e) {
            LOG.info("Server shutting down.");
        }
    }

    private void shutdownHook() {

        final Runnable shutdownRunner = () -> {
            server.shutdown();
            server = null;
        };
        final Thread hook = new Thread(shutdownRunner, "shutdown-hook-thread");
        Runtime.getRuntime().addShutdownHook(hook);
    }

    protected static Configuration initializeConfiguration(String[] args) {
        applicationContext = new SpringApplicationBuilder(SpringBootstrap.class).web(false).run(args);
        return applicationContext.getBean(Configuration.class);
    }

}
