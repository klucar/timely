package timely;

import timely.guice.TimelyModule;
import timely.validator.TimelyServer;

/**
 *
 */
public class TestModule extends TimelyModule {

    public TestModule(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected void bindServer() {
        super.bindServer();
        bind(TestServer.class).toInstance(new TestServer());
    }

}
