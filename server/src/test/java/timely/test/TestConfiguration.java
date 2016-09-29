package timely.test;

import java.util.HashMap;

import timely.Configuration;

public class TestConfiguration {

    public static final String TIMELY_HTTP_ADDRESS_DEFAULT = "localhost";

    public static final int WAIT_SECONDS = 2;

    public static Configuration createMinimalConfigurationForTest() {
        // @formatter:off
        Configuration cfg = new Configuration()
                .getServer().setIp("127.0.0.1")
                .getServer().setTcpPort(54321)
                .getServer().setUdpPort(54325)
                .getServer().setShutdownQuietPeriod(0)
                .getHttp().setIp("127.0.0.1")
                .getHttp().setPort(54322)
                .getHttp().setHost("localhost")
                .getWebsocket().setIp("127.0.0.1")
                .getWebsocket().setPort(54323)
                .getWebsocket().setTimeout(20)
                .getAccumulo().setStandalone(true)
                .getAccumulo().setZookeepers("localhost:9804")  // todo have mac config parse the port from here
                .getAccumulo().setInstanceName("test")
                .getAccumulo().setUsername("root")
                .getAccumulo().setPassword("secret")
                .getAccumulo().getWrite().setLatency("100ms")
                .getSecurity().getSsl().setUseGeneratedKeypair(true);
        HashMap<String,Integer> ageoff = new HashMap<>();
        ageoff.put("default", 10);
        cfg.setMetricAgeOffDays(ageoff);
        // @formatter:on

        return cfg;
    }

}
