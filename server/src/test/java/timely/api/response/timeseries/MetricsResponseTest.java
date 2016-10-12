package timely.api.response.timeseries;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import timely.Configuration;
import timely.TestModule;
import timely.api.model.Meta;
import timely.cache.MetaCache;
import timely.test.TestConfiguration;

public class MetricsResponseTest {

    @Inject
    MetaCache metaCache;

    MetricsResponse r;

    @Before
    public void injectMembers() {
        Configuration cfg = TestConfiguration.createMinimalConfigurationForTest();
        Injector injector = Guice.createInjector(new TestModule(cfg));
        injector.injectMembers(this);

        r = new MetricsResponse(metaCache);
    }

    /*
     * public static class TestMetricsResponse extends MetricsResponse {
     * 
     * @Inject Configuration config;
     * 
     * public TestMetricsResponse() { super(); }
     * 
     * @Override public StringBuilder generateHtml() { return
     * super.generateHtml(); }
     * 
     * @Override public String generateJson(ObjectMapper mapper) throws
     * JsonProcessingException { return super.generateJson(mapper); }
     * 
     * }
     */

    @Test
    public void testGenerateHtml() throws Exception {
        Configuration cfg = TestConfiguration.createMinimalConfigurationForTest();
        metaCache.initialize(cfg);
        metaCache.add(new Meta("sys.cpu.user", "host", "localhost"));
        metaCache.add(new Meta("sys.cpu.user", "instance", "0"));
        metaCache.add(new Meta("sys.cpu.idle", "host", "localhost"));
        metaCache.add(new Meta("sys.cpu.idle", "instance", "0"));
        String html = r.generateHtml().toString();
        Assert.assertTrue(html.contains("<td>sys.cpu.idle</td>"));
        Assert.assertTrue(html.contains("<td>host=localhost instance=0 </td>"));
        Assert.assertTrue(html.contains("<td>sys.cpu.user</td>"));
        Assert.assertTrue(html.contains("<td>host=localhost instance=0 </td>"));
    }

    @Test
    public void testGenerateHtmlWithIgnoredTags() throws Exception {
        Configuration cfg = TestConfiguration.createMinimalConfigurationForTest();
        cfg.getMetricsReportIgnoredTags().add("instance");
        metaCache.initialize(cfg);
        metaCache.add(new Meta("sys.cpu.user", "host", "localhost"));
        metaCache.add(new Meta("sys.cpu.user", "instance", "0"));
        metaCache.add(new Meta("sys.cpu.idle", "host", "localhost"));
        metaCache.add(new Meta("sys.cpu.idle", "instance", "0"));
        String html = r.generateHtml().toString();
        Assert.assertTrue(html.contains("<td>sys.cpu.idle</td>"));
        Assert.assertTrue(html.contains("<td>host=localhost </td>"));
        Assert.assertTrue(html.contains("<td>sys.cpu.user</td>"));
        Assert.assertTrue(html.contains("<td>host=localhost </td>"));
    }

}
