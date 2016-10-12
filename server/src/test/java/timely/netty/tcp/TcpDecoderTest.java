package timely.netty.tcp;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import timely.Configuration;
import timely.TestModule;
import timely.adapter.accumulo.MetricAdapter;
import timely.api.request.MetricRequest;
import timely.model.Metric;
import timely.model.Tag;
import timely.api.request.VersionRequest;
import timely.cache.VisibilityCache;
import timely.test.TestConfiguration;

import static org.junit.Assert.assertTrue;

public class TcpDecoderTest {

    private static final Long TEST_TIME = System.currentTimeMillis();

    @Before
    public void setup() {
        Configuration cfg = TestConfiguration.createMinimalConfigurationForTest();
        Injector injector = Guice.createInjector(new TestModule(cfg));
        injector.injectMembers(this);
    }

    @Test
    public void testPutNoViz() throws Exception {
        TcpDecoder decoder = new TcpDecoder();
        List<Object> results = new ArrayList<>();
        String put = "put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2";
        ByteBuf buf = Unpooled.wrappedBuffer(put.getBytes());
        decoder.decode(null, buf, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(MetricRequest.class, results.get(0).getClass());
        Metric m = ((MetricRequest) results.get(0)).getMetric();
        // @formatter:off
        Metric expected = Metric.newBuilder()
                .name("sys.cpu.user")
                .value(TEST_TIME, 1.0D)
                .tag(new Tag("tag1", "value1"))
                .tag(new Tag("tag2", "value2"))
                .build();
        // @formatter:on

        assertTrue(expected.equals(m));
    }

    @Test
    public void testPutWithViz() throws Exception {
        TcpDecoder decoder = new TcpDecoder();
        List<Object> results = new ArrayList<>();
        String put = "put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 viz=a&b tag2=value2";
        ByteBuf buf = Unpooled.wrappedBuffer(put.getBytes());
        decoder.decode(null, buf, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(MetricRequest.class, results.get(0).getClass());
        Metric m = ((MetricRequest) results.get(0)).getMetric();
        // @formatter:off
        Metric expected = Metric.newBuilder()
                .name("sys.cpu.user")
                .value(TEST_TIME, 1.0D)
                .tag(new Tag("tag1", "value1")).tag(new Tag("tag2", "value2"))
                .tag(MetricAdapter.VISIBILITY_TAG, "a&b")
                .build();
        // @formatter:on
        assertTrue(expected.equals(m));
    }

    @Test
    public void testVersion() throws Exception {
        TcpDecoder decoder = new TcpDecoder();
        List<Object> results = new ArrayList<>();
        String put = "version";
        ByteBuf buf = Unpooled.wrappedBuffer(put.getBytes());
        decoder.decode(null, buf, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(VersionRequest.class, results.get(0).getClass());
    }

    @Test
    public void testUnknownOperation() throws Exception {
        TcpDecoder decoder = new TcpDecoder();
        List<Object> results = new ArrayList<>();
        String put = "foo";
        ByteBuf buf = Unpooled.wrappedBuffer(put.getBytes());
        decoder.decode(null, buf, results);
        Assert.assertEquals(0, results.size());
    }

}
