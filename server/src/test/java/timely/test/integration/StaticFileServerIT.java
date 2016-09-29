package timely.test.integration;

import org.junit.*;
import org.junit.experimental.categories.Category;

import timely.test.IntegrationTest;
import timely.validator.TimelyServer;

@Category(IntegrationTest.class)
public class StaticFileServerIT extends OneWaySSLBase {

    private TimelyServer server;

    @Before
    public void before() throws Exception {
        server = getRunningServer();
    }

    @After
    public void after() throws Exception {
        server.shutdown();
    }

    @Test(expected = NotSuccessfulException.class)
    public void testGetFavIconRequest() throws Exception {
        query("https://127.0.0.1:54322/favicon.ico", 404, "application/json");
    }

    @Test(expected = NotSuccessfulException.class)
    public void testGetBadPath() throws Exception {
        query("https://127.0.0.1:54322/index.html", 403, "application/json");
    }

    @Test(expected = NotSuccessfulException.class)
    public void testGetGoodPath() throws Exception {
        query("https://127.0.0.1:54322/webapp/test.html", 404, "application/json");
    }

}
