package timely.auth;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;

import timely.Configuration;
import timely.cache.AuthCache;
import timely.cache.AuthenticationCache;
import timely.guice.TimelyModule;
import timely.test.TestConfiguration;

import static org.junit.Assert.assertTrue;

public class AuthCacheTest {

    private static Configuration config;
    private static String cookie = null;

    @Inject
    AuthenticationCache authsCache;

    @BeforeClass
    public static void before() throws Exception {
        config = TestConfiguration.createMinimalConfigurationForTest();
        config.getSecurity().setSessionMaxAge(5000);
        cookie = URLEncoder.encode(UUID.randomUUID().toString(), StandardCharsets.UTF_8.name());

    }

    @Before
    public void setup() throws Exception {
        Injector injector = Guice.createInjector(new TimelyModule(config));
        injector.injectMembers(this);

        // AuthCache.setSessionMaxAge(config);
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("test", "test1");
        Authentication auth = AuthenticationService.getAuthenticationManager().authenticate(token);

        authsCache.add(cookie, auth);

        // AuthCache.getCache().put(cookie, auth);
    }

    @After
    public void tearDown() throws Exception {
        // AuthCache.resetSessionMaxAge();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSessionIdNull() throws Exception {
        Authentication authen = authsCache.get("");
        assertTrue(authen == null);

        authsCache.getAuthorizations(""); // throws IAE
    }

    @Test
    public void testGetAuths() throws Exception {
        Authorizations a = authsCache.getAuthorizations(cookie);
        Assert.assertEquals("A,B,C", a.toString());
    }

}
