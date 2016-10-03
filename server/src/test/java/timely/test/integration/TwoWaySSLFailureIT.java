package timely.test.integration;

import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.ssl.*;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import timely.netty.Constants;

import javax.net.ssl.*;
import java.io.File;
import java.net.URL;
import java.util.List;

@SuppressWarnings("deprecation")
public class TwoWaySSLFailureIT extends QueryBase {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    private static SelfSignedCertificate serverCert = null;
    private static File clientTrustStoreFile = null;

    static {
        try {
            // This fqdn does not match what is in security.xml
            serverCert = new SelfSignedCertificate("bad.example.com");
            clientTrustStoreFile = serverCert.certificate().getAbsoluteFile();
        } catch (Exception e) {
            throw new RuntimeException("Error creating self signed certificate", e);
        }
    }

    protected SSLSocketFactory getSSLSocketFactory() throws Exception {
        SslContextBuilder builder = SslContextBuilder.forClient();
        builder.applicationProtocolConfig(ApplicationProtocolConfig.DISABLED);
        // Use server cert / key on client side
        builder.keyManager(serverCert.key(), (String) null, serverCert.cert());
        builder.sslProvider(SslProvider.JDK);
        builder.trustManager(clientTrustStoreFile); // Trust the server cert
        SslContext ctx = builder.build();
        Assert.assertEquals(JdkSslClientContext.class, ctx.getClass());
        JdkSslContext jdk = (JdkSslContext) ctx;
        SSLContext jdkSslContext = jdk.context();
        return jdkSslContext.getSocketFactory();
    }

    protected HttpsURLConnection getUrlConnection(URL url) throws Exception {
        // Username and password not used in 2way SSL case
        return getUrlConnection(null, null, url);
    }

    protected HttpsURLConnection getUrlConnection(String username, String password, URL url) throws Exception {
        HttpsURLConnection.setDefaultSSLSocketFactory(getSSLSocketFactory());
        URL loginURL = new URL(url.getProtocol() + "://" + url.getHost() + ":" + url.getPort() + "/login");
        HttpsURLConnection con = (HttpsURLConnection) loginURL.openConnection();
        con.setHostnameVerifier(new HostnameVerifier() {

            @Override
            public boolean verify(String arg0, SSLSession arg1) {
                return true;
            }
        });
        con.setRequestMethod("GET");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/json");
        con.connect();
        int responseCode = con.getResponseCode();
        if (401 == responseCode) {
            throw new UnauthorizedUserException();
        }
        Assert.assertEquals(200, responseCode);
        List<String> cookies = con.getHeaderFields().get(Names.SET_COOKIE);
        Assert.assertEquals(1, cookies.size());
        Cookie sessionCookie = ClientCookieDecoder.STRICT.decode(cookies.get(0));
        Assert.assertEquals(Constants.COOKIE_NAME, sessionCookie.name());
        con = (HttpsURLConnection) url.openConnection();
        con.setRequestProperty(Names.COOKIE, sessionCookie.name() + "=" + sessionCookie.value());
        con.setHostnameVerifier((arg0, arg1) -> true);
        return con;
    }

    @Before
    public void setupSsl() throws Exception {
        conf.getSecurity().getSsl().setCertificateFile(serverCert.certificate().getAbsolutePath());
        conf.getSecurity().getSsl().setKeyFile(serverCert.privateKey().getAbsolutePath());
        // Needed for 2way SSL
        conf.getSecurity().getSsl().setTrustStoreFile(serverCert.certificate().getAbsolutePath());
        conf.getSecurity().getSsl().setUseOpenssl(false);
        conf.getSecurity().getSsl().setUseGeneratedKeypair(false);
        conf.getSecurity().setAllowAnonymousAccess(false);
        startTimelyServer();
    }

    @Test(expected = UnauthorizedUserException.class)
    public void testBasicAuthLoginFailure() throws Exception {
        String metrics = "https://localhost:54322/api/metrics";
        query(metrics);
    }

}
