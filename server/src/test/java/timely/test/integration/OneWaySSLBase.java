package timely.test.integration;

import io.netty.handler.ssl.*;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import timely.Configuration;
import timely.auth.AuthCache;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.File;
import java.net.URL;

/**
 * Base test class for SSL with anonymous access
 */
@SuppressWarnings("deprecation")
public class OneWaySSLBase extends QueryBase {

    protected static File clientTrustStoreFile = null;

    protected SSLSocketFactory getSSLSocketFactory() throws Exception {
        SslContextBuilder builder = SslContextBuilder.forClient();
        builder.applicationProtocolConfig(ApplicationProtocolConfig.DISABLED);
        builder.sslProvider(SslProvider.JDK);
        builder.trustManager(clientTrustStoreFile); // Trust the server cert
        SslContext ctx = builder.build();
        Assert.assertEquals(JdkSslClientContext.class, ctx.getClass());
        JdkSslContext jdk = (JdkSslContext) ctx;
        SSLContext jdkSslContext = jdk.context();
        return jdkSslContext.getSocketFactory();
    }

    @Before
    public void setupSSL() throws Exception {
        SelfSignedCertificate serverCert = new SelfSignedCertificate();
        conf.getSecurity().getSsl().setCertificateFile(serverCert.certificate().getAbsolutePath());
        clientTrustStoreFile = serverCert.certificate().getAbsoluteFile();
        conf.getSecurity().getSsl().setKeyFile(serverCert.privateKey().getAbsolutePath());
        conf.getSecurity().getSsl().setUseOpenssl(false);
        conf.getSecurity().getSsl().setUseGeneratedKeypair(false);
        conf.getSecurity().setAllowAnonymousAccess(true);
    }

    @Override
    protected HttpsURLConnection getUrlConnection(String username, String password, URL url) throws Exception {
        // No username/password needed for anonymous access
        return getUrlConnection(url);
    }

    protected HttpsURLConnection getUrlConnection(URL url) throws Exception {
        HttpsURLConnection.setDefaultSSLSocketFactory(getSSLSocketFactory());
        HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
        con.setHostnameVerifier((host, session) -> true);
        return con;
    }

    @After
    public void tearDownAuthCache() throws Exception {
        AuthCache.resetSessionMaxAge();
    }

}
