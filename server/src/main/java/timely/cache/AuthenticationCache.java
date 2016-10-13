package timely.cache;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import timely.Configuration;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.Request;
import timely.api.response.TimelyException;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class AuthenticationCache extends AbstractCache<String, Authentication> {

    private static final Logger LOG = LoggerFactory.getLogger(AuthenticationCache.class);

    @Override
    public void initialize(Configuration config) {
        LOG.info("Initializing AuthenticationCache");

        long expirationMinutes = config.getSecurity().getSessionMaxAge();

        super.initialize(expirationMinutes);

        LOG.info("AuthenticationCache initialized");
    }

    public Authorizations getAuthorizations(String sessionId) {
        if (!StringUtils.isEmpty(sessionId)) {
            Authentication auth = this.get(sessionId);
            if (null != auth) {
                Collection<? extends GrantedAuthority> authorities = this.get(sessionId).getAuthorities();
                String[] auths = new String[authorities.size()];
                final AtomicInteger i = new AtomicInteger(0);
                authorities.forEach(a -> auths[i.getAndIncrement()] = a.getAuthority());
                return new Authorizations(auths);
            } else {
                return null;
            }
        } else {
            throw new IllegalArgumentException("session id cannot be null");
        }
    }

    // todo evaluate if this is the right place for this
    public void enforceAccess(Configuration conf, Request r) throws Exception {
        if (!conf.getSecurity().isAllowAnonymousAccess() && (r instanceof AuthenticatedRequest)) {
            AuthenticatedRequest ar = (AuthenticatedRequest) r;
            if (StringUtils.isEmpty(ar.getSessionId())) {
                throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "User must log in",
                        "Anonymous access is disabled, log in first");
            }
            if (!this.contains(ar.getSessionId())) {
                throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "User must log in",
                        "Unknown session id was submitted, log in again");
            }
        }
    }

}
