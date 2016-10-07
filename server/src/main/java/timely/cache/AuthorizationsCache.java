package timely.cache;

import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;

/**
 *
 */
public class AuthorizationsCache extends AbstractCache<Authorizations> {

    private static final Logger LOG = LoggerFactory.getLogger(AuthorizationsCache.class);

    @Override
    public void initialize(Configuration config) {
        LOG.info("Initializing AuthorizationsCache");

        long expirationMinutes = config.getSecurity().getSessionMaxAge();

        super.initialize(expirationMinutes);

        LOG.info("AuthorizationsCache initialized");
    }

}
