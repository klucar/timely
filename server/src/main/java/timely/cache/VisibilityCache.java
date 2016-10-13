package timely.cache;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;

public class VisibilityCache extends AbstractCache<String, ColumnVisibility> {

    private static final Logger LOG = LoggerFactory.getLogger(VisibilityCache.class);

    @Override
    public void initialize(Configuration config) {
        LOG.info("Initializing VisibilityCache");

        long expirationMinutes = config.getVisibilityCache().getExpirationMinutes();
        int initialCapacity = config.getVisibilityCache().getInitialCapacity();
        long maxCapacity = config.getVisibilityCache().getMaxCapacity();

        super.initialize(expirationMinutes, initialCapacity, maxCapacity);

        LOG.info("VisibilityCache initialized.");
    }

}
