package timely.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;
import timely.adapter.accumulo.MetricAdapter;
import timely.api.model.Meta;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class MetaCache extends AbstractCache<String, Meta> {

    private static final Logger LOG = LoggerFactory.getLogger(MetaCache.class);

    public MetaCache() {

    }

    private List<String> ignoredTags;

    @Override
    public void initialize(Configuration config) {
        LOG.info("Initializing MetaCache");

        long expirationMinutes = config.getMetaCache().getExpirationMinutes();
        int initialCapacity = config.getMetaCache().getInitialCapacity();
        long maxCapacity = config.getMetaCache().getMaxCapacity();

        super.initialize(expirationMinutes, initialCapacity, maxCapacity);

        ignoredTags = config.getMetricsReportIgnoredTags();
        if (!ignoredTags.contains(MetricAdapter.VISIBILITY_TAG)) {
            ignoredTags.add(MetricAdapter.VISIBILITY_TAG);
        }

        LOG.info("MetaCache initialized.");
    }

    public void add(Meta m) {
        this.add(m.toString(), m);
    }

    public void addAll(Collection<Meta> meta) {
        meta.forEach(m -> this.add(m));
    }

    public List<String> getIgnoredTags() {
        return Collections.unmodifiableList(ignoredTags);
    }
}
