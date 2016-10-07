package timely.util;

import org.apache.accumulo.core.security.ColumnVisibility;

/**
 *
 */
public class VisibilityUtil {

    public static final ColumnVisibility generateNormalizedVisibility(String visibilityString) {
        // it would be nice if Accumulo would make it easier to do this.
        return new ColumnVisibility(new ColumnVisibility(visibilityString).flatten());
    }
}
