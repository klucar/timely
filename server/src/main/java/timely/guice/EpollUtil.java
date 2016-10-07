package timely.guice;

import io.netty.util.internal.SystemPropertyUtil;

/**
 *
 */
public class EpollUtil {

    private static final int EPOLL_MIN_MAJOR_VERSION = 2;
    private static final int EPOLL_MIN_MINOR_VERSION = 6;
    private static final int EPOLL_MIN_PATCH_VERSION = 32;
    private static final String OS_NAME = "os.name";
    private static final String OS_VERSION = "os.version";

    private static final boolean useEpoll = setUseEpoll();

    public static boolean useEpoll() {
        return useEpoll;
    }

    private static boolean setUseEpoll() {

        // Should we just return true if this is Linux and if we get an error
        // during Epoll
        // setup handle it there?
        final String os = SystemPropertyUtil.get(OS_NAME).toLowerCase().trim();
        final String[] version = SystemPropertyUtil.get(OS_VERSION).toLowerCase().trim().split("\\.");
        if (os.startsWith("linux") && version.length >= 3) {
            final int major = Integer.parseInt(version[0]);
            if (major > EPOLL_MIN_MAJOR_VERSION) {
                return true;
            } else if (major == EPOLL_MIN_MAJOR_VERSION) {
                final int minor = Integer.parseInt(version[1]);
                if (minor > EPOLL_MIN_MINOR_VERSION) {
                    return true;
                } else if (minor == EPOLL_MIN_MINOR_VERSION) {
                    final int patch = Integer.parseInt(version[2].substring(0, 2));
                    return patch >= EPOLL_MIN_PATCH_VERSION;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

}
