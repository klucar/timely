package timely.guice.provider;

import com.google.inject.Provider;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.guice.EpollUtil;

/**
 *
 */
public class EventLoopGroupProvider implements Provider<EventLoopGroup> {

    private static final Logger LOG = LoggerFactory.getLogger(EventLoopGroupProvider.class);

    // test cases created too many open file errors when creating these new all
    // the time.
    // perhaps the groups aren't being shut down in all the test cases
    // todo check event loop group shutdown in tests
    // then, I found this. https://github.com/netty/netty/issues/639
    private EventLoopGroup epollEventLoopGroup = null;
    private EventLoopGroup nioEventLoopGroup = null;

    @Override
    public final EventLoopGroup get() {
        if (EpollUtil.useEpoll()) {
            if (null == epollEventLoopGroup) {
                LOG.info("Creating EpollEventLoopGroup");
                epollEventLoopGroup = new EpollEventLoopGroup();
            }
            return epollEventLoopGroup;
        } else {
            if (null == nioEventLoopGroup) {
                LOG.info("Creating NioEventLoopGroup");
                nioEventLoopGroup = new NioEventLoopGroup();
            }
            return nioEventLoopGroup;
        }
    }

}
