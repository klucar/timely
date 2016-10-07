package timely.guice.provider;

import com.google.inject.Provider;
import io.netty.channel.Channel;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import timely.guice.EpollUtil;

/**
 *
 */
public class ChannelClassProvider implements Provider<Class<? extends Channel>> {

    @Override
    public Class<? extends Channel> get() {
        if (EpollUtil.useEpoll()) {
            return EpollDatagramChannel.class;
        } else {
            return NioSocketChannel.class;
        }
    }
}
