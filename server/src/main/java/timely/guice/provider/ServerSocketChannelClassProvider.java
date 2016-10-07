package timely.guice.provider;

import com.google.inject.Provider;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import timely.guice.EpollUtil;

/**
 *
 */
public class ServerSocketChannelClassProvider implements Provider<Class<? extends ServerSocketChannel>> {

    @Override
    public Class<? extends ServerSocketChannel> get() {
        if (EpollUtil.useEpoll()) {
            return EpollServerSocketChannel.class;
        } else {
            return NioServerSocketChannel.class;
        }
    }

}
