package timely;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.cache.AuthenticationCache;
import timely.netty.tcp.MetricsBufferDecoder;
import timely.netty.tcp.TcpDecoder;
import timely.netty.tcp.TcpPutHandler;
import timely.netty.tcp.TcpVersionHandler;
import timely.netty.udp.UdpDecoder;
import timely.netty.udp.UdpPacketToByteBuf;
import timely.test.TestCaptureRequestHandler;

public class TestServer extends Server {

    private static final Logger LOG = LoggerFactory.getLogger(TestServer.class);

    private final TestCaptureRequestHandler tcpRequests = new TestCaptureRequestHandler();
    private final TestCaptureRequestHandler httpRequests = new TestCaptureRequestHandler();
    private final TestCaptureRequestHandler udpRequests = new TestCaptureRequestHandler();

    // private static StandaloneServer standaloneServer;

    public TestServer() {
        super();
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    @Override
    public void setup() {
        super.setup();
        StandaloneServer.startMiniAccumulo(config);
    }

    @Override
    protected ChannelHandler setupHttpChannel(SslContext sslCtx, AuthenticationCache authenticationCache) {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                LOG.info("Setting up Test Server Http Channel");
                ch.pipeline().addLast("ssl", sslCtx.newHandler(ch.alloc()));
                ch.pipeline().addLast("decompressor", new HttpContentDecompressor());
                ch.pipeline().addLast("decoder", new HttpRequestDecoder());
                ch.pipeline().addLast("aggregator", new HttpObjectAggregator(8192));
                ch.pipeline().addLast("queryDecoder",
                        new timely.netty.http.HttpRequestDecoder(config, authenticationCache));
                ch.pipeline().addLast("capture", httpRequests);
            }
        };
    }

    @Override
    protected ChannelHandler setupTcpChannel() {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                LOG.info("Setting up Test Server Tcp Channel");

                ch.pipeline().addLast("buffer", new MetricsBufferDecoder());
                ch.pipeline().addLast("frame", new DelimiterBasedFrameDecoder(8192, true, Delimiters.lineDelimiter()));
                ch.pipeline().addLast("putDecoder", new TcpDecoder());
                ch.pipeline().addLast("capture", tcpRequests);
                ch.pipeline().addLast("putHandler", new TcpPutHandler(dataStore));
                ch.pipeline().addLast("versionHandler", new TcpVersionHandler());
            }
        };
    }

    @Override
    protected ChannelHandler setupUdpChannel() {
        return new ChannelInitializer<DatagramChannel>() {

            @Override
            protected void initChannel(DatagramChannel ch) throws Exception {
                LOG.info("Setting up Test Server Udp Channel");

                ch.pipeline().addLast("logger", new LoggingHandler());
                ch.pipeline().addLast("packetDecoder", new UdpPacketToByteBuf());
                ch.pipeline().addLast("buffer", new MetricsBufferDecoder());
                ch.pipeline().addLast("frame", new DelimiterBasedFrameDecoder(8192, true, Delimiters.lineDelimiter()));
                ch.pipeline().addLast("putDecoder", new UdpDecoder());
                ch.pipeline().addLast("capture", udpRequests);
            }
        };
    }

    public TestCaptureRequestHandler getTcpRequests() {
        return tcpRequests;
    }

    public TestCaptureRequestHandler getHttpRequests() {
        return httpRequests;
    }

    public TestCaptureRequestHandler getUdpRequests() {
        return udpRequests;
    }

}
