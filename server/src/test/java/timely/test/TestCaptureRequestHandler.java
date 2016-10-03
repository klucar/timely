package timely.test;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.request.Request;

public class TestCaptureRequestHandler extends SimpleChannelInboundHandler<Request> {

    private static final Logger LOG = LoggerFactory.getLogger(TestCaptureRequestHandler.class);

    private AtomicLong counter = new AtomicLong(0);
    private List<Request> responses = new ArrayList<>();

    public List<Request> getResponses() {
        return responses;
    }

    public void setResponses(List<Request> responses) {
        this.responses = responses;
    }

    @Override
    public boolean acceptInboundMessage(Object msg) {
        // doing this here instead of channelRead0 allows this to be put in the
        // netty pipeline anywhere.
        responses.add((Request) msg);
        counter.getAndIncrement();
        return false;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Request msg) throws Exception {
        // do nothing
    }

    public long getCount() {
        return counter.get();
    }

}
