package timely.netty.websocket.timeseries;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import timely.api.request.timeseries.MetricsRequest;
import timely.api.response.timeseries.MetricsResponse;

public class WSMetricsRequestHandler extends SimpleChannelInboundHandler<MetricsRequest> {

    public WSMetricsRequestHandler() {

    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MetricsRequest m) throws Exception {
        MetricsResponse r = new MetricsResponse();
        ctx.writeAndFlush(r.toWebSocketResponse("application/json"));
    }

}
