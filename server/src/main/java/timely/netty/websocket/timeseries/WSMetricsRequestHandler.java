package timely.netty.websocket.timeseries;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import timely.api.request.timeseries.MetricsRequest;
import timely.api.response.timeseries.MetricsResponse;
import timely.cache.MetaCache;

public class WSMetricsRequestHandler extends SimpleChannelInboundHandler<MetricsRequest> {

    MetaCache metaCache;

    public WSMetricsRequestHandler(MetaCache metaCache) {
        this.metaCache = metaCache;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MetricsRequest m) throws Exception {
        MetricsResponse r = new MetricsResponse(metaCache);
        ctx.writeAndFlush(r.toWebSocketResponse("application/json"));
    }

}
