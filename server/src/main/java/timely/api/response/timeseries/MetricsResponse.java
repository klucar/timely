package timely.api.response.timeseries;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import timely.api.model.Meta;
import timely.cache.MetaCache;
import timely.netty.Constants;
import timely.util.JsonUtil;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class MetricsResponse {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsResponse.class);

    private static final String DOCTYPE = "<!DOCTYPE html>\n";
    private static final String META = "<meta charset=\"UTF-8\">\n";
    private static final String HTML_START = "<html>\n";
    private static final String HTML_END = "</html>\n";
    private static final String HEAD_START = "<head>\n";
    private static final String HEAD_END = "</head>\n";
    private static final String TITLE = "<title>Timely Metric Information</title>\n";
    private static final String BODY_START = "<body>\n";
    private static final String BODY_END = "</body>\n";
    private static final String HEADER_START = "<header>\n";
    private static final String HEADER_END = "</header>\n";
    private static final String HEADER_CONTENT = "<h2>Timely Metric Information</h2>\n<p>This page represents the metrics that Timely has in its internal cache, which may not be the entire set of available metrics depending on the configuration. The following tags are not shown here but are available for query: ";
    private static final String TABLE_START = "<table>\n";
    private static final String TABLE_END = "</table>\n";
    private static final String TR_START = "<tr>\n";
    private static final String TR_END = "</tr>\n";
    private static final String TH_START = "<th>";
    private static final String TH_END = "</th>\n";
    private static final String TD_START = "<td>";
    private static final String TD_END = "</td>\n";

    MetaCache metaCache;

    public MetricsResponse(MetaCache metaCache) {
        this.metaCache = metaCache;
    }

    public TextWebSocketFrame toWebSocketResponse(String acceptHeader) throws Exception {
        MediaType negotiatedType = MediaType.TEXT_HTML;
        if (null != acceptHeader) {
            List<MediaType> requestedTypes = MediaType.parseMediaTypes(acceptHeader);
            MediaType.sortBySpecificityAndQuality(requestedTypes);
            LOG.trace("Acceptable response types: {}", MediaType.toString(requestedTypes));
            for (MediaType t : requestedTypes) {
                if (t.includes(MediaType.TEXT_HTML)) {
                    negotiatedType = MediaType.TEXT_HTML;
                    LOG.trace("{} allows HTML", t.toString());
                    break;
                }
                if (t.includes(MediaType.APPLICATION_JSON)) {
                    negotiatedType = MediaType.APPLICATION_JSON;
                    LOG.trace("{} allows JSON", t.toString());
                    break;
                }
            }
        }
        String result;
        if (negotiatedType.equals(MediaType.APPLICATION_JSON)) {
            result = this.generateJson();
        } else {
            result = this.generateHtml().toString();
        }
        return new TextWebSocketFrame(result);
    }

    public FullHttpResponse toHttpResponse(String acceptHeader) throws Exception {
        MediaType negotiatedType = MediaType.TEXT_HTML;
        if (null != acceptHeader) {
            List<MediaType> requestedTypes = MediaType.parseMediaTypes(acceptHeader);
            MediaType.sortBySpecificityAndQuality(requestedTypes);
            LOG.trace("Acceptable response types: {}", MediaType.toString(requestedTypes));
            for (MediaType t : requestedTypes) {
                if (t.includes(MediaType.TEXT_HTML)) {
                    negotiatedType = MediaType.TEXT_HTML;
                    LOG.trace("{} allows HTML", t.toString());
                    break;
                }
                if (t.includes(MediaType.APPLICATION_JSON)) {
                    negotiatedType = MediaType.APPLICATION_JSON;
                    LOG.trace("{} allows JSON", t.toString());
                    break;
                }
            }
        }
        byte[] buf;
        Object responseType = Constants.HTML_TYPE;
        if (negotiatedType.equals(MediaType.APPLICATION_JSON)) {
            buf = this.generateJson().getBytes(StandardCharsets.UTF_8);
            responseType = Constants.JSON_TYPE;
        } else {
            buf = this.generateHtml().toString().getBytes(StandardCharsets.UTF_8);
        }
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.copiedBuffer(buf));
        response.headers().set(Names.CONTENT_TYPE, responseType);
        response.headers().set(Names.CONTENT_LENGTH, response.content().readableBytes());
        return response;
    }

    protected StringBuilder generateHtml() {
        // todo use a library for this: templated Apache Freemarker or Code
        // JSoup.
        TreeSet<Meta> tree = new TreeSet<>();
        metaCache.forEach(m -> tree.add(m));
        final StringBuilder b = new StringBuilder();
        b.append(DOCTYPE);
        b.append(HTML_START);
        b.append(HEAD_START);
        b.append(META);
        b.append(TITLE);
        b.append(HEAD_END);
        b.append(HEADER_START);
        b.append(HEADER_CONTENT).append(metaCache.getIgnoredTags().toString()).append("</p>\n");
        b.append(HEADER_END);
        b.append(BODY_START);
        b.append(TABLE_START);
        b.append(TR_START);
        b.append(TH_START).append("Metric").append(TH_END);
        b.append(TH_START).append("Available Tags").append(TH_END);
        b.append(TR_END);
        String prevMetric = null;
        StringBuilder tags = new StringBuilder();
        for (Meta m : tree) {
            if (prevMetric != null && !m.getMetric().equals(prevMetric)) {
                b.append(TR_START);
                b.append(TD_START).append(prevMetric).append(TD_END);
                b.append(TD_START).append(tags.toString()).append(TD_END);
                b.append(TR_END);
                prevMetric = m.getMetric();
                tags.delete(0, tags.length());
            }
            if (prevMetric == null) {
                prevMetric = m.getMetric();
            }
            if (!(metaCache.getIgnoredTags().contains(m.getTagKey()))) {
                tags.append(m.getTagKey()).append("=").append(m.getTagValue()).append(" ");
            }
        }
        b.append(TR_START);
        b.append(TD_START).append(prevMetric).append(TD_END);
        b.append(TD_START).append(tags.toString()).append(TD_END);
        b.append(TR_END);
        b.append(TABLE_END);
        b.append(BODY_END);
        b.append(HTML_END);
        return b;
    }

    protected String generateJson() throws JsonProcessingException {
        // map non-ignored metrics to their list of tags
        // final MetaCache cache = MetaCacheFactory.getCache(conf);
        ObjectMapper mapper = JsonUtil.getObjectMapper();
        Map<String, List<JsonNode>> metricTagMap = new HashMap<>();
        metaCache.forEach(m -> {
            if (!metricTagMap.containsKey(m.getMetric())) {
                metricTagMap.put(m.getMetric(), new ArrayList<>());
            }
            if (!metaCache.getIgnoredTags().contains(m.getTagKey())) {
                metricTagMap.get(m.getMetric()).add(createTag(m, mapper));
            }
        });

        // todo make this a class instead of building by hand here.
        ObjectNode metricsNode = mapper.createObjectNode();
        ArrayNode metricsArray = metricsNode.putArray("metrics");
        metricTagMap.forEach((metric, tags) -> {
            metricsArray.add(createMetric(metric, tags, mapper));
        });

        return mapper.writeValueAsString(metricsNode);
    }

    private static JsonNode createMetric(String metric, List<JsonNode> tags, ObjectMapper mapper) {
        // todo use Metric class here!
        ObjectNode metricNode = mapper.createObjectNode();
        metricNode.put("metric", metric);
        ArrayNode tagsArray = metricNode.putArray("tags");
        tags.forEach(tagsArray::add);
        return metricNode;
    }

    private static JsonNode createTag(Meta meta, ObjectMapper mapper) {
        // todo use Tag class here!
        ObjectNode tagNode = mapper.createObjectNode();
        tagNode.put("key", meta.getTagKey());
        tagNode.put("value", meta.getTagValue());
        return tagNode;
    }

}
