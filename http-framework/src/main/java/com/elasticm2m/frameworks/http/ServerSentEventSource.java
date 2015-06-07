package com.elasticm2m.frameworks.http;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichSpout;
import com.elasticm2m.sdk.config.JacksonFeature;
import com.elasticm2m.sdk.config.JerseyClientFactory;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.glassfish.jersey.media.sse.EventSource;
import org.glassfish.jersey.media.sse.InboundEvent;
import org.glassfish.jersey.media.sse.SseFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class ServerSentEventSource extends ElasticBaseRichSpout {

    private String endpoint;
    private EventSource eventSource;

    private final LinkedBlockingQueue<InboundEvent> queue = new LinkedBlockingQueue<>(10000);

    @Inject
    public void setEndpoint(@Named("endpoint") String endpoint) {
        this.endpoint = endpoint;
    }

    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        try {
            super.open(config, context, collector);

            Client client = new JerseyClientFactory().provide();
            client.register(JacksonFeature.class).register(SseFeature.class);
            WebTarget target = client.target(endpoint);
            eventSource = EventSource.target(target).build();
            eventSource.register(inboundEvent -> queue.add(inboundEvent));
            eventSource.open();

        } catch (Throwable e) {
            logger.error("Unable to prepare service", e);
            throw e;
        }
    }

    @Override
    public void nextTuple() {
        InboundEvent event = queue.poll();
        if (event != null) {
            collector.emit(eventToTuple(event));
        } else {
            Utils.sleep(50);
        }
    }

    public List<Object> eventToTuple(InboundEvent event) {
        HashMap<String, String> properties = new HashMap<>();
        properties.put("id", event.getId());
        properties.put("name", event.getName());
        List<Object> tuple = new ArrayList<>();
        tuple.add(null);
        tuple.add(event.getRawData());
        tuple.add(properties);
        return tuple;
    }

    @Override
    public void deactivate() {
        eventSource.close();
    }
}
