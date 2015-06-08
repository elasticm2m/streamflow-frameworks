package com.elasticm2m.frameworks.http;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichSpout;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.nkzawa.socketio.client.IO;
import com.github.nkzawa.socketio.client.Socket;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class SocketIoSpout extends ElasticBaseRichSpout {

    private String endpoint;
    private Socket socket;
    private String room;
    private ObjectMapper objectMapper = new ObjectMapper();

    private final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>(10000);

    @Inject
    public void setEndpoint(@Named("endpoint") String endpoint) {
        this.endpoint = endpoint;
    }

    @Inject
    public void setRoom(@Named("room") String room) {
        this.room = room;
    }

    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        try {
            super.open(config, context, collector);
            socket = IO.socket(endpoint);

            socket.on(room, (args) -> {
                queue.add(args[0]);
            });
            socket.connect();
        } catch (Throwable e) {
            logger.error("Unable to prepare service", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        Object event = queue.poll();
        if (event != null) {
            try {
                collector.emit(eventToTuple(event));
            } catch (JsonProcessingException e) {
                logger.error("Error emitting event", e);
            }
        } else {
            Utils.sleep(50);
        }
    }

    public List<Object> eventToTuple(Object event) throws JsonProcessingException {
        logger.debug("event:" + event);
        HashMap<String, String> properties = new HashMap<>();
        List<Object> tuple = new ArrayList<>();
        tuple.add(null);
        tuple.add(objectMapper.writeValueAsString(event));
        tuple.add(properties);
        return tuple;
    }

    @Override
    public void deactivate() {
        socket.close();
    }
}
