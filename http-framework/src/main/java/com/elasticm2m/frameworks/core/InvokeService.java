package com.elasticm2m.frameworks.core;

import backtype.storm.multilang.BoltMsg;
import backtype.storm.multilang.ShellMsg;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;
import com.elasticm2m.frameworks.common.protocol.TupleAdapter;
import com.elasticm2m.sdk.config.JacksonFeature;
import com.elasticm2m.sdk.config.JerseyClientFactory;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.Map;
import java.util.Random;

public class InvokeService extends ElasticBaseRichBolt {

    private String endpoint;
    private Random _rand = new Random();
    private WebTarget target;

    @Inject
    public void setEndpoint(@Named("endpoint") String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {
        try {
            super.prepare(conf, topologyContext, collector);
            Client client = new JerseyClientFactory().provide();
            client.register(new JacksonFeature());
            target = client.target(endpoint);
        } catch (Throwable e) {
            logger.error("Unable to prepare service", e);
            throw e;
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String genId = Long.toString(_rand.nextLong());
            BoltMsg boltMsg = createBoltMessage(tuple, genId);
            ShellMsg shellMsg = target.request().post(Entity.entity(boltMsg, MediaType.APPLICATION_JSON)).readEntity(ShellMsg.class);
            TupleAdapter adapter = new TupleAdapter<Object>(Object.class);
            adapter.setBody(shellMsg.getTuple().get(0));
            collector.emit(adapter.toTuple());
            collector.ack(tuple);
        } catch (Throwable e) {
            logger.error("Unable to process tuple", e);
            collector.fail(tuple);
        }
    }

    private BoltMsg createBoltMessage(Tuple input, String genId) {
        BoltMsg boltMsg = new BoltMsg();
        boltMsg.setId(genId);
        boltMsg.setComp(input.getSourceComponent());
        boltMsg.setStream(input.getSourceStreamId());
        boltMsg.setTask(input.getSourceTask());
        boltMsg.setTuple(input.getValues());
        return boltMsg;
    }

}