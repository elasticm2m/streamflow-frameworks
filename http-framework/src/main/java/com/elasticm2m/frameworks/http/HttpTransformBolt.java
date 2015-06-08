package com.elasticm2m.frameworks.http;

import backtype.storm.multilang.BoltMsg;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;
import com.elasticm2m.frameworks.common.protocol.TupleAdapter;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.io.IOUtils;
import org.apache.storm.http.HttpEntity;
import org.apache.storm.http.client.methods.CloseableHttpResponse;
import org.apache.storm.http.client.methods.HttpPost;
import org.apache.storm.http.impl.client.CloseableHttpClient;
import org.apache.storm.http.impl.client.HttpClients;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Random;

public class HttpTransformBolt extends ElasticBaseRichBolt {

    private String endpoint;
    private Random _rand = new Random();
    CloseableHttpClient httpclient;

    @Inject
    public void setEndpoint(@Named("endpoint") String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {
        try {
            super.prepare(conf, topologyContext, collector);
            httpclient = HttpClients.createDefault();
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
            HttpPost httpPost = new HttpPost(endpoint);
            //httpPost.setEntity();
            CloseableHttpResponse response = httpclient.execute(httpPost);
            HttpEntity responseEntity = response.getEntity();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            IOUtils.copy(responseEntity.getContent(), out);

            TupleAdapter adapter = new TupleAdapter<Object>(Object.class);
            adapter.setBody(out);
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