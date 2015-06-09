package com.elasticm2m.frameworks.http;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;
import com.elasticm2m.frameworks.common.protocol.TupleAdapter;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Random;

public class HttpTransformBolt extends ElasticBaseRichBolt {

    private String endpoint;
    private Random _rand = new Random();
    CloseableHttpClient httpclient;
    private String contentType = "application/json";

    @Inject
    public void setEndpoint(@Named("endpoint") String endpoint) {
        this.endpoint = endpoint;
    }

    @Inject
    public void setContentType(@Named("content-type") String contentType) {
        this.contentType = contentType;
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
            String body = tuple.getString(1);
            HttpPost httpPost = new HttpPost(endpoint);
            httpPost.setHeader("Content-Type", contentType);
            httpPost.setEntity(new ByteArrayEntity(body.getBytes()));
            CloseableHttpResponse response = httpclient.execute(httpPost);

            HttpEntity responseEntity = response.getEntity();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            IOUtils.copy(responseEntity.getContent(), out);

            TupleAdapter adapter = new TupleAdapter<String>(String.class);
            adapter.setBody(new String(out.toString()));
            collector.emit(adapter.toTuple());
            collector.ack(tuple);
        } catch (Throwable e) {
            logger.error("Unable to process tuple", e);
            collector.fail(tuple);
        }
    }

}