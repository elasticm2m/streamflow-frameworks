package com.elasticm2m.frameworks.aws.kinesis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.nio.ByteBuffer;
import java.util.Map;

public class KinesisFirehoseWriter extends ElasticBaseRichBolt {

    private AWSCredentialsProvider credentialsProvider;
    private String deliveryStreamName;
    private AmazonKinesisFirehose firehose;
    private boolean logTuple = false;
    private boolean useRecordSeparator = true;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String body = tuple.getString(1);
            if (useRecordSeparator) {
                body = body + "\n";
            }
            Record record = new Record().withData(ByteBuffer.wrap(body.getBytes()));
            PutRecordRequest request = new PutRecordRequest()
                    .withDeliveryStreamName(deliveryStreamName)
                    .withRecord(record);
            firehose.putRecord(request);

            if (logTuple) {
                logger.info(body);
            } else {
                logger.debug("Published record to Kinesis Firehose");
            }

            collector.ack(tuple);
        } catch (Throwable ex) {
            logger.error("Error writing the entity to Kinesis Firehose:", ex);
            collector.fail(tuple);
        }
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(conf, topologyContext, collector);

        logger.info("Kinesis Firehose Writer: Delivery Stream Name = " + deliveryStreamName);

        credentialsProvider = new DefaultAWSCredentialsProviderChain();

        if (credentialsProvider == null) {
            firehose = new AmazonKinesisFirehoseClient();
        } else {
            firehose = new AmazonKinesisFirehoseClient(credentialsProvider);
        }
    }

    @Inject
    public void setDeliveryStreamName(@Named("kinesis-delivery-stream-name") String deliveryStreamName) {
        this.deliveryStreamName = deliveryStreamName;
    }

    @Inject
    public void setLogTuple(@Named("log-tuple") boolean logTuple) {
        this.logTuple = logTuple;
    }

    @Inject
    public void setUseRecordSeparator(@Named("use-record-separator") boolean useRecordSeparator) {
        this.useRecordSeparator = useRecordSeparator;
    }
}
