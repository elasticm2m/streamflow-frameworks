package com.elasticm2m.frameworks.aws.kinesis;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.Record;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichSpout;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class KinesisReader extends ElasticBaseRichSpout {

    private String applicationName;
    private String streamName;
    private String initialPosition = "LATEST";
    private boolean isReliable = false;
    private AWSCredentialsProvider credentialsProvider;
    private ProcessingService processingService;

    private final LinkedBlockingQueue<Record> queue = new LinkedBlockingQueue<>();

    @Inject
    public void setApplicationName(@Named("kinesis-application-name") String applicationName) {
        this.applicationName = applicationName;
    }

    @Inject
    public void setStreamName(@Named("kinesis-stream-name") String streamName) {
        this.streamName = streamName;
    }

    @Inject(optional=true)
    public void setInitialPosition(@Named("kinesis-initial-position") String initialPosition) {
        this.initialPosition = initialPosition;
    }

    @Inject(optional=true)
    public void setIsReliable(@Named("is-reliable") boolean isReliable) {
        this.isReliable = isReliable;
    }

    /*
    @Inject
    public void setCredentialsProvider(AWSCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }
    */

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector collector) {
        super.open(conf, topologyContext, collector);
        
        logger.info("Kinesis Reader: Stream Name = " + streamName
                + ", Application Name = " + applicationName
                + ", Initial Position = " + initialPosition
                + ", Is Reliable = " + isReliable);
        
        // Use the default credentials provider 
        credentialsProvider = new DefaultAWSCredentialsProviderChain();
        
        processingService = new ProcessingService(
                credentialsProvider, queue, applicationName, streamName,
                InitialPositionInStream.valueOf(initialPosition), logger);
        processingService.startAsync();
    }

    @Override
    public void nextTuple() {
        Record record = queue.poll();
        if (record != null) {
            if (isReliable) {
                collector.emit(recordToTuple(record), record.getSequenceNumber());
            } else {
                collector.emit(recordToTuple(record));
            }
        } else {
            Utils.sleep(50);
        }
    }

    public List<Object> recordToTuple(Record record) {
        HashMap<String, String> properties = new HashMap<>();
        properties.put("sequenceNumber", record.getSequenceNumber());
        properties.put("partitionKey", record.getPartitionKey());

        List<Object> tuple = new ArrayList<>();
        tuple.add(null);
        tuple.add(new String(record.getData().array()));
        tuple.add(properties);
        return tuple;
    }

    @Override
    public void close() {
        processingService.stopAsync();
        processingService.awaitTerminated();
        
        logger.info("Kinesis Reader Spout Stopped");
    }
}
