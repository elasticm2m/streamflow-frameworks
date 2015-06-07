package com.elasticm2m.frameworks.aws.kinesis;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class KinesisReaderSpoutTest extends Assert {

    @Test
    public void testOpenClose() {
        KinesisReaderSpout spout = createSpout();
        assertNotNull(spout);
        Map<String, Object> conf = new HashMap<>();
        TopologyContext context = mock(TopologyContext.class);
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        spout.open(conf, context, collector);
        spout.close();
    }

    public static KinesisReaderSpout createSpout() {
        KinesisReaderSpout result = new KinesisReaderSpout();
        result.setLogger(LoggerFactory.getLogger(KinesisReaderSpout.class));
        result.setApplicationName("aws-framework-unit-test");
        result.setStreamName("pre-process");
        result.setInitialPosition(InitialPositionInStream.TRIM_HORIZON.toString());
        result.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());
        return result;
    }
}
