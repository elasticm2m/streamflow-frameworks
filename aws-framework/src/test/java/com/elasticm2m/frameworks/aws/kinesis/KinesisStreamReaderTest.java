package com.elasticm2m.frameworks.aws.kinesis;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class KinesisStreamReaderTest extends Assert {

    @Test
    public void testOpenClose() {
        KinesisStreamReader spout = createSpout();
        assertNotNull(spout);
        Map<String, Object> conf = new HashMap<>();
        TopologyContext context = mock(TopologyContext.class);
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        spout.open(conf, context, collector);
        spout.close();
    }

    public static KinesisStreamReader createSpout() {
        KinesisStreamReader result = new KinesisStreamReader();
        result.setLogger(LoggerFactory.getLogger(KinesisStreamReader.class));
        result.setApplicationName("core-framework-unit-test");
        result.setStreamName("pre-process");
        result.setQueueCapacity(50);
        result.setInitialPosition(InitialPositionInStream.TRIM_HORIZON.toString());
        result.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());
        return result;
    }
}
