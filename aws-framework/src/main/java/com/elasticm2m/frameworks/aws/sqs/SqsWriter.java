package com.elasticm2m.frameworks.aws.sqs;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.codec.binary.Base64;

public class SqsWriter extends ElasticBaseRichBolt {
    
    private AmazonSQS sqs;
    private AWSCredentialsProvider credentialsProvider;
    private String queueUrl;

    @Inject
    public void setQueueUrl(@Named("sqs-queue-url") String queueUrl) {
        this.queueUrl = queueUrl;
    }
    
    /*
    @Inject(optional=true)
    public void setCredentialsProvider(AWSCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }
    */

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(conf, topologyContext, collector);
        
        logger.info("SQS Writer: Queue URL = " + queueUrl);
        
        credentialsProvider = new DefaultAWSCredentialsProviderChain();
        
        if (credentialsProvider == null) {
            sqs = new AmazonSQSAsyncClient();
        } else {
            sqs = new AmazonSQSClient(credentialsProvider);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Map<String, String> tupleAttributes = (Map<String, String>) tuple.getValue(2);
            Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();

            for (Entry<String, String> tupleAttribute : tupleAttributes.entrySet()) {
                MessageAttributeValue attributeValue = new MessageAttributeValue();
                attributeValue.setStringValue(tupleAttribute.getValue());
                attributeValue.setDataType("String");

                messageAttributes.put(tupleAttribute.getKey(), attributeValue);
            }

            SendMessageRequest request = new SendMessageRequest()
                    .withQueueUrl(queueUrl)
                    .withMessageAttributes(messageAttributes)
                    .withMessageBody(new String(Base64.encodeBase64(tuple.getString(1).getBytes())));
            sqs.sendMessage(request);
            
            collector.ack(tuple);
        } catch (Exception ex) {
            logger.error("Error writing the entity to SQS:", ex);
            collector.fail(tuple);
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
}
