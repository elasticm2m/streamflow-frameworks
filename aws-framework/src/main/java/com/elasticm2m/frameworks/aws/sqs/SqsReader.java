package com.elasticm2m.frameworks.aws.sqs;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichSpout;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.codec.binary.Base64;

public class SqsReader extends ElasticBaseRichSpout {

    private AmazonSQS sqs;
    private AWSCredentialsProvider credentialsProvider;
    private String queueUrl;
    private boolean isReliable;
    
    private final LinkedBlockingQueue<Message> queue = new LinkedBlockingQueue<>();

    @Inject
    public void setQueueUrl(@Named("sqs-queue-url") String queueUrl) {
        this.queueUrl = queueUrl;
    }

    @Inject
    public void setIsReliable(@Named("sqs-reliable") boolean isReliable) {
        this.isReliable = isReliable;
    }

    /*
    @Inject(optional=true)
    public void setCredentialsProvider(AWSCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }
    */

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector collector) {
        super.open(conf, topologyContext, collector);
        
        logger.info("SQS Reader: SQS Queue URL = " + queueUrl + ", Is Reliable = " + isReliable);
        
        credentialsProvider = new DefaultAWSCredentialsProviderChain();
        
        if (credentialsProvider == null) {
            sqs = new AmazonSQSAsyncClient();
        } else {
            sqs = new AmazonSQSClient(credentialsProvider);
        }
    }

    @Override
    public void nextTuple() {
        if (queue.isEmpty()) {
            ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(
                    new ReceiveMessageRequest(queueUrl)
                    .withMaxNumberOfMessages(10).withWaitTimeSeconds(10));
            queue.addAll(receiveMessageResult.getMessages());
        }

        Message message = queue.poll();
        if (message != null) {
            if (isReliable) {
                collector.emit(messageToTuple(message), message.getReceiptHandle());
            } else {
                // Delete it right away
                sqs.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));

                collector.emit(messageToTuple(message));
            }
        } else {
            // Still empty, go to sleep.
            Utils.sleep(50);
        }
    }
    
    private List<Object> messageToTuple(Message message) {
        Values values = new Values();
        values.add(message.getMessageId());
        try {
            values.add(new String(Base64.decodeBase64(message.getBody().getBytes())));
        } catch (Exception ex) {
            logger.error("Exception while decoding the Base64 message body", ex);
        }
        values.add(message.getAttributes());
        
        return values;
    }

    @Override
    public void ack(Object messageId) {
        // Only called in reliable mode.
        try {
            sqs.deleteMessage(new DeleteMessageRequest(queueUrl, (String) messageId));
        } catch (AmazonClientException ex) {
            logger.error("Exception while acking message: " + messageId, ex);
        }
    }

    @Override
    public void fail(Object messageId) {
        // Only called in reliable mode.
        try {
            sqs.changeMessageVisibility(new ChangeMessageVisibilityRequest(queueUrl, (String) messageId, 0));
        } catch (AmazonClientException ex) {
            logger.error("Exception while failing message: " + messageId, ex);
        }
    }
}
