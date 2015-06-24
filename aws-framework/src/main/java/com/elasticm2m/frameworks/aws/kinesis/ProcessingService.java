package com.elasticm2m.frameworks.aws.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

@Singleton
public class ProcessingService extends AbstractExecutionThreadService implements IRecordProcessorFactory {

    private final AWSCredentialsProvider credentialsProvider;
    private final BlockingQueue<Record> queue;
    private final String applicationName;
    private final String streamName;
    private InitialPositionInStream initialPosition;
    private final Logger logger;
    private Worker worker;

    @Inject
    public ProcessingService(AWSCredentialsProvider credentialsProvider,
                             BlockingQueue<Record> queue, String applicationName, String streamName,
                             InitialPositionInStream initialPosition, Logger logger) {
        this.credentialsProvider = credentialsProvider;
        this.queue = queue;
        this.applicationName = applicationName;
        this.streamName = streamName;
        this.initialPosition = initialPosition;
        this.logger = logger;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new QueueingRecordProcessor();
    }

    @Override
    protected void startUp() {
        String workerId;
        try {
            workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        if (initialPosition == null) {
            initialPosition = InitialPositionInStream.LATEST;
        }
        logger.info("Initializing Kinesis client.  Initial Position = " + initialPosition.name());

        KinesisClientLibConfiguration kinesisClientLibConfig =
                new KinesisClientLibConfiguration(applicationName, streamName, credentialsProvider, workerId)
                        .withInitialPositionInStream(initialPosition);
        worker = new Worker(this, kinesisClientLibConfig);
    }

    @Override
    protected void run() throws Exception {
        worker.run();
    }

    @Override
    protected void triggerShutdown() {
        worker.shutdown();
    }

    class QueueingRecordProcessor implements IRecordProcessor {

        @Override
        public void initialize(String s) {
        }

        @Override
        public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkPointer) {
            logger.info("Received records from stream: Count = " + records.size());
            records.forEach(record -> {
                try {
                    queue.put(record);
                } catch (InterruptedException e) {
                    logger.error("Error writing record to queue", e);
                }
            });
            try {
                checkPointer.checkpoint();
            } catch (InvalidStateException e) {
                logger.error("Invalid state exception:", e);
            } catch (ShutdownException e) {
                logger.error("Error performing checkpoint on stream");
            }
        }

        @Override
        public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {
            logger.info("Shutting down Kinesis client");
        }
    }
}
