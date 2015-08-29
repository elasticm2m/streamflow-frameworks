package com.elasticm2m.frameworks.aws.dynamodb;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.Map;

public class DynamoDBWriter extends ElasticBaseRichBolt {

    private AWSCredentialsProvider credentialsProvider;
    private DynamoDB dynamoDB;
    private String tableName;
    private boolean logTuple = false;
    private Table table;

    @Inject
    public void setTableName(@Named("dynamodb-table-name") String tableName) {
        this.tableName = tableName;
    }

    @Inject
    public void setLogTupple(@Named("log-tuple") Boolean logTuple) {
        this.logTuple = logTuple;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(conf, topologyContext, collector);

        logger.info("DynamoDB Writer: Table Name = " + tableName);

        credentialsProvider = new DefaultAWSCredentialsProviderChain();

        if (credentialsProvider == null) {
            dynamoDB = new DynamoDB(new AmazonDynamoDBClient());
        } else {
            dynamoDB = new DynamoDB(new AmazonDynamoDBClient(credentialsProvider));
        }
        table = dynamoDB.getTable(tableName);
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String body = tuple.getString(1);
            Item item = new Item().fromJSON(body);
            table.putItem(item);

            if (logTuple) {
                logger.info(body);
            } else {
                logger.debug("Published record to DynamoDB");
            }

            collector.ack(tuple);
        } catch (Exception ex) {
            logger.error("Error writing the entity to Kinesis:", ex);
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
}
