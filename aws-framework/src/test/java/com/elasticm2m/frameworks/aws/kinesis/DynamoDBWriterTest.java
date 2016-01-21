package com.elasticm2m.frameworks.aws.kinesis;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.elasticm2m.frameworks.aws.dynamodb.DynamoDBWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

@Ignore
public class DynamoDBWriterTest extends Assert {

    private DynamoDBWriter dynamoDBWriter;

    @Before
    public void createTable() {

        AmazonDynamoDB client = new AmazonDynamoDBClient();
        DynamoDB dynamoDB = new DynamoDB(client);

        String tableName = "dynamo-writer-test";
        String hashKeyName = "hash";
        String rangeKeyName = "range";
        long readCapacityUnits = 1;
        long writeCapacityUnits = 1;

        List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement()
                .withAttributeName(hashKeyName)
                .withKeyType(KeyType.HASH));
        keySchema.add(new KeySchemaElement()
                .withAttributeName(rangeKeyName)
                .withKeyType(KeyType.RANGE));

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(keySchema)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(readCapacityUnits)
                        .withWriteCapacityUnits(writeCapacityUnits));

        System.out.println("Issuing CreateTable request for " + tableName);
        Table table = dynamoDB.createTable(request);
        dynamoDBWriter = new DynamoDBWriter();
    }

    @Test
    public void testWriteJson() {
        assertNotNull(dynamoDBWriter);

    }
}
