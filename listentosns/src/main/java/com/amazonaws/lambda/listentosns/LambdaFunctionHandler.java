package com.amazonaws.lambda.listentosns;

import org.json.JSONObject;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;


// Created by Anshul Gautam. Please feel free to share and modify. Thanks!
public class LambdaFunctionHandler implements RequestHandler<SNSEvent, String> {

    final DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.standard().build());

    // Your table name goes here
    final static String TABLE_NAME = "SNS_TEST_DB";

    final static String INSERTION_FAILED = "Insertion into DynamoDB failed.";

    final static String INSERTION_SUCCESS = "Insertion into DynamoDB successfull.";

    @Override
    public String handleRequest(SNSEvent event, Context context) {
        context.getLogger().log("Received event: " + event);
        String jsonString = null;
        Table myTestTable = null;
        jsonString = event.getRecords().get(0).getSNS().getMessage();
        context.getLogger().log("Received jsonString. Its value is : " + jsonString);
        if (null != jsonString) {
            context.getLogger().log("Establishing connection with table :  " + event);
            PutItemOutcome insertionOutcome = null;
            try {
                myTestTable = dynamoDB.getTable(TABLE_NAME);
                final JSONObject jsonObj = new JSONObject(jsonString);
                insertionOutcome = insertIntoTable(
                        createItem(jsonObj, event.getRecords().get(0).getSNS().getMessageId()), myTestTable);
            } catch (Exception e) {
                context.getLogger().log("Exception occurred : " + e);
            }
            if (null != insertionOutcome) {
                context.getLogger().log(INSERTION_SUCCESS);
                return INSERTION_SUCCESS;
            }
        }
        return INSERTION_FAILED;
    }

    private Item createItem(final JSONObject valueToInsert, final String messageId) {
        // Build the item to be inserted.
        Item item = new Item();
        item.withPrimaryKey("id", messageId);
        item.withString("ex_name", valueToInsert.getString("exName"));
        item.withNumber("ex_age", Integer.parseInt(valueToInsert.getString("exAge")));
        item.withString("ex_designation", valueToInsert.getString("exDesignation"));
        item.withString("ex_talent", valueToInsert.getString("exTalent"));
        return item;
    }

    private PutItemOutcome insertIntoTable(final Item itemToInsert, final Table table) {
        // Write the item to the table
        final PutItemOutcome insertionOutcome = table.putItem(itemToInsert);
        return insertionOutcome;
    }
}
