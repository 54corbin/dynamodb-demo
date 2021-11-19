package com.chb;

import java.util.*;
import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.model.*;

public class Main {

    /**
     * get table structure info
     */
    public static void describeTable(AmazonDynamoDB ddb,String table_name) throws Exception {
        System.out.format("Getting description for %s\n\n", table_name);

        TableDescription table_info = ddb.describeTable(table_name).getTable();

        if (table_info != null) {
            System.out.format("Table name  : %s\n", table_info.getTableName());
            System.out.format("Table ARN   : %s\n", table_info.getTableArn());
            System.out.format("Status      : %s\n", table_info.getTableStatus());
            System.out.format("Item count  : %d\n", table_info.getItemCount().longValue());
            System.out.format("Size (bytes): %d\n", table_info.getTableSizeBytes().longValue());

            ProvisionedThroughputDescription throughput_info = table_info.getProvisionedThroughput();
            System.out.println("Throughput");
            System.out.format("  Read Capacity : %d\n", throughput_info.getReadCapacityUnits().longValue());
            System.out.format("  Write Capacity: %d\n", throughput_info.getWriteCapacityUnits().longValue());

            List<AttributeDefinition> attributes = table_info.getAttributeDefinitions();
            System.out.println("Attributes");
            for (AttributeDefinition a : attributes) {
                System.out.format("  %s (%s)\n", a.getAttributeName(), a.getAttributeType());
            }
        }
        System.out.println("\nDone!");
    }

    /**
     * create a new table
     */
    public static void createTable(AmazonDynamoDB ddb,String table_name) throws Exception {

        System.out.format("Creating table \"%s\" with a simple primary key: \"Name\".\n", table_name);

        CreateTableRequest request = new CreateTableRequest()
                .withAttributeDefinitions(new AttributeDefinition("Name", ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement("Name", KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(Long.valueOf(10), Long.valueOf(10)))
                .withTableName(table_name);


        CreateTableResult result = ddb.createTable(request);
        System.out.println(result.getTableDescription().getTableName());
        System.out.println("Done!");
    }

    public static void putItem(AmazonDynamoDB ddb,String table_name, String name,String values) {
        ArrayList<String[]> extra_fields = new ArrayList<String[]>();

        String[] vals = values.split(",");
        for (int x = 2; x < vals.length; x++) {
            String[] fields = vals[x].split("=", 2);
            if (fields.length == 2) {
                extra_fields.add(fields);
            } else {
                System.out.format("Invalid argument: %s\n", vals[x]);
                System.exit(1);
            }
        }

        System.out.format("Adding \"%s\" to \"%s\"", name, table_name);
        if (extra_fields.size() > 0) {
            System.out.println("Additional fields:");
            for (String[] field : extra_fields) {
                System.out.format("  %s: %s\n", field[0], field[1]);
            }
        }

        HashMap<String, AttributeValue> item_values = new HashMap<String, AttributeValue>();

        item_values.put("Name", new AttributeValue(name));

        for (String[] field : extra_fields) {
            item_values.put(field[0], new AttributeValue(field[1]));
        }

        ddb.putItem(table_name, item_values);
        System.out.println("Done!");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("hello !!!");

        String table_name = "table_for_test" + Math.random();

        final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();

        createTable(ddb,table_name);

        describeTable(ddb,table_name);

        putItem(ddb,table_name, "name", "attr1=val1;attr2=val2");


    }
}
