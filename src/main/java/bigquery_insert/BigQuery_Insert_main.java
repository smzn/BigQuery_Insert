package bigquery_insert;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;

public class BigQuery_Insert_main {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		BigQuery bigQuery = getClientWithJsonKey("/Users/mizuno/Downloads/closedqueue-929a267e03b8.json");
		// Create a dataset
	    String datasetId = "mznfe";

	    TableId tableId = TableId.of(datasetId, "Fe_OFF_1_99");
	    
	    Field row[] = new Field[10];
	    for(int i = 0; i < 10; i++) {
	    		row[i] = Field.of("sum"+(i+1), LegacySQLTypeName.INTEGER);
	    }
	    Schema schema = Schema.of(row);
	    
	    Map<String, Object> rowvalue = new HashMap<>();
	    for(int i = 0; i < 10; i++) {
	    		rowvalue.put("sum"+(i+1), i);
	    }
		InsertAllRequest insertRequest = InsertAllRequest.newBuilder(tableId).addRow(rowvalue).build();
	    
	    InsertAllResponse insertResponse = bigQuery.insertAll(insertRequest);
	    // Check if errors occurred
	    if (insertResponse.hasErrors()) {
	      System.out.println("Errors occurred while inserting rows");
	    }
	    System.out.println("Insert OK");
	    
	 // Create a query request
	    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder("SELECT * FROM mznfe.Fe_OFF_1_99").build();
	    // Read rows
	    System.out.println("Table rows:");
	    try {
			for (FieldValueList row1 : bigQuery.query(queryConfig).iterateAll()) {
			  System.out.println(row1);
			}
		} catch (JobException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    	    
	}
	
	//jsonファイル認証関数
	public static BigQuery getClientWithJsonKey(String key) throws IOException {
        return BigQueryOptions.newBuilder()
                .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(key)))
                .build()
                .getService();
    }


}
