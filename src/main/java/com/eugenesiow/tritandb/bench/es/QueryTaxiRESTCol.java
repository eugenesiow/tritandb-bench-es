package com.eugenesiow.tritandb.bench.es;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class QueryTaxiRESTCol {
	static String[] colNames = {"vendorid","lpep_dropoff_datetime","store_and_fwd_flag","ratecodeid","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","passenger_count","trip_distance","fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount","payment_type","trip_type","point_date"};;
	
	public static void main(String[] args)
    {	
		try {
			long fixedSeed = 30L;
			Random rand = new Random(fixedSeed);
			fixedSeed = 20;
			Random randCol = new Random(fixedSeed);
			int max = 1467241200;
			int min = 1459465200;
			int range = (max + 1 - min);
			for (int i=0; i<=101; i++) {
				long a = (rand.nextInt(range)) + min;
				long b = (rand.nextInt(range)) + min;
				long start = a;
				long end = b;
		        if (a > b) {
		            start = b;
		            end = a;
		        }
		        long col = randCol.nextInt(colNames.length);
				long runTime = RunBenchmark(start,end,col);
				System.out.println(runTime);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
    }
	
	private static long RunBenchmark(long startTime, long endTime, long col) {
		long start = System.currentTimeMillis();
		long s = startTime * 1000;
		long e = endTime * 1000;
		
		try {
			RestClient restClient = RestClient.builder(
			        new HttpHost("localhost", 9200, "http")).build();
			
			HttpEntity entity = new NStringEntity(
			        "{\n" + 
			        "  \"query\": {\n" + 
			        "    \"bool\": {\n" + 
			        "      \"must\": { \"match_all\": {} },\n" + 
			        "      \"filter\": {\n" + 
			        "        \"range\": {\n" + 
			        "          \"timestamp\": {\n" + 
			        "            \"gte\": "+s+",\n" + 
			        "            \"lte\": "+e+"\n" + 
			        "          }\n" + 
			        "        }\n" + 
			        "      }\n" + 
			        "    }\n" + 
			        "  },\n" + 
			        "  \"size\": 10000,\n" + 
			        "	\"_source\": [\""+colNames[(int)col]+"\"]\n" + 
			        "}", ContentType.APPLICATION_JSON);
			
			BufferedWriter bw = new BufferedWriter(new FileWriter("query_out_taxi_driver.txt"));
			Map<String,String> params = new HashMap<String,String>();
			params.put("filter_path", "hits.hits._source,_scroll_id");
			params.put("scroll", "1m");
			Response response = restClient.performRequest("GET", "/green_taxi/_search",
			        params, entity);
			JsonElement jelement = new JsonParser().parse(EntityUtils.toString(response.getEntity()));
			String scrollId = jelement.getAsJsonObject().get("_scroll_id").getAsString();
			JsonElement hits = jelement.getAsJsonObject().get("hits").getAsJsonObject().get("hits");
			bw.append(hits.toString());
			bw.newLine();
			bw.flush();
//			System.out.println(scrollId);

			while(hits!=null) {
				entity = new NStringEntity(
				        "{\n" + 
				        "    \"scroll\" : \"1m\", \n" + 
				        "    \"scroll_id\" : \""+scrollId+"\" \n" + 
				        "}", ContentType.APPLICATION_JSON);
				response = restClient.performRequest("GET", "/_search/scroll",
				        Collections.singletonMap("filter_path", "hits.hits._source,_scroll_id"), entity);
				jelement = new JsonParser().parse(EntityUtils.toString(response.getEntity()));
				scrollId = jelement.getAsJsonObject().get("_scroll_id").getAsString();
				hits = jelement.getAsJsonObject().get("hits");
				if(hits!=null) {
					hits = hits.getAsJsonObject().get("hits");
					bw.append(hits.toString());
					bw.newLine();
					bw.flush();
				} else {
					break;
				}
			}
			bw.flush();
			bw.close();
						
			restClient.close();
		} catch(Exception ex) {
			ex.printStackTrace();
		}
		return System.currentTimeMillis() - start;
	}
}
