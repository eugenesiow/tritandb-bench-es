package com.eugenesiow.tritandb.bench.es;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Collections;
import java.util.Random;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class QueryShelburneRESTNoScroll {
	static String[] colNames = {"air_temperature","solar_radiation","soil_moisture","leaf_wetness","internal_temperature","relative_humidity"};
	
	public static void main(String[] args)
    {	
		try {
			long fixedSeed = 100L;
			Random rand = new Random(fixedSeed);
			long max = 1406141325958L;
			long min = 1271692742104L;
			int range = (int)((max + 1 - min )/100);
			for (int i=0; i<=0; i++) {
				long a = (rand.nextInt(range))*100L + min;
				long b = (rand.nextInt(range))*100L + min;
				long start = a;
				long end = b;
		        if (a > b) {
		            start = b;
		            end = a;
		        }
				long runTime = RunBenchmark(start,end);
				System.out.println(runTime);
			}
//			System.out.println("end---shelburne");
		} catch(Exception e) {
			e.printStackTrace();
		}
    }
	
	private static long RunBenchmark(long startTime, long endTime) {
		long start = System.currentTimeMillis();
		long s = startTime;
		long e = endTime;
		
		try {
			RestClient restClient = RestClient.builder(
			        new HttpHost("10.7.5.8", 9200, "http")).build();
			
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
			        "  \"size\": 1\n" + 
			        "}", ContentType.APPLICATION_JSON);
			
			Response response = restClient.performRequest("GET", "/wsda_sensor/_search",
			        Collections.singletonMap("filter_path", "hits.total"), entity);
			JsonElement jelement = new JsonParser().parse(EntityUtils.toString(response.getEntity()));
			long count = jelement.getAsJsonObject().get("hits").getAsJsonObject().get("total").getAsLong();
//			System.out.println(count/10000);	
			
			BufferedWriter bw = new BufferedWriter(new FileWriter("query_out_shelburne_driver.txt"));
			for(int i=0;i<count/10000;i++) {
				entity = new NStringEntity(
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
				        "  \"from\": "+i*10000+",\n" + 
				        "  \"size\": 10000\n" + 
				        "}", ContentType.APPLICATION_JSON);
				response = restClient.performRequest("GET", "/wsda_sensor/_search",
				        Collections.singletonMap("filter_path", "hits.hits._source"), entity);
				bw.append(EntityUtils.toString(response.getEntity()));
				bw.newLine();
			}
			bw.close();
						
			restClient.close();
		} catch(Exception ex) {
			ex.printStackTrace();
		}
		return System.currentTimeMillis() - start;
	}
}
