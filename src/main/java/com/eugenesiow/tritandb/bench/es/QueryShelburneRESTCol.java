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

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class QueryShelburneRESTCol {
	static String[] colNames = {"air_temperature","solar_radiation","soil_moisture","leaf_wetness","internal_temperature","relative_humidity"};
	
	public static void main(String[] args)
    {	
		try {
			long fixedSeed = 30L;
			Random rand = new Random(fixedSeed);
			fixedSeed = 20;
			Random randCol = new Random(fixedSeed);
			long max = 1406141325958L;
			long min = 1271692742104L;
			int range = (int)((max + 1 - min )/100);
			for (int i=0; i<=101; i++) {
				long a = (rand.nextInt(range))*100L + min;
				long b = (rand.nextInt(range))*100L + min;
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
		long s = startTime;
		long e = endTime;
		
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
			        "  \"size\": 10000,\n"+ 
			        "	\"_source\": [\""+colNames[(int)col]+"\"]\n" + 
			        "}", ContentType.APPLICATION_JSON);
			
			BufferedWriter bw = new BufferedWriter(new FileWriter("query_out_shelburne_driver.txt"));
			Map<String,String> params = new HashMap<String,String>();
			params.put("filter_path", "hits.hits._source,_scroll_id");
			params.put("scroll", "1m");
			Response response = restClient.performRequest("GET", "/wsda_sensor/_search",
			        params, entity);
			JsonElement jelement = new JsonParser().parse(EntityUtils.toString(response.getEntity()));
			String scrollId = jelement.getAsJsonObject().get("_scroll_id").getAsString();
			JsonElement hits = jelement.getAsJsonObject().get("hits");
			if(hits!=null) {
				hits = hits.getAsJsonObject().get("hits");
				bw.append(hits.toString());
				bw.newLine();
				bw.flush();
			}
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
