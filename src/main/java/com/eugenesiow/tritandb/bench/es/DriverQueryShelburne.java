package com.eugenesiow.tritandb.bench.es;

import java.net.InetAddress;
import java.util.Random;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class DriverQueryShelburne {
	static String[] colNames = {"air_temperature","solar_radiation","soil_moisture","leaf_wetness","internal_temperature","relative_humidity"};
	
	public static void main(String[] args)
    {	
		try {
			long fixedSeed = 100L;
			Random rand = new Random(fixedSeed);
			long max = 1406141325958L;
			long min = 1271692742104L;
			int range = (int)((max + 1 - min )/100);
			for (int i=0; i<=1; i++) {
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
		long s = startTime * 1000 * 1000;
		long e = endTime * 1000 * 1000;
		
		TransportClient client = null;
		try {
			Settings settings = Settings.builder()
			        .put("client.transport.ignore_cluster_name", true)
			        .put("client.transport.sniff", true).build();
			
			// on startup
			client = new PreBuiltTransportClient(settings)
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
			
			GetResponse response = client.prepareGet("wsda_sensor", "point", "AV1b-4NS9RlebF3TJ59u").get();

//		    BufferedWriter bw = new BufferedWriter(new FileWriter("query_out_shelburne_driver.txt"));
//		    ResultSet rs = session.execute("select series_id,timestamp_ns,value from series_double where timestamp_ns>"+s+" AND timestamp_NS<"+e+" ALLOW FILTERING;");    // (3)
//		    for (Row row : rs) {
//		        if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched())
//		            rs.fetchMoreResults(); // this is asynchronous
////		        System.out.println(row.toString());
//		        bw.append(row.toString());
//		        bw.newLine();
//		    }
//		    bw.close();
			
			client.close();
		} catch(Exception ex) {
			ex.printStackTrace();
		}
		return System.currentTimeMillis() - start;
	}
}
