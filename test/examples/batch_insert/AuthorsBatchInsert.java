package examples.batch_insert;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.t2framework.cassandra.tools.util.CassandraClient;

public class AuthorsBatchInsert {

	public static void main(String[] args) {
		CassandraClient client = new CassandraClient();
		client.connect();
		try {
			String KEYSPACE = "Blog";
			Map<String, Map<String, Map<String, Object>>> rowMap = new HashMap<String, Map<String, Map<String, Object>>>();
			for (int i = 0; i < 10000; i++) {
				Map<String, Object> columnMap = new HashMap<String, Object>();
				columnMap.put("email", "shot" + i + "@shot.com");
				columnMap.put("country", getCountry(i));
				columnMap.put("registeredSince", Calendar.getInstance()
						.getTime().getTime());
				Map<String, Map<String, Object>> columnFamilyMap = new HashMap<String, Map<String, Object>>();
				columnFamilyMap.put("Authors", columnMap);
				rowMap.put("shot" + i, columnFamilyMap);
			}
			client.batchInserts(KEYSPACE, rowMap);
		} finally {
			client.disconnect();
		}
	}

	private static String getCountry(int i) {
		if (i % 2 == 0 && i % 3 != 0) {
			return "japan";
		} else if (i % 2 != 0 && i % 3 == 0) {
			return "usa";
		} else {
			return "italy";
		}
	}
}
