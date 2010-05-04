package examples.batch_insert;

import java.util.HashMap;
import java.util.Map;

import org.t2framework.cassandra.tools.util.CassandraClient;
import org.t2framework.commons.util.Logger;

/**
 * LongTypeのバッチインサート
 */
public class ExperimentalAuthors2BatchInsert {

	private static Logger LOG = Logger
			.getLogger(ExperimentalAuthors2BatchInsert.class);

	public static void main(String[] args) {
		CassandraClient client = new CassandraClient();
		client.connect();
		try {
			String KEYSPACE = "Blog";
			Map<String, Map<String, Map<String, Object>>> rowMap = new HashMap<String, Map<String, Map<String, Object>>>();
			for (int i = 0; i < 10000; i++) {
				Map<String, Object> columnMap = new HashMap<String, Object>();
				columnMap.put(String.format("%08d", i), "shot" + i
						+ "@shot.com");
				Map<String, Map<String, Object>> columnFamilyMap = new HashMap<String, Map<String, Object>>();
				columnFamilyMap.put("ExperimentalAuthors2", columnMap);
				rowMap.put("shot" + i, columnFamilyMap);
			}
			long start = System.currentTimeMillis();
			client.batchInserts(KEYSPACE, rowMap);
			LOG
					.debug("takes " + (System.currentTimeMillis() - start)
							+ " msec");
		} finally {
			client.disconnect();
		}
	}

}
