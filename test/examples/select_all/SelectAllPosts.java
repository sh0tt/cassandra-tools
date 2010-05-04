package examples.select_all;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SuperColumn;
import org.t2framework.cassandra.tools.util.CassandraClient;
import org.t2framework.commons.util.Logger;

public class SelectAllPosts {

	private static Logger LOG = Logger.getLogger(SelectAllPosts.class);

	public static void main(String[] args) {
		CassandraClient client = new CassandraClient();
		client.connect();
		try {
			final String KEYSPACE = "Blog";
			final String COLUMN_FAMILY = "Posts";
			List<KeySlice> slices = client.selectAll(KEYSPACE, COLUMN_FAMILY,
					ConsistencyLevel.ONE);
			long start = System.currentTimeMillis();
			for (KeySlice slice : slices) {
				String rowKey = slice.getKey();
				Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
				for (ColumnOrSuperColumn csc : slice.getColumns()) {
					SuperColumn superColumn = csc.getSuper_column();
					final String superColumnKey = new String(superColumn
							.getName());
					LOG.debug("superColumnKey = " + superColumnKey);
					Map<String, String> map2 = new HashMap<String, String>();
					for (Column c : superColumn.getColumns()) {
						String columnName = new String(c.getName());
						String columnValue = new String(c.getValue());
						LOG.debug("key = " + columnName + ", value = "
								+ columnValue);
						map2.put(columnName, columnValue);
					}
					map.put(superColumnKey, map2);
				}
				LOG.debug("row key : " + rowKey + ", value : " + map);
			}
			LOG
					.debug("takes " + (System.currentTimeMillis() - start)
							+ " msec");
		} finally {
			client.disconnect();
		}
	}
}
