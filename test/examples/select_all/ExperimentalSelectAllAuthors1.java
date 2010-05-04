package examples.select_all;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeySlice;
import org.t2framework.cassandra.tools.util.CassandraClient;
import org.t2framework.commons.util.Logger;

public class ExperimentalSelectAllAuthors1 {

	private static Logger LOG = Logger
			.getLogger(ExperimentalSelectAllAuthors1.class);

	public static void main(String[] args) {
		CassandraClient client = new CassandraClient();
		client.connect();
		try {
			final String KEYSPACE = "Blog";
			final String COLUMN_FAMILY = "ExperimentalAuthors";
			List<KeySlice> slices = client.selectAll(KEYSPACE, COLUMN_FAMILY,
					ConsistencyLevel.ONE);
			long start = System.currentTimeMillis();
			for (KeySlice slice : slices) {
				String rowKey = slice.getKey();
				int columnsSize = slice.getColumnsSize();
				Map<String, String> map = new HashMap<String, String>();
				for (ColumnOrSuperColumn csc : slice.getColumns()) {
					Column column = csc.getColumn();
					String name = new String(column.name);
					String value = new String(column.value);
					map.put(name, value);
				}
				LOG.debug("row key : " + rowKey + ", value : " + map
						+ ", size : " + columnsSize);
			}
			LOG
					.debug("takes " + (System.currentTimeMillis() - start)
							+ " msec");
		} finally {
			client.disconnect();
		}
	}
}
