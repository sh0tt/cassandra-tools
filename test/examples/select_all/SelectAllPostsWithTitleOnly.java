package examples.select_all;

import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;
import org.t2framework.cassandra.tools.util.CassandraClient;
import org.t2framework.commons.util.Logger;

public class SelectAllPostsWithTitleOnly {

	private static Logger LOG = Logger
			.getLogger(SelectAllPostsWithTitleOnly.class);

	public static void main(String[] args) {
		CassandraClient client = new CassandraClient();
		client.connect();
		try {
			final String KEYSPACE = "Blog";
			final String COLUMN_FAMILY = "Posts";
			Client nativeClient = client.getNativeClient();

			SlicePredicate predicate = new SlicePredicate();
			SliceRange slice = new SliceRange(new byte[] {}, new byte[] {},
					false, 100);
			predicate.setSlice_range(slice);
			// predicate.setColumn_names(Arrays.asList("post0".getBytes(),
			// "post6"
			// .getBytes()));

			KeyRange keyrange = new KeyRange();
			keyrange.setStart_key("");
			keyrange.setEnd_key("");
			List<KeySlice> slices = nativeClient.get_range_slices(KEYSPACE,
					new ColumnParent(COLUMN_FAMILY), predicate, keyrange,
					ConsistencyLevel.ONE);
			for (KeySlice keySlice : slices) {
				String key = keySlice.getKey();
				LOG.debug(key);
				for (ColumnOrSuperColumn csc : keySlice.getColumns()) {
					SuperColumn superColumn = csc.getSuper_column();
					if (superColumn != null) {
						LOG.debug("\tsuper column name : "
								+ new String(superColumn.getName()));
						for (Column c : superColumn.getColumns()) {
							if ("title".equals(new String(c.name))) {
								LOG.debug("\t\t" + new String(c.name) + " -> "
										+ new String(c.value) + " at ("
										+ c.timestamp + ")");
							}
						}
					}
				}

			}
		} catch (InvalidRequestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnavailableException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimedOutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			client.disconnect();
		}

	}
}
