package examples.batch_insert;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;
import org.t2framework.cassandra.tools.util.CassandraClient;
import org.t2framework.commons.util.Logger;

public class PostsBatchInsert {

	private static Logger LOG = Logger.getLogger(PostsBatchInsert.class);

	private static final String ENCODING = "UTF8";

	public static void main(String[] args) throws InvalidRequestException,
			UnavailableException, TimedOutException, TException,
			UnsupportedEncodingException {
		CassandraClient client = new CassandraClient();
		client.connect();
		try {
			String KEYSPACE = "Blog";
			String COLUMN_FAMILY = "Posts";
			Client nativeClient = client.getNativeClient();
			for (int i = 0; i < 10; i++) {
				long timestamp = System.currentTimeMillis();
				Map<String, Map<String, List<Mutation>>> job = new HashMap<String, Map<String, List<Mutation>>>();
				List<Mutation> mutations = new ArrayList<Mutation>();

				// update the title
				List<Column> columns = new ArrayList<Column>();
				columns.add(new Column("title".getBytes(ENCODING),
						("Update: cats are funny animals" + i)
								.getBytes(ENCODING), timestamp));
				columns.add(new Column("author".getBytes(ENCODING), "shot"
						.getBytes(ENCODING), timestamp));
				columns.add(new Column("updateDate".getBytes(ENCODING),
						"03/02/2010".getBytes(ENCODING), timestamp));

				SuperColumn superColumn = new SuperColumn(("post" + i)
						.getBytes(ENCODING), columns);
				ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
				columnOrSuperColumn.setSuper_column(superColumn);

				Mutation mutation = new Mutation();
				mutation.setColumn_or_supercolumn(columnOrSuperColumn);
				mutations.add(mutation);

				List<Column> columns2 = new ArrayList<Column>();
				for (int j = 0; j < 5; j++) {
					columns2.add(new Column(String.valueOf(j).getBytes(),
							("tag" + j).getBytes(), timestamp));
				}
				SuperColumn superColumn2 = new SuperColumn(("tag" + i)
						.getBytes(ENCODING), columns2);
				ColumnOrSuperColumn columnOrSuperColumn2 = new ColumnOrSuperColumn();
				columnOrSuperColumn2.setSuper_column(superColumn2);

				Mutation mutation2 = new Mutation();
				mutation2.setColumn_or_supercolumn(columnOrSuperColumn2);
				mutations.add(mutation2);

				Map<String, List<Mutation>> mutationsForColumnFamily = new HashMap<String, List<Mutation>>();
				mutationsForColumnFamily.put(COLUMN_FAMILY, mutations);

				job.put("cats-are-funny-animals", mutationsForColumnFamily);

				nativeClient.batch_mutate(KEYSPACE, job, ConsistencyLevel.ALL);

				LOG.debug("insert " + i + ", value : " + job);
			}
		} finally {
			client.disconnect();
		}
	}

	public static enum Visibility {
		PUBLIC, PRIVATE;
	}
}
