package org.t2framework.cassandra.tools.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.t2framework.commons.util.Logger;

public class CassandraClient {

	private static Logger LOG = Logger.getLogger(CassandraClient.class);

	public static final String DEFAULT_HOST = "localhost";

	public static final int DEFAULT_PORT = 9160;

	protected String host;

	protected int port;

	protected Cassandra.Client thriftClient;

	protected TTransport transport;

	private static final Object lock = new Object();

	public CassandraClient() {
		this(DEFAULT_HOST, DEFAULT_PORT);
	}

	public CassandraClient(Properties prop) {
		this(prop.getProperty("host"), Integer
				.valueOf(prop.getProperty("port")).intValue());
	}

	public CassandraClient(String host) {
		this(host, DEFAULT_PORT);
	}

	public CassandraClient(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public void connect() {
		transport = new TSocket(host, port);
		TProtocol protocol = new TBinaryProtocol(transport);
		thriftClient = new Cassandra.Client(protocol);
		try {
			transport.open();
		} catch (TTransportException e) {
			throw new RuntimeException(e);
		}
	}

	public void disconnect() {
		synchronized (lock) {
			try {
				transport.flush();
			} catch (TTransportException e) {
				e.printStackTrace();
			} finally {
				transport.close();
			}
		}
	}

	protected void assertConnected() {
		if (this.thriftClient == null) {
			throw new NullPointerException("call connect() before doing some.");
		}
	}

	public Cassandra.Client getNativeClient() {
		return this.thriftClient;
	}

	public List<KeySlice> selectAll(final String keyspace,
			final String columnFamily, ConsistencyLevel consistencyLevel) {
		assertConnected();
		// レンジとりすぎるとメモリ不足になる...
		KeyRange keyRange = new KeyRange(10000);
		keyRange.setStart_key("");
		keyRange.setEnd_key("");

		SliceRange sliceRange = new SliceRange();
		sliceRange.setStart(new byte[] {});
		sliceRange.setFinish(new byte[] {});

		SlicePredicate predicate = new SlicePredicate();
		predicate.setSlice_range(sliceRange);

		try {
			return this.thriftClient.get_range_slices(keyspace,
					new ColumnParent(columnFamily), predicate, keyRange,
					consistencyLevel);
		} catch (InvalidRequestException e) {
			throw new RuntimeException(e);
		} catch (UnavailableException e) {
			throw new RuntimeException(e);
		} catch (TimedOutException e) {
			throw new RuntimeException(e);
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	public ColumnOrSuperColumn selectOne(String keyspace, String columnFamily,
			String row, String column) throws InvalidRequestException,
			NotFoundException, UnavailableException, TimedOutException,
			TException {
		assertConnected();
		ColumnPath path = new ColumnPath();
		path.setColumn_family(columnFamily);
		path.setColumn(column.getBytes());
		return thriftClient.get(keyspace, row, path, ConsistencyLevel.ONE);
	}

	public void insertColumns(String keyspace, String columnFamily,
			String rowKey, Map<String, Object> map, long time,
			ConsistencyLevel level) throws InvalidRequestException,
			UnavailableException, TimedOutException, TException {
		assertConnected();
		for (Map.Entry<String, Object> e : map.entrySet()) {
			String columnName = e.getKey();
			Object columnValue = e.getValue();
			insertEachColumn(keyspace, columnFamily, rowKey, columnName,
					columnValue, time, level);
		}

	}

	public void insertEachColumn(String keyspace, String columnFamily,
			String rowKey, String columnName, Object columnValue, long time,
			ConsistencyLevel level) throws InvalidRequestException,
			UnavailableException, TimedOutException, TException {
		assertConnected();
		ColumnPath columnPath = new ColumnPath(columnFamily);
		columnPath.setColumn(columnName.getBytes());
		thriftClient.insert(keyspace, rowKey, columnPath, columnValue
				.toString().getBytes(), time, ConsistencyLevel.ONE);

	}

	public String describeClusterName() {
		try {
			return getNativeClient().describe_cluster_name();
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	public Map<String, Map<String, String>> describeKeyspace(String keyspace) {
		try {
			return getNativeClient().describe_keyspace(keyspace);
		} catch (NotFoundException e) {
			e.printStackTrace();
			return null;
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	public Set<String> describeKeyspaces() {
		try {
			return getNativeClient().describe_keyspaces();
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	public List<TokenRange> describeRing(String keyspace) {
		try {
			return getNativeClient().describe_ring(keyspace);
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	public String getConfigContents() {
		try {
			return getNativeClient().get_string_property("config file");
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	public String getTokenMap() {
		try {
			return getNativeClient().get_string_property("token map");
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	public void batchInserts(final String keyspace,
			final Map<String, Map<String, Map<String, Object>>> map) {
		assertConnected();
		Map<String, Map<String, List<Mutation>>> param = new HashMap<String, Map<String, List<Mutation>>>();
		for (Map.Entry<String, Map<String, Map<String, Object>>> e : map
				.entrySet()) {
			final String rowkey = e.getKey();
			Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();
			for (Map.Entry<String, Map<String, Object>> e2 : e.getValue()
					.entrySet()) {
				final String columnFamily = e2.getKey();
				final List<Mutation> list = new ArrayList<Mutation>();
				for (Map.Entry<String, Object> e3 : e2.getValue().entrySet()) {
					final String columnKey = e3.getKey();
					final Object value = e3.getValue();

					// todo converting
					Column c = new Column(columnKey.getBytes(), value
							.toString().getBytes(), System.currentTimeMillis());
					ColumnOrSuperColumn csc = new ColumnOrSuperColumn();
					csc.setColumn(c);

					Mutation mutation = new Mutation();
					mutation.setColumn_or_supercolumn(csc);
					list.add(mutation);
				}
				mutationMap.put(columnFamily, list);
			}
			param.put(rowkey, mutationMap);
		}
		try {
			LOG.debug("inserts : keyspace" + keyspace + ", param:" + map);
			thriftClient.batch_mutate(keyspace, param, ConsistencyLevel.ALL);
		} catch (InvalidRequestException e) {
			throw new RuntimeException(e);
		} catch (UnavailableException e) {
			throw new RuntimeException(e);
		} catch (TimedOutException e) {
			throw new RuntimeException(e);
		} catch (TException e) {
			throw new RuntimeException(e);
		}
		LOG.debug("batch_mutate inserting done.");
	}

	public void batchSuperColumnInserts(final String keyspace,
			final Map<String, Map<String, Map<String, List<Object>>>> map) {
		assertConnected();
		Map<String, Map<String, List<Mutation>>> param = new HashMap<String, Map<String, List<Mutation>>>();
		for (Map.Entry<String, Map<String, Map<String, List<Object>>>> e : map
				.entrySet()) {
			final String rowkey = e.getKey();
			Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();
			for (Map.Entry<String, Map<String, List<Object>>> e2 : e.getValue()
					.entrySet()) {
				final String columnFamily = e2.getKey();
				final List<Mutation> list = new ArrayList<Mutation>();
				for (Map.Entry<String, List<Object>> e3 : e2.getValue()
						.entrySet()) {
					final String columnKey = e3.getKey();
					final List<Object> value = e3.getValue();
					SuperColumn superColumn = new SuperColumn();
					for (Object o : value) {
						Column c = new Column(columnKey.getBytes(), o
								.toString().getBytes(), System
								.currentTimeMillis());
						superColumn.addToColumns(c);
					}
					ColumnOrSuperColumn csc = new ColumnOrSuperColumn();
					csc.setSuper_column(superColumn);

					Mutation mutation = new Mutation();
					mutation.setColumn_or_supercolumn(csc);
					list.add(mutation);
				}
				mutationMap.put(columnFamily, list);
			}
			param.put(rowkey, mutationMap);
		}
		try {
			LOG.debug("inserts : keyspace" + keyspace + ", param:" + map);
			thriftClient.batch_mutate(keyspace, param, ConsistencyLevel.ALL);
		} catch (InvalidRequestException e) {
			throw new RuntimeException(e);
		} catch (UnavailableException e) {
			throw new RuntimeException(e);
		} catch (TimedOutException e) {
			throw new RuntimeException(e);
		} catch (TException e) {
			throw new RuntimeException(e);
		}
		LOG.debug("batch_mutate inserting done.");
	}

}
