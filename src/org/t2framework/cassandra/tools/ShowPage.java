package org.t2framework.cassandra.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.cassandra.cache.JMXInstrumentedCacheMBean;
import org.apache.cassandra.concurrent.IExecutorMBean;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.tools.NodeProbe;
import org.json.simple.JSONValue;
import org.t2framework.cassandra.tools.util.CassandraClient;
import org.t2framework.t2.annotation.composite.GET;
import org.t2framework.t2.annotation.core.ActionPath;
import org.t2framework.t2.annotation.core.Default;
import org.t2framework.t2.annotation.core.In;
import org.t2framework.t2.annotation.core.Page;
import org.t2framework.t2.annotation.core.RequestParam;
import org.t2framework.t2.annotation.core.Var;
import org.t2framework.t2.navigation.NoOperation;
import org.t2framework.t2.navigation.SimpleText;
import org.t2framework.t2.spi.Navigation;

@Page("/show")
public class ShowPage extends AbstractPage {

	@Default
	public Navigation index(HttpServletRequest req, HttpServletResponse res) {
		try {
			PrintWriter writer = res.getWriter();
			writer.write("<a href='" + req.getContextPath()
					+ "/show/liveNodes'>liveNodes</a>");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return NoOperation.INSTANCE;
	}

	@GET
	@ActionPath
	public Navigation liveNodes(@In NodeProbe nodeProbe,
			@RequestParam("format") Format format) {
		Set<String> liveNodes = nodeProbe.getLiveNodes();
		return process(format, liveNodes);
	}

	@GET
	@ActionPath
	public Navigation unreachableNodes(@In NodeProbe nodeProbe,
			@RequestParam("format") Format format) {
		Set<String> unreachableNodes = nodeProbe.getUnreachableNodes();
		return process(format, unreachableNodes);
	}

	@GET
	@ActionPath
	public Navigation clearSnapshot(@In NodeProbe nodeProbe,
			@RequestParam("format") Format format) {
		Map<String, String> results = invokeNodeProbe(nodeProbe,
				new NodeProbeTask() {

					@Override
					public void invoke(NodeProbe nodeProbe,
							Map<String, String> results) throws Exception {
						nodeProbe.clearSnapshot();
					}
				});
		return process(format, results);
	}

	@GET
	@ActionPath
	public Navigation decommission(final @In NodeProbe nodeProbe) {
		Map<String, String> results = invokeNodeProbe(nodeProbe,
				new NodeProbeTask() {

					@Override
					public void invoke(NodeProbe nodeProbe,
							Map<String, String> results) throws Exception {
						nodeProbe.decommission();
					}
				});
		return toJson(results);
	}

	@GET
	@ActionPath
	public Navigation forceTableCleanup(final @In NodeProbe nodeProbe) {
		Map<String, String> results = invokeNodeProbe(nodeProbe,
				new NodeProbeTask() {

					@Override
					public void invoke(NodeProbe nodeProbe,
							Map<String, String> results) throws Exception {
						nodeProbe.forceTableCleanup();
					}
				});
		return toJson(results);
	}

	@GET
	@ActionPath
	public Navigation forceTableCompaction(final @In NodeProbe nodeProbe) {
		Map<String, String> results = invokeNodeProbe(nodeProbe,
				new NodeProbeTask() {

					@Override
					public void invoke(NodeProbe nodeProbe,
							Map<String, String> results) throws Exception {
						nodeProbe.forceTableCompaction();
					}
				});
		return toJson(results);
	}

	@GET
	@ActionPath
	public Navigation forceTableCompaction(final @In NodeProbe nodeProbe,
			final @RequestParam("tableName") String tableName,
			final @RequestParam("columnFamilies") String[] columnFamilies) {
		Map<String, String> results = invokeNodeProbe(nodeProbe,
				new NodeProbeTask() {

					@Override
					public void invoke(NodeProbe nodeProbe,
							Map<String, String> results) throws Exception {
						nodeProbe.forceTableFlush(tableName, columnFamilies);
					}
				});
		return toJson(results);
	}

	@GET
	@ActionPath
	public Navigation forceTableRepair(final @In NodeProbe nodeProbe,
			final @RequestParam("tableName") String tableName,
			final @RequestParam("columnFamilies") String[] columnFamilies) {
		Map<String, String> results = invokeNodeProbe(nodeProbe,
				new NodeProbeTask() {

					@Override
					public void invoke(NodeProbe nodeProbe,
							Map<String, String> results) throws Exception {
						nodeProbe.forceTableRepair(tableName, columnFamilies);
					}
				});
		return toJson(results);
	}

	@GET
	@ActionPath
	public Navigation getColumnFamilyStoreMBeanProxies(
			final @In NodeProbe nodeProbe) {
		Map<String, String> results = invokeNodeProbe(nodeProbe,
				new NodeProbeTask() {

					@Override
					public void invoke(NodeProbe nodeProbe,
							Map<String, String> results) throws Exception {
						for (Iterator<Entry<String, ColumnFamilyStoreMBean>> itr = nodeProbe
								.getColumnFamilyStoreMBeanProxies(); itr
								.hasNext();) {
							Entry<String, ColumnFamilyStoreMBean> e = itr
									.next();
							String key = e.getKey();
							ColumnFamilyStoreMBean value = e.getValue();
							Map<String, String> map = new HashMap<String, String>();
							map.put("columnFamilyName", value
									.getColumnFamilyName());
							map.put("writeCount", String.valueOf(value
									.getWriteCount()));
							map.put("readCount", String.valueOf(value
									.getReadCount()));
							map.put("liveDiskSpaceUsed", String.valueOf(value
									.getLiveDiskSpaceUsed()));
							map.put("liveSSTableCount", String.valueOf(value
									.getLiveSSTableCount()));
							map.put("maxRowCompactedSize", String.valueOf(value
									.getMaxRowCompactedSize()));
							map.put("meanRowCompactedSize", String
									.valueOf(value.getMeanRowCompactedSize()));
							map.put("memtableColumnsCount", String
									.valueOf(value.getMemtableColumnsCount()));
							map.put("memtableDataSize", String.valueOf(value
									.getMemtableDataSize()));
							map.put("memtableSwitchCount", String.valueOf(value
									.getMemtableSwitchCount()));
							map.put("minRowCompactedSize", String.valueOf(value
									.getMinRowCompactedSize()));
							map.put("pendingTasks", String.valueOf(value
									.getPendingTasks()));
							map.put("recentReadLatencyMicros",
									String.valueOf(value
											.getRecentReadLatencyMicros()));
							map.put("recentWriteLatencyMicros", String
									.valueOf(value
											.getRecentWriteLatencyMicros()));
							map.put("totalDiskSpaceUsed", String.valueOf(value
									.getTotalDiskSpaceUsed()));
							map
									.put(
											"totalReadLatencyMicros",
											String
													.valueOf(value
															.getTotalReadLatencyMicros()));
							map.put("totalWriteLatencyMicros",
									String.valueOf(value
											.getTotalWriteLatencyMicros()));
							results.put(key, JSONValue.toJSONString(map));
						}

					}
				});
		return toJson(results);
	}

	private static Map<String, String> invokeNodeProbe(NodeProbe nodeProbe,
			NodeProbeTask task) {
		Map<String, String> results = new HashMap<String, String>();
		try {
			task.invoke(nodeProbe, results);
			results.put("status", "sucess");
		} catch (Exception e) {
			results.put("status", "fail:" + e.getMessage());
		}
		return results;
	}

	private static interface NodeProbeTask {

		void invoke(NodeProbe nodeProbe, Map<String, String> results)
				throws Exception;
	}

	@GET
	@ActionPath
	public Navigation compactionThreshold(final @In NodeProbe nodeProbe,
			HttpServletResponse res) {
		ServletOutputStream os = null;
		try {
			os = res.getOutputStream();
			PrintStream outs = new PrintStream(os);
			nodeProbe.getCompactionThreshold(outs);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return NoOperation.INSTANCE;
	}

	@GET
	@ActionPath
	public Navigation currentGenerationNumber(final @In NodeProbe nodeProbe,
			final @RequestParam("format") Format format) {
		return process(format, new Integer(nodeProbe
				.getCurrentGenerationNumber()));
	}

	@GET
	@ActionPath
	public Navigation endPoints(final @In NodeProbe nodeProbe,
			final @RequestParam("format") Format format,
			@RequestParam("key") String key, @RequestParam("table") String table) {
		final List<InetAddress> endPoints = nodeProbe.getEndPoints(key, table);
		return process(format, endPoints);
	}

	@GET
	@ActionPath
	public Navigation filesDestinedFor(final @In NodeProbe nodeProbe,
			final @RequestParam("format") Format format,
			@RequestParam("hostname") String hostname) {
		List<String> files = null;
		try {
			InetAddress address = InetAddress.getByName(hostname);
			files = nodeProbe.getFilesDestinedFor(address);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return process(format, files);
	}

	@GET
	@ActionPath
	public Navigation heap(final @In NodeProbe nodeProbe,
			final @RequestParam("format") Format format) {
		MemoryUsage heap = nodeProbe.getHeapMemoryUsage();
		return process(format, heap);
	}

	@GET
	@ActionPath
	public Navigation incomingFiles(final @In NodeProbe nodeProbe,
			final @RequestParam("format") Format format,
			@RequestParam("hostname") String hostname) {
		List<String> files = null;
		try {
			InetAddress address = InetAddress.getByName(hostname);
			files = nodeProbe.getFilesDestinedFor(address);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return process(format, files);
	}

	@GET
	@ActionPath
	public Navigation keycache(final @In NodeProbe nodeProbe,
			final @RequestParam("format") Format format,
			final @RequestParam("table") String table,
			final @RequestParam("cf") String cf) {
		JMXInstrumentedCacheMBean keycache = nodeProbe.getKeyCacheMBean(table,
				cf);
		return process(format, keycache);
	}

	@GET
	@ActionPath
	public Navigation rowcache(final @In NodeProbe nodeProbe,
			final @RequestParam("format") Format format,
			final @RequestParam("table") String table,
			final @RequestParam("cf") String cf) {
		JMXInstrumentedCacheMBean rowcache = nodeProbe.getRowCacheMBean(table,
				cf);
		return process(format, rowcache);
	}

	@GET
	@ActionPath
	public Navigation loadMap(final @In NodeProbe nodeProbe,
			final @RequestParam("format") Format format) {
		Map<String, String> loadMap = nodeProbe.getLoadMap();
		return process(format, loadMap);
	}

	@GET
	@ActionPath
	public Navigation load(final @In NodeProbe nodeProbe,
			final @RequestParam("format") Format format) {
		String s = nodeProbe.getLoadString();
		return process(format, s);
	}

	@GET
	@ActionPath
	public Navigation rangeToEndPointMap(final @In NodeProbe nodeProbe,
			final @RequestParam("format") Format format,
			final @RequestParam("table") String table) {
		Map<Range, List<String>> map = nodeProbe.getRangeToEndPointMap(table);
		return process(format, map);
	}

	@GET
	@ActionPath
	public Navigation operationMode(final @In NodeProbe nodeProbe,
			final @RequestParam("format") Format format) {
		String opmode = nodeProbe.getOperationMode();
		return process(format, opmode);
	}

	@GET
	@ActionPath
	public Navigation streamDestinations(final @In NodeProbe nodeProbe,
			final @RequestParam("format") Format format) {
		Set<InetAddress> dest = nodeProbe.getStreamDestinations();
		return process(format, dest);
	}

	@GET
	@ActionPath
	public Navigation threadPoolMBeanProxies(final @In NodeProbe nodeProbe,
			final @RequestParam("format") Format format) {
		Map<String, Map<String, String>> ret = new HashMap<String, Map<String, String>>();
		for (Iterator<Entry<String, IExecutorMBean>> itr = nodeProbe
				.getThreadPoolMBeanProxies(); itr.hasNext();) {
			Entry<String, IExecutorMBean> e = itr.next();
			String key = e.getKey();
			IExecutorMBean value = e.getValue();
			Map<String, String> map = new HashMap<String, String>();
			map.put("activeCount", String.valueOf(value.getActiveCount()));
			map.put("completeTasks", String.valueOf(value.getCompletedTasks()));
			map.put("pendingTasks", String.valueOf(value.getPendingTasks()));
			ret.put(key, map);
		}
		return process(format, ret);
	}

	@GET
	@ActionPath
	public Navigation token(final @In NodeProbe nodeProbe,
			final @RequestParam("format") Format format) {
		String token = nodeProbe.getToken();
		return process(format, token);
	}

	@GET
	@ActionPath
	public Navigation clusterName(@In CassandraClient client) {
		String clusterName = client.describeClusterName();
		return toJson(clusterName);
	}

	@GET
	@ActionPath("/keyspace/{keyspace}")
	public Navigation keyspace(@Var("keyspace") String keyspace,
			@In CassandraClient client) {
		Map<String, Map<String, String>> keyspaces = client
				.describeKeyspace(keyspace);
		return toJson(keyspaces);
	}

	@GET
	@ActionPath
	public Navigation keyspaces(@In CassandraClient client) {
		Set<String> set = client.describeKeyspaces();
		return toJson(set);
	}

	@GET
	@ActionPath("/ring/{keyspace}")
	public Navigation ring(@Var("keyspace") String keyspace,
			@In CassandraClient client) {
		List<TokenRange> rings = client.describeRing(keyspace);
		return toJson(rings);
	}

	@GET
	@ActionPath
	public Navigation config(@In CassandraClient client) {
		String configContents = client.getConfigContents();
		return SimpleText.out(configContents);
	}

	@GET
	@ActionPath
	public Navigation tokenmap(@In CassandraClient client) {
		String tokenMapAsString = client.getTokenMap();
		return toJson(tokenMapAsString);
	}

}
