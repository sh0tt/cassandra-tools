package org.t2framework.cassandra.tools;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
import org.t2framework.t2.annotation.composite.GET;
import org.t2framework.t2.annotation.core.ActionPath;
import org.t2framework.t2.annotation.core.In;
import org.t2framework.t2.annotation.core.Page;
import org.t2framework.t2.annotation.core.RequestParam;
import org.t2framework.t2.annotation.core.Var;
import org.t2framework.t2.spi.Navigation;

@Page("/search")
public class SearchPage extends AbstractPage {

	protected static Logger LOG = Logger.getLogger(SearchPage.class);

	// @GET
	// @ActionPath("/find/{keyspace}/{columnFamily}/{row}/{column}")
	// public Navigation find(@In CassandraClient client,
	// @Var("keyspace") String keyspace,
	// @Var("columnFamily") String columnFamily, @Var("row") String row,
	// @Var("column") String column, @RequestParam("format") Format format) {
	// try {
	// ColumnOrSuperColumn one = client.selectOne(keyspace, columnFamily,
	// row, column);
	// SuperColumn superColumn = one.getSuper_column();
	// if (superColumn == null) {
	// Column c = one.getColumn();
	// Map<String, String> map = getColumnMap(c);
	// return process(format, map);
	// }
	// } catch (InvalidRequestException e) {
	// e.printStackTrace();
	// } catch (NotFoundException e) {
	// e.printStackTrace();
	// } catch (UnavailableException e) {
	// e.printStackTrace();
	// } catch (TimedOutException e) {
	// e.printStackTrace();
	// } catch (TException e) {
	// e.printStackTrace();
	// }
	// return process(format, "error");
	// }

	@GET
	@ActionPath("/findAll/{keyspace}/{columnFamily}/")
	public Navigation findAll(@In CassandraClient client,
			@Var("keyspace") String keyspace,
			@Var("columnFamily") String columnFamily,
			@RequestParam("format") Format format) {
		try {
			Client nativeClient = client.getNativeClient();

			SlicePredicate predicate = new SlicePredicate();
			SliceRange slice = new SliceRange(new byte[] {}, new byte[] {},
					false, 100);
			predicate.setSlice_range(slice);
			KeyRange keyrange = new KeyRange();
			keyrange.setStart_key("");
			keyrange.setEnd_key("");
			List<KeySlice> slices = nativeClient.get_range_slices(keyspace,
					new ColumnParent(columnFamily), predicate, keyrange,
					ConsistencyLevel.ONE);

			Map<String, Object> ret = new HashMap<String, Object>();
			for (KeySlice keySlice : slices) {
				String key = keySlice.getKey();
				LOG.debug(key);

				Map<String, Map<String, Map<String, String>>> cscmap = new HashMap<String, Map<String, Map<String, String>>>();
				Map<String, Map<String, String>> singlemap = new HashMap<String, Map<String, String>>();
				for (ColumnOrSuperColumn csc : keySlice.getColumns()) {
					SuperColumn superColumn = csc.getSuper_column();
					if (superColumn != null) {
						final String superColumnName = new String(superColumn
								.getName());
						LOG.debug("\tsuper column name : " + superColumnName);
						Map<String, Map<String, String>> scmap = new LinkedHashMap<String, Map<String, String>>();
						for (Column c : superColumn.getColumns()) {
							Map<String, String> columnMap = getColumnMap(c);
							scmap.put(new String(c.name), columnMap);
						}
						cscmap.put(superColumnName, scmap);
					} else {
						Column c = csc.getColumn();
						Map<String, String> columnMap = getColumnMap(c);
						singlemap.put(new String(c.name), columnMap);
					}
				}
				if (cscmap.isEmpty() == false) {
					ret.put(key, cscmap);
				} else {
					ret.put(key, singlemap);
				}
			}
			return process(format, ret);
		} catch (InvalidRequestException e) {
			e.printStackTrace();
		} catch (UnavailableException e) {
			e.printStackTrace();
		} catch (TimedOutException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		return process(format, "error");
	}

	protected Map<String, String> getColumnMap(Column c) {
		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("name", new String(c.name));
		map.put("value", new String(c.value));
		map.put("timestamp", String.valueOf(c.timestamp));
		return map;
	}

}
