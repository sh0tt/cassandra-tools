package org.t2framework.cassandra.tools;

import java.io.IOException;

import javax.servlet.ServletContext;

import org.apache.cassandra.tools.NodeProbe;
import org.t2framework.cassandra.tools.util.CassandraClient;
import org.t2framework.t2.contexts.WebApplication;
import org.t2framework.t2.plugin.AbstractPlugin;

public class CassndraClientPlugin extends AbstractPlugin {

	@Override
	public void initialize(ServletContext servletContext,
			WebApplication webApplication) {
		super.initialize(servletContext, webApplication);
		String host = servletContext.getInitParameter("cassandra.host");
		int port = Integer.valueOf(
				servletContext.getInitParameter("cassandra.port")).intValue();
		CassandraClient client = new CassandraClient(host, port);
		client.connect();
		getContainerAdapter().register(client);

		int jmxport = Integer.valueOf(
				servletContext.getInitParameter("cassandra.jmx.port"))
				.intValue();
		NodeProbe probe = null;
		try {
			probe = new NodeProbe(host, jmxport);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (probe != null) {
			getContainerAdapter().register(probe);
		}
	}

	@Override
	public void destroy(ServletContext servletContext,
			WebApplication webApplication) {
		CassandraClient client = getContainerAdapter().getComponent(
				CassandraClient.class);
		if (client != null) {
			client.disconnect();
		}
		super.destroy(servletContext, webApplication);
	}

}
