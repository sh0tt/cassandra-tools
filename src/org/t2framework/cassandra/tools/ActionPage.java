package org.t2framework.cassandra.tools;

import org.t2framework.cassandra.tools.util.CassandraClient;
import org.t2framework.t2.annotation.composite.GET;
import org.t2framework.t2.annotation.core.ActionPath;
import org.t2framework.t2.annotation.core.In;
import org.t2framework.t2.annotation.core.Page;
import org.t2framework.t2.navigation.SimpleText;
import org.t2framework.t2.spi.Navigation;

@Page("/action")
public class ActionPage {

	@GET
	@ActionPath
	public Navigation connect(@In CassandraClient client) {
		client.connect();
		return SimpleText.out("connected");
	}

	@GET
	@ActionPath
	public Navigation disconnect(@In CassandraClient client) {
		client.disconnect();
		return SimpleText.out("disconnected");
	}

}
