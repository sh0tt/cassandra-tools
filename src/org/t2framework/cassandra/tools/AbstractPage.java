package org.t2framework.cassandra.tools;

import org.t2framework.t2.annotation.core.RequestParam;
import org.t2framework.t2.navigation.SimpleText;
import org.t2framework.t2.spi.Navigation;

import com.github.GsonPrettyPrinter;
import com.google.gson.Gson;

public abstract class AbstractPage {

	protected Navigation toJson(Object o) {
		// Gson gson = new Gson();
		// String json = JSONValue.toJSONString(o);
		String json = new GsonPrettyPrinter(new Gson()).ppJson(o);
		// String json = gson.toJson(o);
		return SimpleText.out(json);
	}

	protected Navigation process(@RequestParam("format") Format format,
			Object ret) {
		if (format == null || format == Format.json) {
			return toJson(ret);
		} else if (format == Format.text) {
			return SimpleText.out(ret.toString());
		} else if (format == Format.xml) {
			// TODO
			return SimpleText.out(ret.toString());
		} else {
			return toJson(ret);
		}
	}

}
