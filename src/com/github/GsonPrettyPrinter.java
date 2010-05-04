package com.github;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * A decent pretty-printer for Gson. Features include:
 * 
 * <ul>
 * <li>Will try to use a single line where possible</li>
 * <li>Configurable indent string</li>
 * </ul>
 * 
 * This file is (c) Copyright 2009 Uprizer Labs LLC It is released to the public under the Lesser GNU Public License
 * v3.0 See http://www.gnu.org/licenses/lgpl.html for details.
 * 
 * @author Ian Clarke <ian@uprizer.com>, minor additions by Eric Poggel (NuOrbit Media, nuorbit.com)
 * @see http://gist.github.com/212791, http://blog.locut.us/main/2009/10/18/a-good-pretty-printer-for-google-gson.html
 * 
 * @example
 * new GsonPrettyPrinter(gb.create()).ppJson(some_object);
 */

public class GsonPrettyPrinter {

	private static <V> SortedSet<Entry<String, V>> orderEntrySet(final Set<Entry<String, V>> es) {
		final TreeSet<Entry<String, V>> ret = new TreeSet<Entry<String, V>>(new Comparator<Entry<String, V>>() {

			public int compare(final Entry<String, V> o1, final Entry<String, V> o2) {
				return o1.getKey().compareTo(o2.getKey());
			}
		});
		ret.addAll(es);
		return ret;
	}

	private final Gson gson;

	private static final int MAX_LEN_FOR_ONE_LINE = 150;

	private final String indent;

	public GsonPrettyPrinter(final Gson gson) {
		this(gson, " ");
	}

	public GsonPrettyPrinter(final Gson gson, final String indent) {
		this.gson = gson;
		this.indent = indent;
	}
	
	public String ppJson(final Object o) {
		return ppJson(o, null);
	}

	public String ppJson(final Object o, Type t) {
		if (t==null)
			t = o.getClass();		
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		ppJson(o, pw, t);
		return sw.toString();
	}

	public void ppJson(final Object o, final PrintWriter pw, Type t) {
		final JsonElement jsonTree = gson.toJsonTree(o, t);
		final List<String> stringList = toStringList(jsonTree);
		for (final String s : stringList) {
			pw.println(s);
		}
	}

	private List<String> arrayToStringList(final JsonArray jsonArray) {
		final List<List<String>> sll = new LinkedList<List<String>>();
		for (final JsonElement arrayElement : jsonArray) {
			sll.add(toStringList(arrayElement));
		}
		// Can we fit the array on a single line?
		boolean oneLine = true;
		int approxLineLength = 0;
		for (final List<String> sl : sll) {
			if (sl.size() > 1) {
				oneLine = false;
				break;
			}
			approxLineLength += sl.get(0).length() + 2;
		}
		if (approxLineLength > MAX_LEN_FOR_ONE_LINE) {
			oneLine = false;
		}
		if (oneLine) {
			final StringBuffer sb = new StringBuffer(approxLineLength * 2 + 10);
			sb.append("[");
			boolean first = true;
			for (final List<String> sl : sll) {
				if (!first) {
					sb.append(", ");
				}
				first = false;
				sb.append(sl.get(0));
			}
			sb.append("]");
			return Collections.singletonList(sb.toString());
		}
		final LinkedList<String> ret = new LinkedList<String>();
		ret.add("[");
		int oPos = 0;
		for (final List<String> sl : sll) {
			oPos++;
			final int slLen = sl.size();
			int pos = 0;
			for (final String s : sl) {
				pos++;
				final boolean needComma = (pos == slLen && oPos < sll.size());
				ret.add(indent + s + (needComma ? ", " : ""));
			}
		}
		ret.add("]");
		return ret;
		
	}

	private List<String> objectToStringList(final JsonObject jsonObject) {
		final Map<String, List<String>> slm = new HashMap<String, List<String>>();
		for (final Entry<String, JsonElement> e : jsonObject.entrySet()) {
			slm.put(e.getKey(), toStringList(e.getValue()));
		}
		// Can we fit the object on a single line?
		boolean oneLine = true;
		int approxLineLength = 0;
		for (final Entry<String, List<String>> e : slm.entrySet()) {
			if (e.getValue().size() > 1) {
				oneLine = false;
				break;
			}
			approxLineLength += e.getKey().length() + e.getValue().get(0).length() + 6;
		}
		if (approxLineLength > MAX_LEN_FOR_ONE_LINE) {
			oneLine = false;
		}
		if (oneLine) {
			final StringBuffer sb = new StringBuffer(approxLineLength * 2 + 10);
			sb.append("{");
			boolean first = true;
			for (final Entry<String, List<String>> e : orderEntrySet(slm.entrySet())) {
				if (!first) {
					sb.append(", ");
				}
				first = false;
				sb.append("\"" + e.getKey() + "\" : " + e.getValue().get(0));
			}
			sb.append("}");
			return Collections.singletonList(sb.toString());
		}
		final LinkedList<String> ret = new LinkedList<String>();
		ret.add("{");
		int oPos = 0;
		for (final Entry<String, List<String>> e : orderEntrySet(slm.entrySet())) {
			oPos++;
			final int slLen = e.getValue().size();
			int pos = 0;
			for (final String s : e.getValue()) {
				pos++;
				final boolean needComma = (pos == slLen && oPos < slm.size());
				if (pos == 1) {
					ret.add(indent + "\"" + e.getKey() + "\" : " + s + (needComma ? ", " : ""));
				} else {
					ret.add(indent + s + (needComma ? ", " : ""));
				}
			}
		}
		ret.add("}");
		return ret;
	}

	private List<String> toStringList(final JsonElement je) {
		if (je==null || je.isJsonNull())
			return new ArrayList<String>(Arrays.asList(new String[]{"null"}));
		if (je.isJsonPrimitive())
			return Collections.singletonList(je.getAsJsonPrimitive().toString());
		if (je.isJsonArray()) {
			final JsonArray jsonArray = je.getAsJsonArray();
			return arrayToStringList(jsonArray);
		} else if (je.isJsonObject()) {
			final JsonObject jsonObject = je.getAsJsonObject();
			return objectToStringList(jsonObject);
		}
		throw new RuntimeException("Unsupported Json element: " + je.getClass().getName());
	}
}