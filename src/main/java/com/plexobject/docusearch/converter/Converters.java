package com.plexobject.docusearch.converter;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.query.QueryPolicy;

/**
 * @author bhatti@plexobject.com
 * 
 */
public final class Converters {
	private static final class Key {
		private final Class<?> from;
		private final Class<?> to;

		private Key(final Class<?> from, final Class<?> to) {
			this.from = from;
			this.to = to;
		}

		/**
		 * @see java.lang.Object#equals(Object)
		 */
		@Override
		public boolean equals(Object object) {
			if (!(object instanceof Key)) {
				return false;
			}
			Key rhs = (Key) object;
			return new EqualsBuilder().append(this.from, rhs.from).append(
					this.to, rhs.to).isEquals();
		}

		/**
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			return new HashCodeBuilder(786529047, 1924536713).append(this.from)
					.append(this.to).toHashCode();
		}

		/**
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return new ToStringBuilder(this).append("from", this.from).append(
					"to", this.to).toString();
		}
	}

	private final Map<Key, Converter<?, ?>> converters = new HashMap<Key, Converter<?, ?>>();
	private static Converters instance = new Converters();

	private Converters() {
		register(JSONObject.class, Object.class, new JsonToJavaConverter());
		register(JSONObject.class, Map.class, new JsonToMapConverter());
		register(JSONArray.class, Object.class, new JsonToJavaConverter());
		register(Object.class, JSONObject.class, new JavaToJsonConverter());
		register(Object.class, JSONArray.class, new JavaToJsonConverter());
		//
		register(JSONObject.class, Document.class,
				new JsonToDocumentConverter());
		register(Document.class, JSONObject.class, new JavaToJsonConverter());

		//
		register(IndexPolicy.class, JSONObject.class, new IndexPolicyToJson());
		register(JSONObject.class, IndexPolicy.class, new JsonToIndexPolicy());
		register(IndexPolicy.class, Map.class, new IndexPolicyToMap());
		register(Map.class, IndexPolicy.class, new MapToIndexPolicy());

		//
		register(QueryPolicy.class, JSONObject.class, new QueryPolicyToJson());
		register(JSONObject.class, QueryPolicy.class, new JsonToQueryPolicy());
		register(QueryPolicy.class, Map.class, new QueryPolicyToMap());
		register(Map.class, QueryPolicy.class, new MapToQueryPolicy());

	}

	public static Converters getInstance() {
		return instance;
	}

	public void register(final Class<?> from, final Class<?> to,
			final Converter<?, ?> converter) {
		if (from == null) {
			throw new NullPointerException("from class not specified");
		}
		if (to == null) {
			throw new NullPointerException("to class not specified");
		}
		if (converter == null) {
			throw new NullPointerException("converter not specified");
		}

		converters.put(new Key(from, to), converter);
	}

	@SuppressWarnings("unchecked")
	public <FROM, TO> Converter<FROM, TO> getConverter(final Class<FROM> from,
			final Class<TO> to) {
		if (from == null) {
			throw new NullPointerException("from class not specified");
		}
		if (to == null) {
			throw new NullPointerException("to class not specified");
		}

		return (Converter<FROM, TO>) converters.get(new Key(from, to));
	}
}
