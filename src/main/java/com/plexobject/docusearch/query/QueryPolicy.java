package com.plexobject.docusearch.query;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.TreeSet;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class QueryPolicy {
	private final Collection<String> fields = new HashSet<String>();

	public QueryPolicy() {
	}

	public QueryPolicy(String... fields) {
		this(Arrays.asList(fields));
	}

	public QueryPolicy(Collection<String> fields) {
		add(fields);
	}

	public void add(String... fields) {
		add(Arrays.asList(fields));
	}

	public void add(Collection<String> fields) {
		if (fields != null) {
			this.fields.addAll(fields);
		}
	}

	public Collection<String> getFields() {
		return fields;
	}

	/**
	 * @see java.lang.Object#equals(Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof QueryPolicy)) {
			return false;
		}
		QueryPolicy rhs = (QueryPolicy) object;
		return new EqualsBuilder().append(new TreeSet<String>(this.fields),
				new TreeSet<String>(rhs.fields)).isEquals();
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return new HashCodeBuilder(786529047, 1924536713).append(this.fields)
				.toHashCode();
	}

	public String toString() {
		return new ToStringBuilder(this).append("fields", fields).toString();
	}
}
