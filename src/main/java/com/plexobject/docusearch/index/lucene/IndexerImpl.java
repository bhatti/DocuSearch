package com.plexobject.docusearch.index.lucene;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.index.Indexer;
import com.plexobject.docusearch.lucene.LuceneUtils;

/**
 * @author bhatti@plexobject.com
 * 
 */
public class IndexerImpl implements Indexer {
	private static final Logger LOGGER = Logger.getLogger(IndexerImpl.class);
	private static final boolean OPTIMIZE = false;
	Pattern JSON_PATTERN = Pattern.compile("[,;:\\[\\]{}()\\s]+");
	private int numIndexed;
	//
	private final Directory dir;

	public IndexerImpl(final File dir) {
		this(LuceneUtils.toFSDirectory(dir));
	}

	public IndexerImpl(final Directory dir) {
		this.dir = dir;
	}

	@Override
	public int index(final IndexPolicy policy, final Collection<Document> docs) {
		IndexWriter writer = null;
		int succeeded = 0;
		try {
			writer = createWriter();
			for (Document doc : docs) {
				try {
					index(writer, policy, doc);
					succeeded++;
				} catch (final Exception e) {
					LOGGER.error("Error indexing " + doc, e);
				}
			}
		} catch (final Exception e) {
			LOGGER.error("Error indexing documents", e);
		} finally {
			if (writer != null) {
				try {
					if (OPTIMIZE) {
						writer.optimize();
					}
				} catch (Exception e) {
					LOGGER.error(e);
				} finally {
					try {
						writer.close();
					} catch (Exception e) {
						LOGGER.error(e);
					}
				}
			}
		}
		return succeeded;
	}

	IndexWriter createWriter() throws IOException {
		return LuceneUtils.newWriter(dir);
		// return LuceneUtils.newThreadedWriter(dir);

	}

	@SuppressWarnings("deprecation")
	private void index(final IndexWriter writer, final IndexPolicy policy,
			final Document doc) throws CorruptIndexException, IOException {
		if (writer == null) {
			throw new NullPointerException("writer not specified");
		}
		if (policy == null) {
			throw new NullPointerException("index policy not specified");
		}
		if (doc == null) {
			throw new NullPointerException("document not specified");
		}

		final Map<String, Object> map = doc.getAttributes();
		final JSONObject json = Converters.getInstance().getConverter(
				Object.class, JSONObject.class).convert(map);
		final org.apache.lucene.document.Document ldoc = new org.apache.lucene.document.Document();
		if (doc.getId() == null) {
			throw new IllegalStateException("id not defined for " + doc);
		}
		ldoc.add(new Field(Document.DATABASE, doc.getDatabase(),
				Field.Store.YES, Field.Index.NOT_ANALYZED));
		ldoc.add(new Field(Document.ID, doc.getId(), Field.Store.YES,
				Field.Index.NOT_ANALYZED));

		ldoc.add(new Field("indexDate", DateTools.dateToString(new Date(),
				DateTools.Resolution.DAY), Field.Store.YES,
				Field.Index.NOT_ANALYZED));

		if (policy.getBoost() > 0) {
			ldoc.setBoost(policy.getBoost());
		}
		if (policy.getScore() > 0) {
			ldoc.add(new Field("score", Integer.toString(policy.getScore()),
					Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
		}
		for (IndexPolicy.Field field : policy.getFields()) {
			try {
				final String value = getValue(json, field.name);
				if (value != null) {
					final Field.Store store = field.storeInIndex ? Field.Store.YES
							: Field.Store.NO;
					final Field.Index index = field.tokenize ? Field.Index.TOKENIZED
							: field.analyze ? Field.Index.ANALYZED
									: Field.Index.NOT_ANALYZED;
					final Field.TermVector termVector = field.tokenize ? Field.TermVector.YES
							: Field.TermVector.NO;
					final Field locField = new Field(field.name, value, store,
							index, termVector);
					locField.setBoost(field.boost);
					ldoc.add(locField);
					if (LOGGER.isTraceEnabled()) {
						LOGGER.trace("Indexing " + field.name + " using "
								+ locField + ", doc " + doc + ", policy "
								+ policy);
					}
				}
			} catch (JSONException e) {
				LOGGER.error("Failed to index value for " + field.name
						+ " from " + json + " due to ", e);
				throw new RuntimeException(e.toString());
			}
		}
		writer.deleteDocuments(LuceneUtils.docQuery(doc.getDatabase(), doc
				.getId()));
		writer.deleteDocuments(new Term(Document.ID, doc.getId()));
		try {
			writer.addDocument(ldoc);
		} catch (RuntimeException e) {
			throw new RuntimeException("Failed to index " + ldoc, e);
		}

		// writer.updateDocument(new Term(Document.ID, doc));
		if (++numIndexed % 1000 == 0 && LOGGER.isInfoEnabled()) {
			LOGGER.info(numIndexed + ": Indexing " + ldoc + " for input " + doc
					+ " with policy " + policy);
		}
	}

	private String getValue(final JSONObject json, final String name)
			throws JSONException {
		String value = null;
		int ndx;
		if ((ndx = name.indexOf("[")) != -1) {
			value = getArrayValue(json, name, ndx);
		} else if ((ndx = name.indexOf("{")) != -1) {
			value = getHashValue(json, name, ndx);
		} else {
			value = json.optString(name);
		}
		if (value != null) {
			Matcher matcher = JSON_PATTERN.matcher(value);
			value = matcher.replaceAll(" ");
		}
		return value;
	}

	private String getHashValue(final JSONObject json, final String name,
			int ndx) throws JSONException {
		String value;
		final String tagName = name.substring(0, ndx);
		value = json.optString(tagName);
		if (value == null) {
			// do nothing
		} else if (!value.startsWith("{")) {
			throw new IllegalStateException("Failed to get hash value for "
					+ tagName + " in " + value + " from json " + json);
		} else {
			final JSONObject jsonObject = new JSONObject(value);
			final String subscript = name.substring(ndx + 1, name.indexOf("}"));
			value = jsonObject.optString(subscript);
		}
		return value;
	}

	private String getArrayValue(final JSONObject json, final String name,
			int ndx) throws JSONException {
		String value;
		final String subscript = name.substring(ndx + 1, name.indexOf("]"));

		final String tagName = name.substring(0, ndx);
		value = json.optString(tagName);
		if (value == null) {
			// do nothing
		} else if (!value.startsWith("[")) {
			if (value != null && value.startsWith("{")) {
				final JSONObject jsonObject = new JSONObject(value);
				value = jsonObject.optString(subscript);
			} else {
				LOGGER.warn("Failed to get array value for " + tagName + " in "
						+ value + " from json " + json);
			}
		} else {
			final JSONArray jsonArray = new JSONArray(value);
			try {
				int offset = Integer.parseInt(subscript);
				value = jsonArray.optString(offset);
			} catch (NumberFormatException e) {
				StringBuilder sb = new StringBuilder();
				int len = jsonArray.length();
				for (int i = 0; i < len; i++) {
					JSONObject elementJson = jsonArray.getJSONObject(i);
					sb.append(elementJson.getString(subscript));
					sb.append(" ");
				}
				value = sb.toString();
			}
		}
		return value;
	}
}
