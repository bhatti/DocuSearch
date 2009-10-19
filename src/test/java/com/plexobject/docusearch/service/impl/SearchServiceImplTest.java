package com.plexobject.docusearch.service.impl;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.RepositoryFactory;
import com.plexobject.docusearch.query.Query;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.SearchDocList;
import com.plexobject.docusearch.service.SearchService;
import com.plexobject.docusearch.service.impl.SearchServiceImpl;

public class SearchServiceImplTest {
	private static final String PEOPLE = "people";
	private static final String DB_NAME = "MYDB";
	private static final int MAX_LIMIT = 2048;

	static class StubSearchServiceImpl extends SearchServiceImpl {
		final Query stubQuery;

		StubSearchServiceImpl(final DocumentRepository docRepository,
				final ConfigurationRepository configRepository,
				final Query stubQuery) {
			super(new RepositoryFactory(docRepository, configRepository));
			this.stubQuery = stubQuery;
		}

		@Override
		protected Query newQueryImpl(final File dir) {
			return stubQuery;
		}
	}

	private DocumentRepository repository;
	private ConfigurationRepository configRepository;
	private Query query;
	private SearchService service;

	@Before
	public void setUp() throws Exception {
		repository = EasyMock.createMock(DocumentRepository.class);
		configRepository = EasyMock.createMock(ConfigurationRepository.class);
		query = EasyMock.createMock(Query.class);
		service = new StubSearchServiceImpl(repository, configRepository, query);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testQuery() throws JSONException {
		final QueryPolicy policy = newQueryPolicy();
		final QueryCriteria criteria = new QueryCriteria().setKeywords(
				"keywords").addFields(policy.getFields());
		EasyMock.expect(configRepository.getQueryPolicy(PEOPLE)).andReturn(
				policy);
		EasyMock.expect(query.search(criteria, 0, MAX_LIMIT)).andReturn(
				newDocuments());
		EasyMock.expect(repository.getDocument(PEOPLE, "id")).andReturn(
				newDocument());

		EasyMock.replay(repository);
		EasyMock.replay(configRepository);
		EasyMock.replay(query);

		Response response = service.query(PEOPLE, "keywords", 0, MAX_LIMIT,
				true);
		EasyMock.verify(repository);
		EasyMock.verify(configRepository);
		EasyMock.verify(query);

		Assert.assertEquals(200, response.getStatus());

		JSONObject jsonResponse = new JSONObject(response.getEntity()
				.toString());
		Assert.assertEquals("keywords", jsonResponse.getString("keywords"));
		Assert.assertEquals("0", jsonResponse.getString("start"));
		Assert.assertEquals(String.valueOf(MAX_LIMIT), jsonResponse
				.getString("limit"));
		Assert.assertEquals("1", jsonResponse.getString("totalHits"));
		JSONArray jsonDocs = jsonResponse.getJSONArray("docs");
		JSONObject jsonDoc = jsonDocs.getJSONObject(0);
		Assert.assertEquals("1", jsonDoc.getString("A"));
		Assert.assertEquals("2", jsonDoc.getString("B"));
		Assert
				.assertEquals("{\"Y\":\"8\",\"Z\":\"9\"}", jsonDoc
						.getString("C"));
		Assert.assertEquals("[11,\"12\",13,\"14\"]", jsonDoc.getString("D"));
	}

	@Test
	public final void testGet() throws JSONException {
		EasyMock.expect(repository.getDocument(PEOPLE, "id")).andReturn(
				newDocument());

		EasyMock.replay(repository);
		EasyMock.replay(configRepository);
		EasyMock.replay(query);

		Response response = service.get(PEOPLE, "id");
		EasyMock.verify(repository);
		EasyMock.verify(configRepository);
		EasyMock.verify(query);

		Assert.assertEquals(200, response.getStatus());
		JSONObject jsonDoc = new JSONObject(response.getEntity().toString());
		Assert.assertEquals("1", jsonDoc.getString("A"));
		Assert.assertEquals("2", jsonDoc.getString("B"));
		Assert
				.assertEquals("{\"Y\":\"8\",\"Z\":\"9\"}", jsonDoc
						.getString("C"));
		Assert.assertEquals("[11,\"12\",13,\"14\"]", jsonDoc.getString("D"));

	}

	private static QueryPolicy newQueryPolicy() {
		final QueryPolicy policy = new QueryPolicy();

		for (int i = 0; i < 10; i++) {
			policy.add("name" + i);
		}
		return policy;
	}

	@SuppressWarnings("unchecked")
	private Document newDocument() {
		final Map<String, String> map = new TreeMap<String, String>();
		map.put("Y", "8");
		map.put("Z", "9");
		final List<? extends Object> arr = Arrays.asList(11, "12", 13, "14");

		final Document doc = new DocumentBuilder(DB_NAME).setId("id").put("A",
				"1").put("B", "2").put("C", map).put("D", arr).build();
		return doc;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> newMapDocument() {
		final Map<String, Object> doc = new TreeMap<String, Object>();
		doc.put(Document.DATABASE, DB_NAME);
		doc.put(Document.ID, "id");

		doc.put("A", "1");
		doc.put("B", "2");
		final Map<String, String> map = new TreeMap<String, String>();
		map.put("Y", "8");
		map.put("Z", "9");
		doc.put("C", map);
		final List<? extends Object> arr = Arrays.asList(11, "12", 13, "14");
		doc.put("D", arr);

		return doc;
	}

	private SearchDocList newDocuments() {
		final SearchDoc result = new SearchDoc(newMapDocument());

		return new SearchDocList(1, Arrays.asList(result));

	}
}
