package com.plexobject.docusearch.service.impl;

import java.io.File;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.RepositoryFactory;
import com.plexobject.docusearch.query.Query;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.SearchDocList;
import com.plexobject.docusearch.query.lucene.QueryImpl;
import com.plexobject.docusearch.service.SearchService;

@Path("/search")
public class SearchServiceImpl implements SearchService {
	private static final Logger LOGGER = Logger
			.getLogger(SearchServiceImpl.class);
	private final ConfigurationRepository configRepository;
	private final DocumentRepository docRepository;

	public SearchServiceImpl() {
		this(new RepositoryFactory());
	}

	public SearchServiceImpl(final RepositoryFactory repositoryFactory) {
		this.docRepository = repositoryFactory.getDocumentRepository();
		this.configRepository = repositoryFactory.getConfigurationRepository();
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes( { MediaType.TEXT_HTML, MediaType.APPLICATION_JSON })
	@Path("{index}")
	@Override
	public Response query(@PathParam("index") final String index,
			@QueryParam("keywords") final String keywords,
			@QueryParam("start") final int start,
			@QueryParam("limit") final int limit,
			@QueryParam("detailedResults") final boolean detailedResults) {
		if (GenericValidator.isBlankOrNull(index)) {
			return Response.status(500).type("text/plain").entity(
					"index not specified").build();
		}
		if (index.contains("\"")) {
			return Response.status(500).type("text/plain").entity(
					"index name is valid " + index + "\n").build();
		}

		if (GenericValidator.isBlankOrNull(keywords)) {
			return Response.status(500).type("text/plain").entity(
					"keywrods not specified").build();
		}

		try {

			QueryPolicy policy = configRepository.getQueryPolicy(index);
			final QueryCriteria criteria = new QueryCriteria().setKeywords(
					keywords).addFields(policy.getFields());

			final File dir = new File(LuceneUtils.INDEX_DIR, index);

			Query query = newQueryImpl(dir);

			SearchDocList results = query.search(criteria, start, limit);
			JSONArray docs = new JSONArray();
			for (SearchDoc result : results) {
				if (detailedResults) {
					Document doc = docRepository.getDocument(index, result
							.getId());
					JSONObject jsonDoc = Converters.getInstance().getConverter(
							Object.class, JSONObject.class).convert(doc);
					docs.put(jsonDoc);
				} else {
					JSONObject jsonDoc = Converters.getInstance().getConverter(
							Object.class, JSONObject.class).convert(result);
					docs.put(jsonDoc);
				}
			}
			final JSONObject response = new JSONObject();
			response.put("keywords", keywords);
			response.put("start", start);
			response.put("limit", limit);
			response.put("totalHits", results.getTotalHits());
			response.put("docs", docs);

			if (LOGGER.isInfoEnabled()) {
				LOGGER.info("Found " + results.getTotalHits()
						+ " hits for index " + index + ", detailed "
						+ detailedResults + ", start " + start + ", limit "
						+ limit);
			}
			return Response.ok(response.toString()).build();

		} catch (Exception e) {
			LOGGER.error("failed to query " + index + " with " + keywords
					+ " from " + start + "/" + limit, e);
			return Response.status(500).type("text/plain").entity(
					"failed to query " + index + " with " + keywords + " from "
							+ start + "/" + limit + "\n").build();
		}
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes( { MediaType.TEXT_HTML, MediaType.APPLICATION_JSON })
	@Path("{index}/{id}")
	@Override
	public Response get(@PathParam("index") final String index,
			@PathParam("id") final String id) {
		if (GenericValidator.isBlankOrNull(index)) {
			return Response.status(500).type("text/plain").entity(
					"index not specified").build();
		}
		if (index.contains("\"")) {
			return Response.status(500).type("text/plain").entity(
					"index name is valid " + index + "\n").build();
		}

		if (GenericValidator.isBlankOrNull(id)) {
			return Response.status(500).type("text/plain").entity(
					"id not specified").build();
		}

		try {

			Document doc = docRepository.getDocument(index, id);
			JSONObject jsonDoc = Converters.getInstance().getConverter(
					Object.class, JSONObject.class).convert(doc);
			return Response.ok(jsonDoc.toString()).build();

		} catch (Exception e) {
			LOGGER.error("failed to get details for " + index + " with " + id,
					e);
			return Response.status(500).type("text/plain")
					.entity(
							"failed to get details for " + index + " with "
									+ id + "\n").build();
		}
	}

	protected Query newQueryImpl(final File dir) {
		return new QueryImpl(dir);
	}

}
