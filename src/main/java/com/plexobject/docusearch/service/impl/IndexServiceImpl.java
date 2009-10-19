package com.plexobject.docusearch.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.Logger;

import com.plexobject.docusearch.docs.DocumentsDatabaseIndexer;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.RepositoryFactory;
import com.plexobject.docusearch.service.IndexService;

@Path("/index")
public class IndexServiceImpl implements IndexService {
	private static final Logger LOGGER = Logger
			.getLogger(IndexServiceImpl.class);

	private final DocumentRepository docRepository;
	private final DocumentsDatabaseIndexer documentsDatabaseIndexer;

	public IndexServiceImpl() {
		this(new RepositoryFactory());
	}

	public IndexServiceImpl(final RepositoryFactory repositoryFactory) {
		this(repositoryFactory, new DocumentsDatabaseIndexer(repositoryFactory));
	}

	public IndexServiceImpl(final RepositoryFactory repositoryFactory,
			final DocumentsDatabaseIndexer documentsDatabaseIndexer) {
		if (repositoryFactory == null) {
			throw new NullPointerException("repositoryFactory not specified");
		}
		if (documentsDatabaseIndexer == null) {
			throw new NullPointerException(
					"documentsDatabaseIndexer not specified");
		}
		this.docRepository = repositoryFactory.getDocumentRepository();
		this.documentsDatabaseIndexer = documentsDatabaseIndexer;
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes( { MediaType.WILDCARD })
	@Path("{index}")
	@Override
	public Response create(@PathParam("index") final String index) {
		if (GenericValidator.isBlankOrNull(index)) {
			return Response.status(500).type("text/plain").entity(
					"index not specified\n").build();
		}
		if (index.contains("\"")) {
			return Response.status(500).type("text/plain").entity(
					"index name is valid " + index + "\n").build();
		}

		try {
			documentsDatabaseIndexer.indexDatabase(index);
			return Response.ok().entity("rebuilt index for " + index + "\n")
					.build();
		} catch (Exception e) {
			LOGGER.error("failed to create index " + index, e);
			return Response.status(500).type("text/plain").entity(
					"failed to create index " + index + "\n").build();
		}
	}

	@PUT
	@Produces("application/json")
	@Consumes( { "*/*" })
	@Path("{index}")
	@Override
	public Response update(@PathParam("index") final String index,
			@QueryParam("docIds") final String docIds) {
		if (GenericValidator.isBlankOrNull(index)) {
			return Response.status(500).type("text/plain").entity(
					"index not specified\n").build();
		}
		if (index.contains("\"")) {
			return Response.status(500).type("text/plain").entity(
					"index name is valid " + index + "\n").build();
		}
		if (GenericValidator.isBlankOrNull(docIds)) {
			return Response.status(500).type("text/plain").entity(
					"docIds not specified\n").build();
		}
		final String[] ids = docIds.split(",");
		for (String id : ids) {
			if (GenericValidator.isBlankOrNull(id)) {
				return Response.status(500).type("text/plain").entity(
						"empty docId specified in " + Arrays.asList(docIds)
								+ "\n").build();
			}
		}

		try {
			final Collection<Document> docs = new ArrayList<Document>();
			for (String id : ids) {
				Document doc = docRepository.getDocument(index, id);
				docs.add(doc);
			}
			int succeeded = documentsDatabaseIndexer
					.indexDocuments(index, docs);
			return Response.ok().entity(
					"updated " + succeeded + " documents in index for " + index
							+ " with ids " + docIds + "\n").build();
		} catch (Exception e) {
			LOGGER.error("failed to update index " + index, e);
			return Response.status(500).type("text/plain").entity(
					"failed to update index " + index + "\n").build();
		}

	}

}
