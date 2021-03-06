package com.plexobject.docusearch.docs;

import java.util.Collection;
import java.util.List;

import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.Pair;

public interface DocumentsDatabaseIndexer {
    int indexUsingPrimaryDatabase(final String index,
            final String sourceDatabase, final String policyName);

    int updateIndexUsingPrimaryDatabase(final String index,
            final String policyName, final String[] docIds);

    int updateIndexUsingPrimaryDatabase(final String index,
            final String policyName, final List<Document> docs);

    int indexUsingSecondaryDatabase(final String index,
            final String policyName, final String sourceDatabase,
            final String joinDatabase, final String indexIdInJoinDatabase,
            final String sourceIdInJoinDatabase);

    int updateIndexUsingSecondaryDatabase(final String index,
            final String policyName, final String sourceDatabase,
            final String joinDatabase, final String indexIdInJoinDatabase,
            final String sourceIdInJoinDatabase, final String[] docIds);

    int removeIndexedDocuments(String index, String database,
            String secondaryIdName,
            Collection<Pair<String, String>> primaryAndSecondaryIds,
            int olderThanDays);

}