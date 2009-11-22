package com.plexobject.docusearch.query.lucene;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.solr.analysis.SynonymMap;
import org.junit.Assert;
import org.junit.Test;

import com.plexobject.docusearch.index.lucene.IndexerImplTest;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.lucene.analyzer.DiacriticAnalyzer;
import com.plexobject.docusearch.lucene.analyzer.MetaphoneReplacementAnalyzer;
import com.plexobject.docusearch.lucene.analyzer.PorterAnalyzer;
import com.plexobject.docusearch.lucene.analyzer.SynonymAnalyzer;
import com.plexobject.docusearch.persistence.RepositoryFactory;
import com.plexobject.docusearch.query.Query;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.SearchDocList;

@SuppressWarnings( { "serial" })
public class QueryImplTest extends IndexerImplTest {
}
