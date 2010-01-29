package com.plexobject.docusearch.lucene.analyzer;

import java.io.Reader;
import java.util.Arrays;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;
import org.apache.solr.analysis.SynonymFilter;
import org.apache.solr.analysis.SynonymMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.plexobject.docusearch.docs.DocumentPropertiesHelper;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.sun.jersey.spi.inject.Inject;

@Component("synonymAnalyzer")
public class SynonymAnalyzer extends Analyzer {
    private static final boolean PORTER = false;
    private static Logger LOGGER = Logger.getLogger(SynonymAnalyzer.class);
    private static final String SYNONYMS_DATA = "synonyms.properties";
    final SynonymMap synonymMap;

    @Autowired
    @Inject
    DocumentPropertiesHelper documentPropertiesHelper = new DocumentPropertiesHelper();
    static SynonymMap defaultSynonymMap;;

    public SynonymAnalyzer() {
        this(null);
    }

    public SynonymAnalyzer(SynonymMap synonymMap) {
        if (synonymMap == null) {
            if (defaultSynonymMap == null) {
                defaultSynonymMap = getDefaultSynonymMap();
            }
            synonymMap = defaultSynonymMap;
        }
        this.synonymMap = synonymMap;
    }

    public TokenStream tokenStream(String fieldName, Reader reader) {
        TokenStream result = new SynonymFilter(
                new StopFilter(true,
                        new LowerCaseFilter(new StandardFilter(
                                new StandardTokenizer(Version.LUCENE_CURRENT,
                                        reader))), LuceneUtils.STOP_WORDS_SET),
                synonymMap);
        if (PORTER) {
            result = new PorterStemFilter(result);
	}
        return result;
    }

    private SynonymMap getDefaultSynonymMap() {
        final boolean ignoreCase = true;
        final SynonymMap synMap = new SynonymMap(ignoreCase);
        Properties synDefs = null;
        try {
            synDefs = documentPropertiesHelper.load(SYNONYMS_DATA);
        } catch (Exception e) {
            LOGGER.error("Failed to load " + SYNONYMS_DATA, e);
            synDefs = new Properties();
        }
        for (String name : synDefs.stringPropertyNames()) {
            final String replacement = name.trim();
            final String match = synDefs.getProperty(name).trim();
            if (!replacement.equalsIgnoreCase(match)) {
                final boolean orig = false;
                final boolean merge = true;
                synMap.add(Arrays.asList(match), SynonymMap.makeTokens(Arrays
                        .asList(replacement)), orig, merge);
            }
        }
        return synMap;
    }
}
