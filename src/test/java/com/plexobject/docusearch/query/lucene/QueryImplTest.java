package com.plexobject.docusearch.query.lucene;

import java.io.File;

import org.junit.Test;

import com.plexobject.docusearch.index.lucene.IndexerImplTest;
import com.plexobject.docusearch.lucene.LuceneUtils;

public class QueryImplTest extends IndexerImplTest {

    @Test(expected = NullPointerException.class)
    public void testNullDirConstructor() throws Exception {
        new QueryImpl(null, "index");
    }

    @Test(expected = NullPointerException.class)
    public void testNullIndexConstructor() throws Exception {
        new QueryImpl(LuceneUtils.toFSDirectory(new File(DB_NAME)), null);
    }
}
