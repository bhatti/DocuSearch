package com.plexobject.docusearch.index.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.tier.CartesianPolyFilterBuilder;
import org.apache.lucene.spatial.tier.Shape;
import org.apache.lucene.util.OpenBitSet;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrConstantScoreQuery;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Given a user query, create a Filter based on the proper box ids of the
 * cartesian tiers.
 * <p/>
 * <ul>
 * <li>x - double, The x coordinate of the center point. Required</li>
 * <li>y - double, The y coordinate of the center point. Required</li>
 * <li>prefix - String, The name of the tier field prefix to filter on.
 * Optional.</li>
 * <li>dist - double, the distance, in miles, from the centerpoint to the box
 * edge on the diagonal(?). Required</li>
 * </ul>
 * <p/>
 * Example: {!tier x=32 y=-79 radians=false dist=10 unit=m}
 * 
 * <p/>
 * Sample Config: <div>
 * 
 * <pre>
 * &lt;queryParser name=&quot;tier&quot; class=&quot;CartesianTierQParserPlugin&quot;&gt;    
 *     &lt;str name=&quot;tierPrefix&quot;&gt;tier_&lt;/str&gt;
 *   &lt;/queryParser&gt;
 * </pre>
 * 
 * </div>
 */
public class CartesianTierQParserPlugin extends QParserPlugin {
    public static String NAME = "tier";
    public static final String DEFAULT_TIER_PREFIX = "tier_";
    private String tierPrefix = DEFAULT_TIER_PREFIX;

    @SuppressWarnings("unchecked")
    public void init(NamedList args) {
        SolrParams params = SolrParams.toSolrParams(args);
        tierPrefix = params.get("tierPrefix", DEFAULT_TIER_PREFIX);
    }

    public QParser createParser(String qstr, SolrParams localParams,
            SolrParams params, SolrQueryRequest req) {
        return new QParser(qstr, localParams, params, req) {
            public Query parse() throws ParseException {
                final Double x = localParams.getDouble("x");
                final Double y = localParams.getDouble("y");

                final String fieldPrefix = localParams
                        .get("prefix", tierPrefix);
                final Double distance = localParams.getDouble("dist");
                // GSI: kind of funky passing in an empty string, but AFAICT, it
                // is safe b/c we don't want to assume tier prefix stuff
                CartesianPolyFilterBuilder cpfb = new CartesianPolyFilterBuilder(
                        fieldPrefix);
                // Get the box based on this point and our distance
                final Shape shape = cpfb.getBoxShape(x, y, distance);
                final List<Double> boxIds = shape.getArea();

                // Sort them, so they are in order, which will be faster for
                // termdocs in the tier filter
                Collections.sort(boxIds);
                // Get the field type so we can properly encode the data
                IndexSchema schema = req.getSchema();
                FieldType ft = schema.getFieldTypeNoEx(shape.getTierId());

                // Create the Filter and wrap it in a constant score query
                Filter filter = new TierFilter(shape.getTierId(), ft, boxIds);
                return new SolrConstantScoreQuery(filter);
            }
        };
    }

    private class TierFilter extends Filter {
        private static final long serialVersionUID = 1L;
        private List<Double> sortedBoxIds;
        private String field;
        private FieldType ft;

        private TierFilter(String field, FieldType ft, List<Double> sortedBoxIds) {
            this.field = field;
            this.ft = ft;
            this.sortedBoxIds = sortedBoxIds;
            // System.out.println("bi: " + sortedBoxIds);
            // System.out.println("Tier: " + field + " ids: " + sortedBoxIds);
        }

        public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
            OpenBitSet result = new OpenBitSet(reader.maxDoc());

            TermDocs termDocs = reader.termDocs();// position it at the start
            Term term = new Term(field);
            for (Double boxId : sortedBoxIds) {
                // System.out.println("boxId: " + boxId);
                // Tiers are encoded as TrieDoubleFiled
                term = term.createTerm(ft.toInternal(String.valueOf(boxId)));
                termDocs.seek(term);
                // iterate through all documents
                // which have this boxId
                while (termDocs.next()) {
                    // System.out.println("Match: " + termDocs.doc());
                    result.fastSet(termDocs.doc());
                }
            }
            termDocs.close();
            return result;
        }
    }

}
