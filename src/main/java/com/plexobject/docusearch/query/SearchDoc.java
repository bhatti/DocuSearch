package com.plexobject.docusearch.query;

import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.plexobject.docusearch.domain.Document;

/**
 * This class represents document stored in the document-based database.
 * 
 * @author Shahzad Bhatti
 */
public class SearchDoc extends Document {
    public static final String SCORE = "score";
    public static final String DOC = "doc";

    public SearchDoc(final Map<String, Object> properties) {
        super(properties);
    }

    /**
     * @return - score of the document
     */
    public double getScore() {
        return getDouble(SCORE, 0);
    }

    /**
     * @return - hit document number
     */
    public int getHitDocumentNumber() {
        return getInteger(DOC, 0);
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    @Override
    public boolean equals(Object object) {
        if (!(object instanceof SearchDoc)) {
            return false;
        }
        SearchDoc rhs = (SearchDoc) object;
        return new EqualsBuilder().append(this.getId(), rhs.getId()).isEquals();
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(786529047, 1924536713).append(this.getId())
                .toHashCode();
    }
}
