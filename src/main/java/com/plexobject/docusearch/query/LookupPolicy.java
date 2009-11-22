package com.plexobject.docusearch.query;

import java.util.Collection;

public class LookupPolicy extends QueryPolicy {
    private String fieldToReturn;

    public LookupPolicy() {
    }

    public LookupPolicy(final String fieldToReturn,
            final Collection<Field> fields) {
        super(fields);
        setFieldToReturn(fieldToReturn);
    }

    public void setFieldToReturn(final String fieldToReturn) {
        this.fieldToReturn = fieldToReturn;
    }

    public String getFieldToReturn() {
        return fieldToReturn;
    }
}
