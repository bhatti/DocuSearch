/**
 * 
 */
package com.plexobject.docusearch.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * @author Shahzad Bhatti
 * 
 */
public class SearchDocList implements List<SearchDoc> {
    private final int start;
    private final int pageSize;
    private final int totalHits;
    private final Collection<String> similarWords;
    private final List<SearchDoc> docs = new ArrayList<SearchDoc>();

    public SearchDocList(final int start, final int pageSize,
            final int totalHits, final List<SearchDoc> c,
            final Collection<String> similarWords) {
        this.start = start;
        this.pageSize = pageSize;
        this.similarWords = similarWords;
        this.totalHits = totalHits;
        Collections.sort(c, new Comparator<SearchDoc>() {
            @Override
            public int compare(SearchDoc first, SearchDoc second) {
                if (first.getScore() > second.getScore()) {
                    return -1;
                } else if (first.getScore() < second.getScore()) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });
        this.docs.addAll(c);
    }

    @Override
    public boolean add(final SearchDoc d) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(final int index, final SearchDoc d) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(final Collection<? extends SearchDoc> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(int index, Collection<? extends SearchDoc> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(final Object o) {
        return docs.contains(o);
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        return docs.containsAll(c);
    }

    @Override
    public SearchDoc get(final int index) {
        return docs.get(index);
    }

    @Override
    public int indexOf(final Object o) {
        return docs.indexOf(o);
    }

    @Override
    public boolean isEmpty() {
        return docs.isEmpty();
    }

    @Override
    public Iterator<SearchDoc> iterator() {
        return docs.iterator();
    }

    @Override
    public int lastIndexOf(final Object o) {
        return docs.lastIndexOf(o);

    }

    @Override
    public ListIterator<SearchDoc> listIterator() {
        return docs.listIterator();
    }

    @Override
    public ListIterator<SearchDoc> listIterator(final int index) {
        return docs.listIterator(index);
    }

    @Override
    public boolean remove(final Object o) {
        return docs.remove(o);
    }

    @Override
    public SearchDoc remove(final int index) {
        return docs.remove(index);
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchDoc set(final int index, final SearchDoc element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return docs.size();
    }

    @Override
    public List<SearchDoc> subList(final int fromIndex, final int toIndex) {
        return docs.subList(fromIndex, toIndex);
    }

    @Override
    public Object[] toArray() {
        return docs.toArray();
    }

    public int getTotalHits() {
        return totalHits;
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return docs.toArray(a);
    }

    public int getStart() {
        return start;
    }

    public int getPageSize() {
        return pageSize;
    }

    public Collection<String> getSimilarWords() {
        return similarWords;
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    @Override
    public boolean equals(Object object) {
        if (!(object instanceof SearchDocList)) {
            return false;
        }
        SearchDocList rhs = (SearchDocList) object;
        return new EqualsBuilder().append(this.docs, rhs.docs).isEquals();
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(786529047, 1924536713).append(this.docs)
                .toHashCode();
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("similarWords",
                this.similarWords).append("size", this.docs.size()).append(
                "docs", this.docs).toString();
    }

}
