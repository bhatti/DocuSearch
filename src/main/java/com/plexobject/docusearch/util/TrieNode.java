package com.plexobject.docusearch.util;

import java.util.Collection;
import java.util.TreeMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class TrieNode {

    private Character character;
    private Map<Character, TrieNode> children;

    public TrieNode(char c) {
        super();
        this.character = Character.valueOf(c);
        children = new TreeMap<Character, TrieNode>();
    }

    public char getNodeValue() {
        return character.charValue();
    }

    public Collection<TrieNode> getChildren() {
        return children.values();
    }

    public Set<Character> getChildrenNodeValues() {
        return children.keySet();
    }

    public void add(char c) {
        if (children.get(Character.valueOf(c)) == null) {
            // children does not contain c, add a TrieNode
            children.put(Character.valueOf(c), new TrieNode(c));
        }
    }

    public TrieNode getChildNode(char c) {
        return children.get(Character.valueOf(c));
    }

    public boolean contains(char c) {
        return (children.get(Character.valueOf(c)) != null);
    }

    @Override
    public int hashCode() {
        return character.hashCode();
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    @Override
    public boolean equals(Object object) {
        if (!(object instanceof TrieNode)) {
            return false;
        }
        TrieNode rhs = (TrieNode) object;
        return new EqualsBuilder().append(this.getNodeValue(),
                rhs.getNodeValue()).isEquals();
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("value", this.getNodeValue())
                .toString();
    }
}
