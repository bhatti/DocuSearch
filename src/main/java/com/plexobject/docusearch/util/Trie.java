package com.plexobject.docusearch.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.validator.GenericValidator;

public class Trie {

    private TrieNode rootNode;

    public Trie() {
        super();
        rootNode = new TrieNode(' ');
    }

    public void load(String phrase) {
        loadRecursive(rootNode, phrase.toLowerCase() + "$");
    }

    private void loadRecursive(TrieNode node, String phrase) {
        if (GenericValidator.isBlankOrNull(phrase)) {
            return;
        }
        char firstChar = phrase.charAt(0);
        node.add(firstChar);
        TrieNode childNode = node.getChildNode(firstChar);
        if (childNode != null) {
            loadRecursive(childNode, phrase.substring(1));
        }
    }

    public boolean matchPrefix(String prefix) {
        TrieNode matchedNode = matchPrefixRecursive(rootNode, prefix);
        return (matchedNode != null);
    }

    public List<String> findCompletions(String prefix) {
        TrieNode matchedNode = matchPrefixRecursive(rootNode, prefix);
        List<String> completions = new ArrayList<String>();
        findCompletionsRecursive(matchedNode, prefix, completions);
        return completions;
    }

    private void findCompletionsRecursive(TrieNode node, String prefix,
            List<String> completions) {
        if (node == null) {
            // our prefix did not match anything, just return
            return;
        }
        if (node.getNodeValue() == '$') {
            // end reached, append prefix into completions list. Do not append
            // the trailing $, that is only to distinguish words like ann and
            // anne
            // into separate branches of the tree.
            completions.add(prefix.substring(0, prefix.length() - 1));
            return;
        }
        Collection<TrieNode> childNodes = node.getChildren();
        for (TrieNode childNode : childNodes) {
            char childChar = childNode.getNodeValue();
            findCompletionsRecursive(childNode, prefix + childChar, completions);
        }
    }

    public String toString() {
        return "Trie:" + rootNode.toString();
    }

    private TrieNode matchPrefixRecursive(TrieNode node, String prefix) {
        if (GenericValidator.isBlankOrNull(prefix)) {
            return node;
        }
        char firstChar = prefix.charAt(0);
        TrieNode childNode = node.getChildNode(firstChar);
        if (childNode == null) {
            // no match at this char, exit
            return null;
        } else {
            // go deeper
            return matchPrefixRecursive(childNode, prefix.substring(1));
        }
    }
}
