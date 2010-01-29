package com.plexobject.docusearch.converter;

import java.io.StringReader;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.w3c.tidy.Tidy;

public class HtmlToTextConverter implements Converter<String, String> {
    private static final String BODY = "body";
    private final static String LF = System.getProperty("line.separator");

    @Override
    public String convert(final String html) {
        final StringReader in = new StringReader(html);
        Tidy tidy = new Tidy();
        tidy.setQuiet(true);
        tidy.setShowWarnings(false);
        org.w3c.dom.Document root = tidy.parseDOM(in, null);
        Element rawDoc = root.getDocumentElement();
        return getBody(rawDoc);
    }

    /**
     * Gets the body text of the HTML document.
     * 
     * @rawDoc the DOM Element to extract body Node from
     * @return the body text
     */
    protected static String getBody(Element rawDoc) {
        if (rawDoc == null) {
            return null;
        }
        String body = "";
        NodeList children = rawDoc.getElementsByTagName(BODY);
        if (children.getLength() > 0) {
            body = getText(children.item(0));
        }
        return body;
    }

    /**
     * Extracts text from the DOM node.
     * 
     * @param node
     *            a DOM node
     * @return the text value of the node
     */
    protected static String getText(Node node) {
        NodeList children = node.getChildNodes();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            switch (child.getNodeType()) {
            case Node.ELEMENT_NODE:
                sb.append(getText(child)).append(LF);
                break;
            case Node.TEXT_NODE:
                sb.append(((Text) child).getData());
                break;
            }
        }
        return sb.toString();
    }
}
