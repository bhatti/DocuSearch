package com.plexobject.docusearch.converter;

/**
 * @author bhatti@plexobject.com
 * 
 */
public interface Converter<FROM, TO> {
	/**
	 * @param from
	 *            - original raw object
	 * @return converted object
	 */
	TO convert(FROM from);
}
