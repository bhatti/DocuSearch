package com.plexobject.docusearch.persistence;

/**
 * This class is thrown when object doesn't exist in the database.
 *
 * @author bhatti@plexobject.com
 *
 */
public class NotFoundException extends PersistenceException {
	private static final long serialVersionUID = 1L;
	public NotFoundException(final String message) {
		super(message);
	}
	public NotFoundException(final Throwable error) {
		super(error);
	}
    public NotFoundException(final String message, final Throwable error) {
		super(message, error);
	}
}
