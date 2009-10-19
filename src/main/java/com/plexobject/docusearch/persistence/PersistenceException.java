package com.plexobject.docusearch.persistence;

/**
 * This class is thrown when error occurs while reading or saving peristed data or metadata.
 *
 * @author bhatti@plexobject.com
 *
 */
public class PersistenceException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	private int errorCode;
	public PersistenceException(final String message) {
		super(message);
	}
	public PersistenceException(final Throwable error) {
		super(error);
	}
    public PersistenceException(final Throwable error, final int errorCode) {
		super(error);
		this.errorCode = errorCode;
	}

    public PersistenceException(final String message, final Throwable error) {
		super(message, error);
	}
    public PersistenceException(final String message, final Throwable error, final int errorCode) {
		super(message, error);
		this.errorCode = errorCode;
	}
	public int getErrorCode() {
		return errorCode;
	}
}
