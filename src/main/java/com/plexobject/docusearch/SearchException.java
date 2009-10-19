package com.plexobject.docusearch;

/**
 * This class is thrown when error occurs while searching
 * 
 * @author bhatti@plexobject.com
 * 
 */
public class SearchException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	private int errorCode;

	public SearchException(final String message) {
		super(message);
	}

	public SearchException(final Throwable error) {
		super(error);
	}

	public SearchException(final Throwable error, final int errorCode) {
		super(error);
		this.errorCode = errorCode;
	}

	public SearchException(final String message, final Throwable error) {
		super(message, error);
	}

	public SearchException(final String message, final Throwable error,
			final int errorCode) {
		super(message, error);
		this.errorCode = errorCode;
	}

	public int getErrorCode() {
		return errorCode;
	}
}
