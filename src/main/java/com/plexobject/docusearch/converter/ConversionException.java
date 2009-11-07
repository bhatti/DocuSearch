package com.plexobject.docusearch.converter;

/**
 * This is thrown when conversion error occurs
 * 
 * @author Shahzad Bhatti
 * 
 */
public class ConversionException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	private int errorCode;

	public ConversionException(final String message) {
		super(message);
	}

	public ConversionException(final Throwable error) {
		super(error);
	}

	public ConversionException(final Throwable error, final int errorCode) {
		super(error);
		this.errorCode = errorCode;
	}

	public ConversionException(final String message, final Throwable error) {
		super(message, error);
	}

	public ConversionException(final String message, final Throwable error,
			final int errorCode) {
		super(message, error);
		this.errorCode = errorCode;
	}

	public int getErrorCode() {
		return errorCode;
	}
}
