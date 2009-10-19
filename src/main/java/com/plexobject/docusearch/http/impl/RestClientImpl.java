package com.plexobject.docusearch.http.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.io.IOUtils;
import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.Logger;

import com.plexobject.docusearch.domain.Tuple;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.http.RestException;


/**
 * This class provides APIs to call remote Web service with JSON payload
 *
 * @author bhatti@plexobject.com
 */
public class RestClientImpl implements RestClient {
	private static final Logger LOGGER = Logger.getLogger(RestClientImpl.class);
	private HttpClient httpClient = new HttpClient();
    private final String url;

	public RestClientImpl(final String url) {
		this(url, null, null);
	}
	public RestClientImpl(final String url, final String username, final String password) {
		if (GenericValidator.isBlankOrNull(url)) {
			throw new IllegalArgumentException("url not specified");
		}

        this.url = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
        httpClient.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
		if (!GenericValidator.isBlankOrNull(username) && !GenericValidator.isBlankOrNull(password)) { 
            httpClient.getParams().setAuthenticationPreemptive(true);
            final Credentials creds = new UsernamePasswordCredentials(username, password);
            httpClient.getState().setCredentials(AuthScope.ANY, creds);
        }
	}

    /* 
	 * @see com.plexobject.search.http.RestClient#get(java.lang.String)
	 */
    public Tuple get(final String path) throws IOException {
        return execute(new GetMethod(url(path)));
    }

    /* 
	 * @see com.plexobject.search.http.RestClient#put(java.lang.String, java.lang.String)
	 */
    public Tuple put(final String path, final String body) throws IOException {
        final PutMethod method = new PutMethod(url(path));
        if (body != null) {
            method.setRequestEntity(new StringRequestEntity(body, "application/json", "UTF-8"));
        }
        try {
        	return execute(method);
        } finally {
            method.releaseConnection();
        }
    }

    /* 
	 * @see com.plexobject.search.http.RestClient#delete(java.lang.String)
	 */
    public int delete(final String path) throws IOException {
        final DeleteMethod method = new DeleteMethod(url(path));
        try {
            return httpClient.executeMethod(method);
        } finally {
            method.releaseConnection();
        }
    }


    private String url(final String path) {
        return String.format("%s/%s", url, path);
    }


    /* 
	 * @see com.plexobject.search.http.RestClient#post(java.lang.String, java.lang.String)
	 */
    public Tuple post(final String path, final String body) throws IOException {
    	if (LOGGER.isDebugEnabled()) {
    		LOGGER.debug("Posting to " + path + ": " + body);
    	}
        final PostMethod post = new PostMethod(url(path));
        post.setRequestEntity(new StringRequestEntity(body, "application/json", "UTF-8"));
        return execute(post);
    }


    private Tuple execute(final HttpMethodBase method) throws IOException {
        try {
            final int sc = httpClient.executeMethod(method);
            if (sc < OK_MIN || sc > OK_MAX) {
                throw new RestException("Unexpected status code: " + sc + ": " + method.getStatusText(), sc);
            }
            final InputStream in = method.getResponseBodyAsStream();
            try {
                final StringWriter writer = new StringWriter(2048);
                IOUtils.copy(in, writer, method.getResponseCharSet());
                return new Tuple(sc, writer.toString());
            } finally {
                in.close();
            }
        } finally {
            method.releaseConnection();
        }
    }
}
