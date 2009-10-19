package com.plexobject.docusearch.index.lucene;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;

import com.plexobject.docusearch.Configuration;

/**
 * Drop-in replacement for IndexWriter that uses multiple threads, under the
 * hood, to index added documents.
 * 
 * @author bhatti@plexobject.com
 */

public class ThreadedIndexWriter extends IndexWriter {
	private static final Logger LOGGER = Logger
			.getLogger(ThreadedIndexWriter.class);

	private static final int MAX_THREAD_COUNT = Configuration.getInstance()
			.getInteger("lucene.max.threads", 2);
	private static final int MAX_PENDING_TASKS = Configuration.getInstance()
			.getInteger("lucene.max.pending.tasks", 128);
	private static final int MAX_WAIT_BEFORE_SHUTDOWN = Configuration
			.getInstance().getInteger("lucene.wait.before.shutdown.secs", 60);

	private volatile ThreadPoolExecutor executor;
	private BlockingQueue<Runnable> pendingTasksQueue = new ArrayBlockingQueue<Runnable>(
			MAX_PENDING_TASKS, false);

	public ThreadedIndexWriter(final Directory dir, Analyzer a, boolean create,
			final IndexWriter.MaxFieldLength mfl) throws CorruptIndexException,
			IOException {
		this(dir, a, create, mfl, MAX_THREAD_COUNT / 2, MAX_THREAD_COUNT);
	}

	public ThreadedIndexWriter(final Directory dir, Analyzer a, boolean create,
			final IndexWriter.MaxFieldLength mfl, final int defaultThreadCount,
			final int maxThreadCount) throws CorruptIndexException, IOException {
		super(dir, a, create, mfl);
		executor = new ThreadPoolExecutor(defaultThreadCount, maxThreadCount,
				Long.MAX_VALUE, TimeUnit.NANOSECONDS, pendingTasksQueue,
				new ThreadPoolExecutor.CallerRunsPolicy());
		executor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
			public void rejectedExecution(final Runnable r,
					final ThreadPoolExecutor e) {
				LOGGER.error("Re-executing rejected Execution:- " + r + " /"
						+ e + ", tasks remaining : " + e.getTaskCount()
						+ ", tasks executed: " + e.getCompletedTaskCount());
				r.run();
			}
		});
	}

	public void addDocument(final Document doc) throws CorruptIndexException,
			IOException {
		if (executor != null) {
			executor.execute(new Runnable() {
				public void run() {
					try {
						ThreadedIndexWriter.super.addDocument(doc);
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("addDocument " + doc);
						}
					} catch (CorruptIndexException e) {
						LOGGER.error("Error updating index.", e);
					} catch (IOException e) {
						LOGGER.error("Error updating index.", e);
					}
				}
			});
		} else {
			super.addDocument(doc);
		}
	}

	public void addDocument(final Document doc, final Analyzer a)
			throws CorruptIndexException, IOException {
		if (executor != null) {
			executor.execute(new Runnable() {
				public void run() {
					try {
						ThreadedIndexWriter.super.addDocument(doc, a);
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("addDocument " + doc);
						}
					} catch (CorruptIndexException e) {
						LOGGER.error("Error updating index.", e);
					} catch (IOException e) {
						LOGGER.error("Error updating index.", e);
					}
				}
			});
		} else {
			super.addDocument(doc, a);
		}

	}

	public void updateDocument(final Term term, final Document doc)
			throws CorruptIndexException, IOException {
		if (executor != null) {
			executor.execute(new Runnable() {
				public void run() {
					try {
						ThreadedIndexWriter.super.updateDocument(term, doc);
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("updateDocument " + doc);
						}
					} catch (CorruptIndexException e) {
						LOGGER.error("Error updating index.", e);
					} catch (IOException e) {
						LOGGER.error("Error updating index.", e);
					}
				}
			});
		} else {
			super.updateDocument(term, doc);
		}
	}

	public void updateDocument(final Term term, final Document doc,
			final Analyzer a) throws CorruptIndexException, IOException {
		if (executor != null) {
			executor.execute(new Runnable() {
				public void run() {
					try {
						ThreadedIndexWriter.super.updateDocument(term, doc, a);
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("updateDocument " + doc);
						}
					} catch (CorruptIndexException e) {
						LOGGER.error("Error updating index.", e);
					} catch (IOException e) {
						LOGGER.error("Error updating index.", e);
					}
				}
			});
		} else {
			super.updateDocument(term, doc, a);
		}
	}

	public void close() throws CorruptIndexException, IOException {
		if (executor != null) {
			executor.execute(new Runnable() {
				public void run() {

					try {
						finish();
						ThreadedIndexWriter.super.close();
					} catch (CorruptIndexException e) {
						LOGGER.error("Error updating index.", e);
					} catch (IOException e) {
						LOGGER.error("Error updating index.", e);
					}
				}
			});
		} else {
			finish();
			super.close();
		}

	}

	public void close(final boolean doWait) throws CorruptIndexException,
			IOException {

		if (executor != null) {
			executor.execute(new Runnable() {
				public void run() {

					try {
						finish();
						ThreadedIndexWriter.super.close(doWait);
					} catch (CorruptIndexException e) {
						LOGGER.error("Error updating index.", e);
					} catch (IOException e) {
						LOGGER.error("Error updating index.", e);
					}
				}
			});
		} else {
			finish();
			super.close(doWait);
		}

	}

	public void rollback() throws CorruptIndexException, IOException {
		if (executor != null) {
			executor.execute(new Runnable() {
				public void run() {

					try {
						finish();
						ThreadedIndexWriter.super.rollback();
					} catch (CorruptIndexException e) {
						LOGGER.error("Error updating index.", e);
					} catch (IOException e) {
						LOGGER.error("Error updating index.", e);
					}
				}
			});
		} else {
			finish();
			super.rollback();
		}

	}

	private void finish() {
		if (executor != null) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Shutting down threadpool for index writer "
						+ MAX_WAIT_BEFORE_SHUTDOWN + ", tasks remaining : "
						+ executor.getTaskCount() + ", tasks executed: "
						+ executor.getCompletedTaskCount());
			}
			executor.shutdown();
			try {
				executor.awaitTermination(MAX_WAIT_BEFORE_SHUTDOWN,
						TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.interrupted();
			}

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Completed shutdown threadpool for index writer");
			}
			executor = null;
		}
	}
}
