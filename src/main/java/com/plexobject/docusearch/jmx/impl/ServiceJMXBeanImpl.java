/**
 * 
 */
package com.plexobject.docusearch.jmx.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.AttributeChangeNotification;
import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import com.plexobject.docusearch.jmx.ServiceJMXBean;
import com.plexobject.docusearch.metrics.Metric;

/**
 * @author Shahzad Bhatti
 * 
 */
public class ServiceJMXBeanImpl extends NotificationBroadcasterSupport
        implements ServiceJMXBean {
    private Map<String, String> properties = new ConcurrentHashMap<String, String>();
    private final String serviceName;
    private AtomicLong totalErrors;
    private AtomicLong totalRequests;

    private AtomicLong sequenceNumber;
    private String state;

    public ServiceJMXBeanImpl(final String serviceName) {
        this.serviceName = serviceName;
        this.totalErrors = new AtomicLong();
        this.totalRequests = new AtomicLong();
        this.sequenceNumber = new AtomicLong();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.plexobject.docusearch.jmx.ServiceJMXBean#getAverageElapsedTimeInNanoSecs()
     */
    @Override
    public double getAverageElapsedTimeInNanoSecs() {
        return Metric.getMetric(getServiceName())
                .getAverageDurationInNanoSecs();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.jmx.ServiceJMXBean#getProperty(java.lang.String)
     */
    @Override
    public String getProperty(final String name) {
        return properties.get(name);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.jmx.ServiceJMXBean#setProperty(java.lang.String,
     * java.lang.String)
     */
    @Override
    public void setProperty(final String name, final String value) {
        final String oldValue = properties.put(name, value);
        final Notification notification = new AttributeChangeNotification(this,
                sequenceNumber.incrementAndGet(), System.currentTimeMillis(),
                name + " changed", name, "String", oldValue, value);
        sendNotification(notification);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.jmx.ServiceJMXBean#getServiceName()
     */
    @Override
    public String getServiceName() {
        return serviceName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.jmx.ServiceJMXBean#getTotalDurationInNanoSecs()
     */
    @Override
    public long getTotalDurationInNanoSecs() {
        return Metric.getMetric(getServiceName()).getTotalDurationInNanoSecs();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.jmx.ServiceJMXBean#getTotalErrors()
     */
    @Override
    public long getTotalErrors() {
        return totalErrors.get();
    }

    public void incrementError() {
        final long oldErrors = totalErrors.getAndIncrement();
        final Notification notification = new AttributeChangeNotification(this,
                sequenceNumber.incrementAndGet(), System.currentTimeMillis(),
                "Errors changed", "Errors", "long", oldErrors, oldErrors + 1);
        sendNotification(notification);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.jmx.ServiceJMXBean#getTotalRequests()
     */
    @Override
    public long getTotalRequests() {
        return totalRequests.get();
    }

    public void incrementRequests() {
        final long oldRequests = totalRequests.getAndIncrement();
        final Notification notification = new AttributeChangeNotification(this,
                sequenceNumber.incrementAndGet(), System.currentTimeMillis(),
                "Requests changed", "Requests", "long", oldRequests,
                oldRequests + 1);
        sendNotification(notification);
    }

    @Override
    public MBeanNotificationInfo[] getNotificationInfo() {
        String[] types = new String[] { AttributeChangeNotification.ATTRIBUTE_CHANGE };
        String name = AttributeChangeNotification.class.getName();
        String description = "An attribute of this MBean has changed";
        MBeanNotificationInfo info = new MBeanNotificationInfo(types, name,
                description);
        return new MBeanNotificationInfo[] { info };
    }

    @Override
    public String getState() {
        return state;
    }

    /**
     * @param state
     *            the state to set
     */
    public void setState(String state) {
        this.state = state;
    }

}
