package com.plexobject.docusearch.jms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.web.BrokerFacadeSupport;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.BrowserCallback;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.cache.CachedMap;
import com.sun.jersey.spi.inject.Inject;

/**
 * 
 * Move messages from DLQ to original queue
 * 
 * Note: uncomment SUNJMX="-Dcom.sun.management.jmxremote.port=1099 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
 * in activemq script
 * 
 * @author Shahzad Bhatti
 * 
 */
public class DeadLetterReprocessor implements InitializingBean {
    protected static final Logger LOGGER = Logger
            .getLogger(DeadLetterReprocessor.class);
    private static final String DEFAULT_DLQ = "ActiveMQ.DLQ";
    private static final String SERVER_NAME = Configuration.getInstance()
            .getProperty("activemq.server.name", "localhost");
    private static final long INDEFINITE = 0;

    private CachedMap<String, Boolean> seenIds = new CachedMap<String, Boolean>(
            INDEFINITE, 1000);
    @Autowired
    @Inject
    private JmsTemplate jmsTemplate;

    @Autowired
    @Inject
    BrokerFacadeSupport brokerQuery;

    @Autowired
    @Inject
    ConnectionFactory connectionFactory;

    public long getQueueSize(final String brokerName, final String from,
            final String to) {
        try {
            final QueueViewMBean queue = getQueue(brokerName);

            return queue.getQueueSize();
        } catch (Exception e) {
            LOGGER.error("Failed to reprocess messages", e);
            throw new RuntimeException("Failed to get queue size", e);
        }
    }

    @SuppressWarnings("unchecked")
    public Collection<String> yredeliverDLQ(final String brokerName,
            final String to) {
        final Collection<String> messageIds = new ArrayList<String>();
        Connection connection = null;

        Session session = null;

        try {
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue dlq = session.createQueue(DEFAULT_DLQ);
            QueueBrowser browser = session.createBrowser(dlq);

            Enumeration<Message> e = browser.getEnumeration();
            while (e.hasMoreElements()) {
                e.nextElement();
            }
            Thread.sleep(5000);

            browser = session.createBrowser(dlq);
            e = browser.getEnumeration();

            LOGGER.info("browsing " + browser + ", dlq " + dlq);
            while (e.hasMoreElements()) {
                Message message = e.nextElement();
                String messageID = (String) message.getJMSMessageID();
                if (seenIds.get(messageID) == null) {
                    continue;
                }
                LOGGER.info("Moving" + messageID);

                final String messageBody = ((TextMessage) message).getText();
                jmsTemplate.send(to, new MessageCreator() {
                    @Override
                    public Message createMessage(final Session session)
                            throws JMSException {
                        return session.createTextMessage(messageBody);
                    }
                });
                seenIds.put(messageID, Boolean.TRUE);
                messageIds.add(messageID);

            }

            LOGGER.info("Moved " + messageIds.size() + " from " + DEFAULT_DLQ
                    + " to " + to);
        } catch (Exception e) {
            LOGGER.error("Failed to reprocess messages", e);
            throw new RuntimeException(
                    "Failed to reprocess messages (successful-ids "
                            + messageIds + ")", e);
        } finally {
            try {
                session.close();
            } catch (Exception e) {
                LOGGER.error("Failed to close session", e);
            }
            try {
                connection.close();
            } catch (Exception e) {
                LOGGER.error("Failed to close connection", e);
            }
        }
        return messageIds;
    }

    public Collection<String> xxredeliverDLQ(final String brokerName,
            final String to) {
        final Collection<String> messageIds = new ArrayList<String>();
        try {
            jmsTemplate.browse(DEFAULT_DLQ, new BrowserCallback() {

                @SuppressWarnings("unchecked")
                @Override
                public Object doInJms(Session session, QueueBrowser browser)
                        throws JMSException {
                    Enumeration<Message> e = browser.getEnumeration();
                    while (e.hasMoreElements()) {
                        Message message = e.nextElement();
                        String messageID = (String) message.getJMSMessageID();
                        if (seenIds.get(messageID) == null) {
                            continue;
                        }
                        final String messageBody = ((TextMessage) message)
                                .getText();
                        jmsTemplate.send(to, new MessageCreator() {
                            @Override
                            public Message createMessage(final Session session)
                                    throws JMSException {
                                return session.createTextMessage(messageBody);
                            }
                        });
                        seenIds.put(messageID, Boolean.TRUE);
                        messageIds.add(messageID);

                    }
                    return null;
                }
            });

            LOGGER.info("Moved " + messageIds.size() + " from " + DEFAULT_DLQ
                    + " to " + to);
        } catch (Exception e) {
            LOGGER.error("Failed to reprocess messages", e);
            throw new RuntimeException(
                    "Failed to reprocess messages (successful-ids "
                            + messageIds + ")", e);
        }
        return messageIds;
    }

    public Collection<String> redeliverDLQ(final String brokerName,
            final String to) {
        Collection<String> messageIds = new ArrayList<String>();
        try {
            final QueueViewMBean queue = getQueue(brokerName);
            long messageCount = 0;

            while ((messageCount = queue.getQueueSize()) > 0) {
                CompositeData[] compdatalist = queue.browse();
                LOGGER.info("message count " + messageCount + ", browse "
                        + compdatalist.length);
                for (CompositeData cdata : compdatalist) {
                    String messageID = (String) cdata.get("JMSMessageID");
                    if (seenIds.get(messageID) != null) {
                        continue;
                    }

                    queue.moveMessageTo(messageID, to);
                    seenIds.put(messageID, Boolean.TRUE);
                    messageIds.add(messageID);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Moving " + messageID);
                    }
                }
            }
            LOGGER.info("Moved " + messageIds.size() + " from " + DEFAULT_DLQ
                    + " to " + to);
        } catch (Exception e) {
            LOGGER.error("Failed to reprocess messages", e);
            throw new RuntimeException(
                    "Failed to reprocess messages (successful-ids "
                            + messageIds + ")", e);
        }
        return messageIds;
    }

    public Collection<String> copyMessages(final String from, final String to) {
        Collection<String> messageIds = new ArrayList<String>();
        try {
            jmsTemplate.setReceiveTimeout(100);
            Message message = null;
            while ((message = jmsTemplate.receive(from)) != null) {
                final String messageId = message.getJMSMessageID();
                if (seenIds.get(messageId) == null) {
                    continue;
                }
                seenIds.put(messageId, Boolean.TRUE);
                messageIds.add(messageId);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Moving " + messageId + ": "
                            + message.toString());
                }
                final String messageBody = ((TextMessage) message).getText();
                jmsTemplate.send(to, new MessageCreator() {
                    @Override
                    public Message createMessage(final Session session)
                            throws JMSException {
                        return session.createTextMessage(messageBody);
                    }
                });
            }
            LOGGER.info("Moved " + messageIds.size() + " from " + from + " to "
                    + to);
        } catch (Exception e) {
            LOGGER.error("Failed to reprocess messages", e);
            throw new RuntimeException(
                    "Failed to reprocess messages (successful-ids "
                            + messageIds + ")", e);
        }
        return messageIds;
    }

    /**
     * @return the jmsTemplate
     */
    public JmsTemplate getJmsTemplate() {
        return jmsTemplate;
    }

    /**
     * @param jmsTemplate
     *            the jmsTemplate to set
     */
    public void setJmsTemplate(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    /**
     * @return the brokerQuery
     */
    public BrokerFacadeSupport getBrokerQuery() {
        return brokerQuery;
    }

    /**
     * @param brokerQuery
     *            the brokerQuery to set
     */
    public void setBrokerQuery(BrokerFacadeSupport brokerQuery) {
        this.brokerQuery = brokerQuery;
    }

    /**
     * @return the connectionFactory
     */
    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    /**
     * @param connectionFactory
     *            the connectionFactory to set
     */
    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    QueueViewMBean getQueue(final String brokerName) throws Exception {
        QueueViewMBean queue = brokerQuery.getQueue(DEFAULT_DLQ);
        if (queue == null && LOGGER.isInfoEnabled()) {
            final ObjectName dlq = new ObjectName(
                    "org.apache.activemq:BrokerName=" + brokerName
                            + ",Type=Queue,Destination=" + DEFAULT_DLQ);
            MBeanServerConnection mbsc = getMBeanServerConnection();
            if (mbsc != null) {
                Set<?> mbeans = mbsc.queryMBeans(dlq, null);
                if (mbeans != null) {
                    if (mbeans.size() == 1) {
                        LOGGER.info("FOUND " + mbeans);
                    } else {
                        Set<?> all = mbsc.queryMBeans(null, null);
                        StringBuilder availableMbeans = new StringBuilder();
                        for (Object o : all) {
                            ObjectInstance bean = (ObjectInstance) o;
                            availableMbeans.append("\t" + bean.getObjectName()
                                    + "\n");
                        }

                        LOGGER.info("Could not find " + dlq
                                + ", instead found " + mbeans.size() + ": "
                                + availableMbeans);
                    }
                }
            }
        }
        return queue;
    }

    private MBeanServerConnection getMBeanServerConnection() throws IOException {
        final JMXServiceURL url = new JMXServiceURL(
                "service:jmx:rmi:///jndi/rmi://" + SERVER_NAME + ":1099/jmxrmi");
        JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
        return jmxc.getMBeanServerConnection();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (connectionFactory == null) {
            throw new IllegalStateException("connectionFactory is not set");
        }
        if (brokerQuery == null) {
            throw new IllegalStateException("remoteJMXBrokerFacade is not set");
        }

        if (jmsTemplate == null) {
            throw new IllegalStateException("jmsTemplate is not set");
        }
        System.getProperties()
                .setProperty(
                        "webconsole.jmx.url",
                        "service:jmx:rmi:///jndi/rmi://" + SERVER_NAME
                                + ":1099/jmxrmi");
    }
}
