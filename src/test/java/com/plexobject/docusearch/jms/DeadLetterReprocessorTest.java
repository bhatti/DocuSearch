package com.plexobject.docusearch.jms;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.web.LocalBrokerFacade;
import org.apache.activemq.web.RemoteJMXBrokerFacade;
import org.apache.activemq.web.config.AbstractConfiguration;
import org.codehaus.jettison.json.JSONException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.listener.SimpleMessageListenerContainer;

import com.plexobject.docusearch.persistence.PersistenceException;

public class DeadLetterReprocessorTest extends BaseMessageTestCase {
    private static final int MAX_COUNT = 2;
    private static final String QUEUE_NAME = "Test_QUEUE"
            + System.currentTimeMillis();
    private static final String TEST_MESSAGE = "[{\"unique_id\":\"1\"},"
            + "{\"unique_id\":\"2\"}]";

    private class MessageHandler implements MessageListener {
        @Override
        public void onMessage(final Message message) {
            received.incrementAndGet();
            ActiveMQTextMessage activeMQTextMessage = (ActiveMQTextMessage) message;
            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Received redelivered "
                            + message.getJMSRedelivered()
                            + ", redlievery count "
                            + activeMQTextMessage.getRedeliveryCounter()
                            + ", message " + message);
                }
            } catch (JMSException e) {
            }
            if (received.get() < (MAX_COUNT * 2)) {
                LOGGER.debug("rolling back");
                throw new RuntimeException("rollback message test");
            }
            LOGGER.info("not rolling back");
        }
    }

    private DeadLetterReprocessor deadLetterReprocessor;
    private final MessageHandler handler = new MessageHandler();
    private final AtomicLong received = new AtomicLong();
    private SimpleMessageListenerContainer listenerContainer;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        setupDLQProcessor();
        setupListener();
        sendMessages();

    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public final void testRedeliver() throws PersistenceException,
            JSONException, InterruptedException {
        for (int i = 0; i < MAX_COUNT; i++) {
            final int id = i;
            jmsTemplate.send(QUEUE_NAME, new MessageCreator() {

                @Override
                public Message createMessage(Session session)
                        throws JMSException {
                    Message m = session.createTextMessage(TEST_MESSAGE);
                    m.setJMSMessageID("M" + id);
                    m.setJMSCorrelationID("C" + id);
                    LOGGER.debug("Publishing message " + m);

                    return m;
                }

            });
        }
        Thread.sleep(1000);
        Assert.assertEquals(MAX_COUNT * 2, received.get());

        // Assert.assertEquals(10, deadLetterReprocessor.getQueueSize(
        // DEFAULT_BROKER, DEFAULT_DLQ, QUEUE_NAME));
        final Collection<String> ids = deadLetterReprocessor.redeliverDLQ(
                DEFAULT_BROKER, QUEUE_NAME);
        Assert.assertEquals(2, ids.size());

        Thread.sleep(1000);
        Assert.assertEquals(MAX_COUNT * 3, received.get());
    }

    private void sendMessages() throws JMSException {
        Session session = connection.createSession(false,
                Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(new ActiveMQQueue(
                "ActiveMQ.DLQ"));

        Message message = session.createTextMessage("[]");
        producer.send(message);
    }

    private void setupListener() {
        listenerContainer = new SimpleMessageListenerContainer() {
            {
                setAutoStartup(true);
                setSessionTransacted(true);
                setConnectionFactory(connectionFactory);
                setDestinationName(QUEUE_NAME);
                setMessageListener(handler);
                setConcurrentConsumers(1);
                // setReceiveTimeout(200);
                // setRecoveryInterval(100);
            }
        };
        listenerContainer.start();
    }

    private void setupDLQProcessor() throws Exception {
        deadLetterReprocessor = new DeadLetterReprocessor() {
            // @Override
            QueueViewMBean getQueue(final String brokerName) throws Exception {
                QueueViewMBean bean = super.getQueue(brokerName);
                if (bean == null) {
                    ObjectName queueViewMBeanName = new ObjectName(
                            "org.apache.activemq:Type=Queue,Destination=ActiveMQ.DLQ,BrokerName="
                                    + DEFAULT_BROKER);

                    bean = (QueueViewMBean) broker.getManagementContext()
                            .newProxyInstance(queueViewMBeanName,
                                    QueueViewMBean.class, true);
                }
                if (bean == null) {
                    ManagementContext managementContext = broker
                            .getManagementContext();

                    final Method getMBeanServer = ManagementContext.class
                            .getDeclaredMethod("getMBeanServer");
                    getMBeanServer.setAccessible(true);
                    MBeanServer mbeanServer = (MBeanServer) getMBeanServer
                            .invoke(managementContext);

                    bean = (QueueViewMBean) MBeanServerInvocationHandler
                            .newProxyInstance(
                                    mbeanServer,
                                    new ObjectName(
                                            "org.apache.activemq:BrokerName="
                                                    + DEFAULT_BROKER
                                                    + ",Type=Queue,Destination=ActiveMQ.DLQ"),
                                    QueueViewMBean.class, true);
                }
                return bean;
            }
        };
        deadLetterReprocessor.setJmsTemplate(jmsTemplate);
        // deadLetterReprocessor.setBrokerQuery(new LocalBrokerFacade(broker));
        final RemoteJMXBrokerFacade remoteJMXBrokerFacade = new RemoteJMXBrokerFacade();
        remoteJMXBrokerFacade.setConfiguration(new AbstractConfiguration() {
            public ConnectionFactory getConnectionFactory() {
                return connectionFactory;
            }

            public Collection<JMXServiceURL> getJmxUrls() {
                return makeJmxUrls("service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi");
            }

            public String getJmxPassword() {
                return null;
            }

            public String getJmxUser() {
                return null;
            }

        });

        deadLetterReprocessor.setBrokerQuery(new LocalBrokerFacade(broker));
        if (LOGGER.isDebugEnabled()) {
            dumpJmxBeans();
        }
    }
}
