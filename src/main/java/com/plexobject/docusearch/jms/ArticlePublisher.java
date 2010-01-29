package com.plexobject.docusearch.jms;

import java.io.File;
import java.io.IOException;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MessageConversionException;
import org.springframework.jms.support.converter.MessageConverter;

import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.sun.jersey.spi.inject.Inject;

public class ArticlePublisher {
    static Logger LOGGER = Logger.getLogger(ArticlePublisher.class);
    @Autowired
    @Inject
    private JmsTemplate jmsTemplate;

    @Autowired
    @Inject
    private Queue queue;

    public ArticlePublisher() {

    }

    public void run(File inputFile) {
        final Timer timer = Metric.newTimer("ArticlePublisher.run");
        try {
            jmsTemplate.setMessageConverter(new MessageConverter() {

                @Override
                public Object fromMessage(Message arg0) throws JMSException,
                        MessageConversionException {
                    return null;
                }

                @Override
                public Message toMessage(Object strJson, Session session)
                        throws JMSException, MessageConversionException {
                    try {
                        final JSONArray arr = new JSONArray(strJson.toString());
                        LOGGER.info("Sending " + arr.length() + " messages to "
                                + queue);
                        return session.createTextMessage(arr.toString());
                    } catch (JSONException e) {
                        throw new MessageConversionException(
                                "failed to convert " + strJson, e);
                    }
                }

            });

            final String jsonStr = FileUtils.readFileToString(inputFile);
            jmsTemplate.convertAndSend(queue, jsonStr);

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            timer.stop();
        }
    }

    private static void usage() {
        System.err.println("Usage: <file-name> ");
        System.exit(1);
    }

    public static void main(String[] args) {
        Logger root = Logger.getRootLogger();
        root.setLevel(Level.INFO);

        root.addAppender(new ConsoleAppender(new PatternLayout(
                PatternLayout.TTCC_CONVERSION_PATTERN)));
        if (args.length != 1) {
            usage();
        }
        try {
            XmlBeanFactory factory = new XmlBeanFactory(new FileSystemResource(
                    "src/main/webapp/WEB-INF/applicationContext.xml"));
            final ArticlePublisher articlePublisher = new ArticlePublisher();
            articlePublisher.queue = (Queue) factory.getBean("secondary_test_dataQ");
            articlePublisher.jmsTemplate = (JmsTemplate) factory
                    .getBean("jmsTemplate");

            articlePublisher.run(new File(args[0]));
            System.exit(0);
        } catch (Exception e) {
            LOGGER.error("Failed to import " + args[0] + "/" + args[1], e);
        }
    }
}
