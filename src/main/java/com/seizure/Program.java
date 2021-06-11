package com.seizure;

import com.seizure.configuration.Configuration;
import com.seizure.models.ConnectionInfo;
import com.seizure.models.PubSubTableInfo;
import com.seizure.publisher.ChangeDataCapture;
import com.seizure.publisher.Publication;
import com.seizure.subscriber.DataListener;
import com.seizure.subscriber.SettingUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Program {
    private static final Logger logger = LogManager.getLogger(Program.class);

    private static boolean isStarted = false;
    private DataListener dataListener = null;
    private ChangeDataCapture changeDataCapture = null;

    public void start(String confContent) throws Exception {

        if (isStarted == true) {
            logger.info("the seizure is already started...");
            return;
        }
        isStarted = true;

        final URL url = getClass().getClassLoader().getResource("log4j2.xml");
        if (Optional.ofNullable(url).isEmpty()) {
            throw new IllegalStateException("cannot read the log configuration file");
        }
        final String path = url.getPath();
        Configurator.initialize(null, path);

        logger.info("reading the configuration data ...");

        com.seizure.configuration.Configuration configuration = new Configuration(confContent);
        final ConnectionInfo publisherConnectionInfo = configuration.getPublisherConnectionInfo();
        final ConnectionInfo subscriberConnectionInfo = configuration.getSubscriberConnectionInfo();
        final List<PubSubTableInfo> pubSubTableInfoList = configuration.getPubSubTableInfoList();

        final String publicationName = configuration.getPublicationName();
        final String slot = configuration.getPublisherSlot();

        final int maxTasks = configuration.getMaxTasks();
        final int batchSize = configuration.getBatchSize();
        logger.info("done.");

        logger.info("setting up...");
        try (SettingUp settingUp = new SettingUp(publisherConnectionInfo, subscriberConnectionInfo, pubSubTableInfoList)) {
            settingUp.createSubscriberTables();
        }
        logger.info("done.");

        logger.info("publication ...");
        try (Publication publication = new Publication(publisherConnectionInfo, publicationName, pubSubTableInfoList)) {
            publication.initializePublication();
        }
        logger.info("done.");

        ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> concurrentHashMap = new ConcurrentHashMap<>();
        logger.info("starting the change data capture process ...");
        changeDataCapture = new ChangeDataCapture(concurrentHashMap, publisherConnectionInfo, publicationName, slot, true, true, false);
        changeDataCapture.start();
        logger.info("done.");

        logger.info("trying to start the data listener ...");
        ConcurrentLinkedQueue<String> queue = concurrentHashMap.get(publisherConnectionInfo.getDatabase());
        dataListener = new DataListener(queue, maxTasks, batchSize, subscriberConnectionInfo, pubSubTableInfoList);
        dataListener.start();
        logger.info("done.");
    }

    public void stop() {

        if (dataListener != null) {
            logger.info("trying to interrupt the work of DataListener class...");
            dataListener.interrupt();

            while (dataListener.isAlive() == true) {
                try {
                    logger.info("it is alive, lets wait a little, and try again...");
                    Thread.sleep(10); // Sleep 10 millis
                } catch (InterruptedException ex) {
                }
            }
            dataListener = null;
            logger.info("done.");
        }

        if (changeDataCapture != null) {
            logger.info("trying to interrupt the work of ChangeDataCapture class...");
            changeDataCapture.interrupt();

            while (changeDataCapture.isAlive() == true) {
                try {
                    logger.info("it is alive, lets wait a little, and try again...");
                    Thread.sleep(10); // Sleep 10 millis
                } catch (InterruptedException ex) {
                }
            }
            changeDataCapture = null;
            logger.info("done.");
        }

        isStarted = false;
    }

    public long getRecordsCount() {
        if (dataListener != null) {
            return dataListener.getRecordsCount();
        }
        return 0;
    }
}
