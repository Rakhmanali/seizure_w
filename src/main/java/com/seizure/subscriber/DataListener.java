package com.seizure.subscriber;

import com.seizure.models.ConnectionInfo;
import com.seizure.models.PubSubTableInfo;
import com.seizure.services.BasicConnectionPool;
import com.seizure.services.ConnectionPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DataListener extends Thread {
    private static final Logger logger = LogManager.getLogger(DataListener.class);

    private final ConcurrentLinkedQueue<String> queue;
    private final ConnectionPool connectionPool;
    private final String database;
    private final int maxTasks;
    private final int batchSize;

    private final List<PubSubTableInfo> pubSubTableInfoList;

    public DataListener(ConcurrentLinkedQueue<String> queue,
                        int maxTasks,
                        int batchSize,
                        ConnectionInfo connectionInfo,
                        List<PubSubTableInfo> pubSubTableInfoList) throws SQLException {
        this.queue = queue;
        this.maxTasks = maxTasks;
        this.batchSize = batchSize;
        this.database = connectionInfo.getDatabase();
        this.pubSubTableInfoList = pubSubTableInfoList;

        String url = "jdbc:postgresql://" + connectionInfo.getServer() + "/" + database;
        this.connectionPool = BasicConnectionPool.create(url, connectionInfo.getUser(), connectionInfo.getPassword());
    }

    long recordsCount = 0;

    public long getRecordsCount() {
        return this.recordsCount;
    }

    public void run() {
        List<RecordCreator> recordCreators = new ArrayList<>();
        List<String> records;
        int count;
        while (Thread.interrupted() == false) {
            try {
                if (recordCreators.size() < this.maxTasks) {
                    records = new ArrayList<>();
                    for (int i = 0; i < this.batchSize; i++) {
                        if (this.queue.peek() != null) {
                            records.add(this.queue.poll());
                        } else {
                            Thread.sleep(10); // Sleep 10 millis
                        }
                    }
                    if (records.size() > 0) {

                        recordsCount += records.size();

                        RecordCreator recordCreator = new RecordCreator(this.connectionPool.getConnection(), records, this.pubSubTableInfoList);
                        recordCreator.start();
                        recordCreators.add(recordCreator);
                    }

                    if (recordCreators.size() < this.maxTasks) {
                        continue;
                    }
                }

                count = 0;
                Iterator<RecordCreator> threadIterator = recordCreators.iterator();
                while (threadIterator.hasNext()) {
                    RecordCreator recordCreator = threadIterator.next();
                    if (recordCreator.isAlive() == false) {
                        Connection connection = recordCreator.getConnection();
                        this.connectionPool.releaseConnection(connection);
                        threadIterator.remove();
                        count++;
                    }
                }

                if (count != 0) {
                    logger.info("there are {} tasks from {} has been finished", count, this.maxTasks);
                }

                if (recordCreators.size() == this.maxTasks) {
                    logger.info("there is a necessary to wait a little, all tasks busy now ...");
                    Thread.sleep(10); // Sleep 10 millis
                }

            } catch (InterruptedException ex) {
                logger.info("run() - listening of the database: {} has been interrupted...", this.database);
                break;
            } catch (Exception ex) {
                ex.printStackTrace();
                logger.error("run() - {}", ex.toString());
                break;
            }
        }

        logger.info("the work is interrupted...");

        int size;
        while ((size = recordCreators.size()) > 0) {
            logger.info("there are {} tasks is working still, lets wait a little ...", size);

            Iterator<RecordCreator> threadIterator = recordCreators.iterator();
            while (threadIterator.hasNext()) {
                RecordCreator recordCreator = threadIterator.next();
                if (recordCreator.isAlive() == false) {
                    Connection connection = recordCreator.getConnection();
                    this.connectionPool.releaseConnection(connection);
                    threadIterator.remove();
                }
            }

            try {
                Thread.sleep(10); // Sleep 10 millis
            } catch (InterruptedException ex) {
            }
        }

        logger.info("all tasks are finished, trying to release used connections ... ");

        try {
            this.connectionPool.shutdown();
            logger.info("done.");
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("run() - cannot close the connection: {}", ex.toString());
        }
    }
}
