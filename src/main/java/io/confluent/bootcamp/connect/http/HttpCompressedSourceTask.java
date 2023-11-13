package io.confluent.bootcamp.connect.http;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import org.apache.kafka.connect.errors.ConnectException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class HttpCompressedSourceTask extends SourceTask {
    private static final Logger logger = LoggerFactory.getLogger(HttpCompressedSourceTask.class);
    private static final String URL_FIELD = "url";
    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final String CURRENT_LINE_FIELD = "current_line";
    public static final String EOF_TRUE = "{\"EOF\":true}";

    private HttpURLConnection httpConn;

    private HttpCompressedSourceConfiguration config;
    private BufferedReader bufferedReader = null;

    long lastModified = 0;
    long totalLines = 0;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting HttpCompressedSource Connector");
        config = new HttpCompressedSourceConfiguration(props);

        Authenticator.setDefault(new BasicAuthenticator(config.username, config.password));
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        long linesToSkip = 0;

        if (httpConn == null) {
            lastModified = openHttpConnection();

            Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(URL_FIELD, config.url));
            if (offset != null) {
                //   Find the date first from the offset and compare it with the date from the header
                //   Is it the same date?
                //      Look for the EOF marker
                //      If Set --> Synchronized Wait
                //      Else
                //         Find offset
                //           Skip offset messages
                //         Edge case -> Offset date is set but nothing read yet ?? Not possible
                Long lastTimestamp = (Long) offset.get(TIMESTAMP_FIELD);
                Long current_line = (Long) offset.get(CURRENT_LINE_FIELD);

                if (lastTimestamp == lastModified) {
                    if (current_line > 0) {
                        // We have been here before but got interrupted.
                        linesToSkip = current_line;
                        totalLines = current_line;
                    }
                    else {
                        // standard case: we have processed today's file and there is nothing to do for now
                        httpConn.disconnect();
                        httpConn = null;

                        logger.info("Nothing to do, sleeping for {} ms", config.taskPause); // TODO was trace

                        synchronized (this) {
                            this.wait(config.taskPause);
                        }

                        return null;
                    }
                }

            }
            // If there is no offset, we have not tried to read anything yet -> start fresh
        }
        // skip lines if call was interrupted
        // read n lines from stream (pagination)
        //    Create Record with line, set line as offset
        //    If EOF encountered (and/or stream closed), set EOF marker, close stream, exit method with remaining records
        //    if maximum lines hit, exit the method with the records

        try {
            if (bufferedReader == null) {
                GZIPInputStream uncompressStream = new GZIPInputStream(httpConn.getInputStream());
                Reader decoder = new InputStreamReader(uncompressStream, StandardCharsets.US_ASCII);
                bufferedReader = new BufferedReader(decoder);
            }

            if (linesToSkip > 0) {
                logger.info("Skipping {} lines", linesToSkip);
                while (linesToSkip > 0 && bufferedReader.ready()) {
                    bufferedReader.readLine();
                    linesToSkip -= 1;
                }
            }

            int currentLine = 0;
            boolean eof = false;
            List<SourceRecord> records = new ArrayList<>();
            while (bufferedReader.ready()) {
                String line = bufferedReader.readLine();
                if (line.equals(EOF_TRUE)) {
                    totalLines = -1;
                    eof = true;
                }
                else {
                    currentLine++;
                    totalLines++;
                }

                records.add(
                        new SourceRecord(
                                offsetKey(config.url),
                                offsetValue(lastModified, totalLines),
                                config.topic,
                                null,
                                null,
                                null,
                                Schema.STRING_SCHEMA,
                                line,
                                System.currentTimeMillis())
                );

                if (currentLine >= config.pageSize) {
                    return records;
                }
                if (eof) {
                    closeReaderAndConnection();

                    return records;
                }
            }

            logger.warn("Should never get here. This means the file does not end with {}}",EOF_TRUE);

            // Add dummy record to ensure the connector pauses until the timestamp changes

            records.add(
                    new SourceRecord(
                            offsetKey(config.url),
                            offsetValue(lastModified, -1),
                            config.topic,
                            null,
                            null,
                            null,
                            Schema.STRING_SCHEMA,
                            EOF_TRUE,
                            System.currentTimeMillis())
            );
            closeReaderAndConnection();

            return records;
        } catch (IOException e) {
            // Something went wrong. Log it, return null
            logger.error("Something went wrong!", e);
        }
        return null;
    }

    private void closeReaderAndConnection() throws IOException {
        bufferedReader.close();
        bufferedReader = null;
        httpConn.disconnect();
        httpConn = null;
    }

    private Map<String, String> offsetKey(String url) {
        return Collections.singletonMap(URL_FIELD, url);
    }

    private Map<String, ?> offsetValue(long timestamp, long currentLine) {
        Map<String, Object> map = new HashMap<>();
        map.put(TIMESTAMP_FIELD, timestamp);
        map.put(CURRENT_LINE_FIELD, currentLine);

        return map;
    }
    private long openHttpConnection() {
        long lastModified;
        try {
            URL obj = new URL(config.url);
            httpConn = (HttpURLConnection) obj.openConnection();

            int responseCode = httpConn.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED) {
                logger.error("Failed to authenticate user {}", config.username);
                throw new ConnectException("Failed to authenticate user " + config.username);
            }
            else if (responseCode != HttpURLConnection.HTTP_OK) {
                logger.error("Failed for unknown reason with {}", responseCode);
                throw new ConnectException("Failed for unknown reason with " + responseCode);
            }
            else { // we got 200, let's proceed
                var disposition = httpConn.getHeaderField("Content-Disposition");
                var contentType = httpConn.getContentType();
                var contentLength = httpConn.getContentLength();
                lastModified = httpConn.getLastModified();
                var lastModifiedFromHeader = httpConn.getHeaderField("Last-Modified");

                logger.info("HttpURLConnection::Disposition: {}", disposition);
                logger.info("HttpURLConnection::contentType: {}", contentType);
                logger.info("HttpURLConnection::contentLength: {}", contentLength);
                logger.info("HttpURLConnection::lastModified: {}", lastModifiedFromHeader);
            }

        } catch (MalformedURLException e) {
            logger.error("Error in URL {}", config.url, e);
            throw new ConnectException(e);
        } catch (IOException e) {
            logger.error("Error while connecting to {}", config.url, e);
            throw new ConnectException(e);
        }

        return lastModified;
    }

    @Override
    public void stop() {
        logger.info("Invoked stop"); // TODO should be trace

        if (bufferedReader != null) {
            try {
                bufferedReader.close();
            } catch (IOException e) {
                throw new ConnectException(e);
            }
        }
        bufferedReader = null;

        if (httpConn != null) {
            httpConn.disconnect();
        }
        httpConn = null;
    }
}
