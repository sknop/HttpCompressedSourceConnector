package io.confluent.bootcamp.connect.http;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import org.apache.kafka.connect.errors.ConnectException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static io.confluent.bootcamp.connect.http.Configuration.*;

public class HttpCompressedSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(HttpCompressedSourceTask.class);
    private String urlString;
    private String username;
    private String password;
    private String topic;
    private HttpURLConnection httpConn;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        urlString = props.get(URL_CONFIG);
        username = props.get(USERNAME_CONFIG);
        password = props.get(PASSWORD_CONFIG);
        topic = props.get(TOPIC_CONFIG);

        Authenticator.setDefault(new BasicAuthenticator(username, password));

        try {
            URL obj = new URL(urlString);
             httpConn = (HttpURLConnection) obj.openConnection();

            int responseCode = httpConn.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED) {
                log.error("Failed to authenticate user {}", username);
                throw new ConnectException("Failed to authenticate user " + username);
            }
            else if (responseCode != HttpURLConnection.HTTP_OK) {
                log.error("Failed for unknown reason with {}", responseCode);
                throw new ConnectException("Failed for unknown reason with " + responseCode);
            }
            else { // we got 200, let's proceed
                var fileName = "";
                var disposition = httpConn.getHeaderField("Content-Disposition");
                var contentType = httpConn.getContentType();
                var contentLength = httpConn.getContentLength();

                if (disposition != null) {
                    // extracts file name from header field
                    int index = disposition.indexOf("filename=");
                    if (index > 0) {
                        fileName = disposition.substring(index + 9);
                    }
                } else {
                    // extracts file name from URL
                    fileName = urlString.substring(urlString.lastIndexOf("/") + 1);
                }

                log.info("HttpURLConnection::Disposition: " + disposition);
                log.info("HttpURLConnection::contentType: " + contentType);
                log.info("HttpURLConnection::contentLength: " + contentLength);
                log.info("HttpURLConnection::fileName: " + fileName);
            }

        } catch (MalformedURLException e) {
            log.error("Error in URL {}", urlString, e);
            throw new ConnectException(e);
        } catch (IOException e) {
            log.error("Error while connecting to {}", urlString, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        return null;
    }

    @Override
    public void stop() {
        httpConn.disconnect();
    }
}
