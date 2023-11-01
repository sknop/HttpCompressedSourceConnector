package io.confluent.bootcamp;

import io.confluent.bootcamp.connect.http.BasicAuthenticator;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.zip.GZIPInputStream;

public class DownloadFromHttpUrl {
    public static void main(String[] args) {
        boolean processedHeader = false;
        LocalDateTime dataTimestamp = null;
        
        String myURLString = args[0];
        String user = args[1];
        String password = args[2];

        Authenticator.setDefault(new BasicAuthenticator(user, password));

        try {
            URL obj = new URL(myURLString);
            HttpURLConnection httpConn = (HttpURLConnection) obj.openConnection();

            int responseCode = httpConn.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_OK) {
                String fileName = "";
                String disposition = httpConn.getHeaderField("Content-Disposition");
                var lastModified = httpConn.getLastModified();
                var lastModifiedFromHeader = httpConn.getHeaderField("Last-Modified");
                var headerFields = httpConn.getHeaderFields();
                String contentType = httpConn.getContentType();
                int contentLength = httpConn.getContentLength();

                if (disposition != null) {
                    // extracts file name from header field
                    int index = disposition.indexOf("filename=");
                    if (index > 0) {
                        fileName = disposition.substring(index + 9);
                    }
                } else {
                    // extracts file name from URL
                    fileName = myURLString.substring(myURLString.lastIndexOf("/") + 1);
                }

                System.out.println("Content-Type = " + contentType);
                System.out.println("Content-Disposition = " + disposition);
                System.out.println("Content-Length = " + contentLength);
                System.out.println("fileName = " + fileName);
                System.out.println("Lasty modified timestamp : " + lastModified + " Converted " + Instant.ofEpochMilli(lastModified));
                System.out.println("Last modified from header " + lastModifiedFromHeader + " type = " + lastModifiedFromHeader.getClass());

                for (var header: headerFields.entrySet()) {
                    System.out.println("Header: " + header);
                }

                String saveFilePath = fileName.replace(".gz", "");
                int totalLines = 0;
                try (GZIPInputStream uncompressStream = new GZIPInputStream(httpConn.getInputStream());
                     Reader decoder = new InputStreamReader(uncompressStream, StandardCharsets.US_ASCII);
                     BufferedReader buffered = new BufferedReader(decoder);
                     BufferedWriter writer = new BufferedWriter(new FileWriter(saveFilePath))) {

                    while (buffered.ready()) {
                        String line = buffered.readLine();

                        if (!processedHeader) {
                            dataTimestamp = processHeader(line);
                            processedHeader= true;
                        }
                        writer.write(line);
                        writer.newLine();
                        totalLines++;
                    }
                }

                System.out.println("File " + saveFilePath + " downloaded.");
                System.out.println("Read " + totalLines + " lines.");
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                assert dataTimestamp != null;
                System.out.println("Timestamp is " + dataTimestamp.format(formatter));
            } else {
                System.out.println("No file to download. Server replied HTTP code: " + responseCode);
            }

            httpConn.disconnect();
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private static LocalDateTime processHeader(String line) throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse(line);
        JSONObject entry = (JSONObject) jsonObject.get("JsonTimetableV1");
        long epochSecond = (Long) entry.get("timestamp");

        return LocalDateTime.ofEpochSecond(epochSecond, 0, ZoneOffset.UTC);
    }
}