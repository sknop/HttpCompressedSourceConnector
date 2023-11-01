package io.confluent.bootcamp;

import io.confluent.bootcamp.connect.http.BasicAuthenticator;

import java.io.*;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

public class ReadFromHttpUrl {
    public static void main(String[] args) {
        String myURLString = args[0];
        String user = args[1];
        String password = args[2];
        // int totalLines = args.length > 2 ? Integer.parseInt(args[3]) : -1;

        Authenticator.setDefault(new BasicAuthenticator(user, password));

        try {
            URL obj = new URL(myURLString);
            HttpURLConnection httpConn = (HttpURLConnection) obj.openConnection();

            int responseCode = httpConn.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_OK) {
                String fileName = "";
                String disposition = httpConn.getHeaderField("Content-Disposition");
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

                // opens input stream from the HTTP connection
                try (GZIPInputStream uncompressStream = new GZIPInputStream(httpConn.getInputStream());
                     Reader decoder = new InputStreamReader(uncompressStream, StandardCharsets.US_ASCII);
                     BufferedReader buffered = new BufferedReader(decoder)) {

                    while (buffered.ready()) {
                        String line = buffered.readLine();
                        System.out.println("==> " + line);
                    }
                }

            } else {
                System.out.println("No file to download. Server replied HTTP code: " + responseCode);
            }

            httpConn.disconnect();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}