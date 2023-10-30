package io.confluent.bootcamp;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.GZIPInputStream;

public class DownloadFromHttpUrl {
    public static void main(String[] args) {
        final int BUFFER_SIZE = 4096;

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
                InputStream inputStream = httpConn.getInputStream();
                GZIPInputStream uncompressStream = new GZIPInputStream(inputStream);
                String saveFilePath = fileName.replace(".gz", "");

                // opens an output stream to save into file
                FileOutputStream outputStream = new FileOutputStream(saveFilePath);

                int bytesRead;
                byte[] buffer = new byte[BUFFER_SIZE];
                while ((bytesRead = uncompressStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }

                outputStream.close();
                inputStream.close();

                System.out.println("File " + saveFilePath + " downloaded");
            } else {
                System.out.println("No file to download. Server replied HTTP code: " + responseCode);
            }

            httpConn.disconnect();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}