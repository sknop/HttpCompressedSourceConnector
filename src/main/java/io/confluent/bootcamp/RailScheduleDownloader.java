package io.confluent.bootcamp;

import io.confluent.bootcamp.connect.http.BasicAuthenticator;
import io.confluent.bootcamp.connect.http.Version;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.*;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.zip.GZIPInputStream;

@CommandLine.Command(
        scope = CommandLine.ScopeType.INHERIT,
        synopsisHeading = "%nUsage:%n",
        descriptionHeading   = "%nDescription:%n%n",
        parameterListHeading = "%nParameters:%n%n",
        optionListHeading    = "%nOptions:%n%n",
        mixinStandardHelpOptions = true,
        sortOptions = false,
        versionProvider = Version.class,
        description = "Download compressed Schedule file from URL, and print it/save it/produce it.")
public class RailScheduleDownloader implements Callable<Integer>  {
    private static final Logger logger = LoggerFactory.getLogger(RailScheduleDownloader.class);
    @CommandLine.Option(names = {"-c", "--config-file"}, required = true,
            description = "All configurations go in here")
    protected String configFile = null;

    @CommandLine.Option(names = {"-v", "--verbose"}, description = "Print out every line to stdout")
    boolean verbose;

    @CommandLine.Option(names = {"-s", "--save"}, description = "Save file locally")
    boolean save;

    @CommandLine.Option(names = {"-F", "--kafka-config"}, description = "Use this config file and produce to Kafka")
    String kafkaConfigFile;

    @CommandLine.Option(names = {"-t", "--topic"}, defaultValue ="CIF_FULL_DAILY", description = "The topic to write to [CIF_FULL_DAILY]")
    String topic;

    @CommandLine.Option(names = {"--day"}, defaultValue = "toc-full", description = "The file to download, default is toc-full, otherwise toc-update-DAY")
    String day;

    @CommandLine.Option(names = {"--type"}, defaultValue = "CIF_ALL_FULL_DAILY", description = "The download type [${DEFAULT-VALUE}] or [CIF_ALL_UPDATE_DAILY]")
    String type;

    @CommandLine.Option(names = {"--skip-lines"}, description = "How many lines to skip from the file [${DEFAULT-VALUE}]")
    long skipLines = 0;

    @CommandLine.Option(names = {"--skip-bytes"}, description = "How many bytes to skip from the file [${DEFAULT-VALUE}]")
    long skipBytes = 0;

    @CommandLine.Option(names = {"--max-lines"}, description = "How many lines to process before cancelling [${DEFAULT-VALUE}]")
    long maxLines = -1;

    private String urlString;
    private String username;
    private String password;

    // Counting individual bytes for skipping bytes
    private long index = 0;

    public RailScheduleDownloader() { }

    public void processConfigFile() {
        logger.info("Reading config file " + configFile);

        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            Reader reader = new InputStreamReader(inputStream);

            properties.load(reader);

            urlString = properties.getProperty("urlString");
            username = properties.getProperty("username");
            password = properties.getProperty("password");

        } catch (FileNotFoundException e) {
            logger.error("Inputfile " + configFile + " not found");
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void readFromUrl() {
        Authenticator.setDefault(new BasicAuthenticator(username, password));

        var fullUrl = String.format("%s&type=%s&day=%s",urlString, type,day);
        logger.info(fullUrl);

        try {
            URL url = new URL(fullUrl);
            HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();

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
                    // could make optional
                    fileName = day;
                }

                System.out.println("Content-Type = " + contentType);
                System.out.println("Content-Disposition = " + disposition);
                System.out.println("Content-Length = " + contentLength);
                System.out.println("fileName = " + fileName);

                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                var localDataTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(lastModified), ZoneOffset.UTC);

                System.out.println("Lasty modified timestamp : " + lastModified + " Converted " + localDataTime.format(formatter));
                System.out.println("Last modified from header " + lastModifiedFromHeader + " type = " + lastModifiedFromHeader.getClass());

                for (var header: headerFields.entrySet()) {
                    System.out.println("Header: " + header);
                }

                String saveFilePath = fileName.replace(".gz", "");

                int totalLines;

                if (save) {
                    totalLines = saveFile(httpConn, saveFilePath);
                }
                else if (kafkaConfigFile != null && !kafkaConfigFile.isEmpty()) {
                    totalLines = produceFile(httpConn);
                }
                else {
                    totalLines = printFile(httpConn);
                }
                System.out.println("Read " + totalLines + " lines.");
                System.out.println("Total lines including skipped: " + (totalLines+skipLines));
                System.out.println("Current index is " + index);
            } else {
                System.out.println("No file to download. Server replied HTTP code: " + responseCode);
            }

            httpConn.disconnect();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int produceFile(HttpURLConnection httpConn) throws IOException {
        int totalLines = 0;

        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(kafkaConfigFile)) {
            Reader reader = new InputStreamReader(inputStream);

            properties.load(reader);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        } catch (FileNotFoundException e) {
            logger.error("KafkaConfig " + kafkaConfigFile + " not found");
            throw new RuntimeException(e);
        }

        KafkaProducer<Void, String> producer = new KafkaProducer<>(properties);

        try (GZIPInputStream uncompressStream = new GZIPInputStream(httpConn.getInputStream());
             Reader decoder = new InputStreamReader(uncompressStream, StandardCharsets.US_ASCII);
             BufferedReader buffered = new BufferedReader(decoder)) {

            while (buffered.ready()) {
                String line = buffered.readLine();

                ProducerRecord<Void, String> record = new ProducerRecord<>(topic, line);

                if (verbose) {
                    producer.send(record, ((recordMetadata, e) -> {
                        if (e != null) {
                            throw new RuntimeException(e);
                        }
                        else {
                            var valueSize = recordMetadata.serializedValueSize();
                            System.out.println("Produced [" + valueSize
                                    + "] at offset " + recordMetadata.offset()
                                    + " in partition " + recordMetadata.partition()
                                    + " with data " + record);
                        }
                    }));
                }
                else {
                    producer.send(record);
                    if (totalLines % 1000 == 0) {
                        logger.info("Produced {}", totalLines);
                    }
                }

                totalLines++;
            }
        }
        producer.close();

        return totalLines;
    }

    private int printFile(HttpURLConnection httpConn) throws IOException{
        int totalLines = 0;
        try (GZIPInputStream uncompressStream = new GZIPInputStream(httpConn.getInputStream());
             Reader decoder = new InputStreamReader(uncompressStream, StandardCharsets.US_ASCII);
             BufferedReader buffered = new BufferedReader(decoder)) {

            if (skipLines > 0) {
                long linesToSkip = skipLines;
                while (linesToSkip > 0 && buffered.ready()) {
                    String line = buffered.readLine();
                    index += line.length() + 1;
                    linesToSkip -= 1;
                }
                logger.info("Skipped {} lines", skipLines - linesToSkip);
            }
            else if (skipBytes > 0) {
                long bytesToSkip = skipBytes;
                while (bytesToSkip > 0) {
                    long skipped = buffered.skip(bytesToSkip);
                    bytesToSkip -= skipped;
                    logger.info("Skipped {} bytes", skipped);
                }

                index = skipBytes;
            }
            while (buffered.ready()) {
                String line = buffered.readLine();
                index += line.length() + 1;

                System.out.println(line);

                totalLines++;
                if (maxLines > 0 && totalLines >= maxLines) {
                    logger.info("Stopped processing after {} lines", totalLines);
                    break;
                }
            }
        }
        return totalLines;
    }

    private int saveFile(HttpURLConnection httpConn, String saveFilePath) throws IOException {
        int totalLines = 0;
        try (GZIPInputStream uncompressStream = new GZIPInputStream(httpConn.getInputStream());
             Reader decoder = new InputStreamReader(uncompressStream, StandardCharsets.US_ASCII);
             BufferedReader buffered = new BufferedReader(decoder);
             BufferedWriter writer = new BufferedWriter(new FileWriter(saveFilePath))) {

            while (buffered.ready()) {
                String line = buffered.readLine();

                if (save) {
                    writer.write(line);
                    writer.newLine();
                }
                if (verbose) {
                    System.out.println(line);
                }
                totalLines++;
            }
        }

        if (save)
            System.out.println("File " + saveFilePath + " downloaded.");
        return totalLines;
    }

//    private static LocalDateTime processHeader(String line) throws ParseException {
//        JSONParser parser = new JSONParser();
//        JSONObject jsonObject = (JSONObject) parser.parse(line);
//        JSONObject entry = (JSONObject) jsonObject.get("JsonTimetableV1");
//        long epochSecond = (Long) entry.get("timestamp");
//
//        return LocalDateTime.ofEpochSecond(epochSecond, 0, ZoneOffset.UTC);
//    }

    @Override
    public Integer call() {

        processConfigFile();
        readFromUrl();

        return 0;
    }

    public static void main(String[] args) {
        new CommandLine(new RailScheduleDownloader()).execute(args);
    }
}