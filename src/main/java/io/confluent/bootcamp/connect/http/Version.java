package io.confluent.bootcamp.connect.http;

// See https://github.com/remkop/picocli/blob/main/picocli-examples/src/main/java/picocli/examples/VersionProviderDemo2.java

import io.confluent.bootcamp.RailScheduleDownloader;
import picocli.CommandLine;

public class Version implements CommandLine.IVersionProvider {
    public String[] getVersion() {
        Package mainPackage = RailScheduleDownloader.class.getPackage();
        String version = mainPackage.getImplementationVersion();

        return new String[] { version};
    }
}
