package edu.brown.library.repository.ocflhttp;

import javax.json.Json;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class OcflHttpConfig {

    public static int DEFAULT_FILE_SIZE_THRESHOLD = 2500000;
    public static int DEFAULT_PORT = 8000;

    public int port;
    public int minThreads;
    public int maxThreads;
    public Path repoRootDir;
    public Path workDir;
    public int fileSizeThreshold;
    public List<Path> allowedUploadDirs;

    public OcflHttpConfig() throws IOException {
        setDefaults();
    }

    public OcflHttpConfig(String[] args) throws IOException {
        setDefaults();
        if(args.length == 1) {
            var pathToConfigFile = args[0];
            try(InputStream is = Files.newInputStream(Path.of(pathToConfigFile))) {
                var reader = Json.createReader(is);
                var object = reader.readObject();
                repoRootDir = Path.of(object.getString("OCFL-ROOT"));
                port = object.getInt("PORT", DEFAULT_PORT);
                minThreads = object.getInt("JETTY_MIN_THREADS", -1);
                maxThreads = object.getInt("JETTY_MAX_THREADS", -1);
                var workDirParam = object.getString("WORK-DIRECTORY", null);
                if(workDirParam != null) {
                    workDir = Path.of(workDirParam);
                }
                fileSizeThreshold = object.getInt("FILE_SIZE_THRESHOLD", DEFAULT_FILE_SIZE_THRESHOLD);
                var allowedUploadDirsInfo = object.getJsonArray("ALLOWED-UPLOAD-DIRS");
                if (allowedUploadDirsInfo != null) {
                    int index = 0;
                    while (index < allowedUploadDirsInfo.size()) {
                        final var path = allowedUploadDirsInfo.getString(index);
                        allowedUploadDirs.add(Path.of(path));
                        index++;
                    }
                }
            }
        }
    }

    void setDefaults() {
        port = DEFAULT_PORT;
        minThreads = -1;
        maxThreads = -1;
        var tmp = System.getProperty("java.io.tmpdir");
        repoRootDir = Path.of(tmp).resolve("ocfl-java-http");
        workDir = Path.of(tmp);
        fileSizeThreshold = DEFAULT_FILE_SIZE_THRESHOLD;
        allowedUploadDirs = new ArrayList<>();
    }
}
