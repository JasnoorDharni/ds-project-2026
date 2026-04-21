package it.unitn.ds;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public final class Logger {

    public enum Destination {
        STDOUT,
        FILE
    }

    private static final Object LOCK = new Object();
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static Destination destination = Destination.STDOUT;
    private static boolean debugEnabled = false;
    private static boolean loggingEnabled = true;

    private static Writer writer =
            new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));

    private static Path logPath = null;

    private Logger() {}

    // -------------------------------------------------
    // Configuration
    // -------------------------------------------------

    public static void setDestinationStdout() {
        synchronized (LOCK) {
            closeWriterIfNeeded();
            destination = Destination.STDOUT;
            writer = new BufferedWriter(
                    new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
        }
    }

    public static void setDestinationFile(String filename) {
        synchronized (LOCK) {
            closeWriterIfNeeded();
            try {
                logPath = Paths.get(filename);
                Files.deleteIfExists(logPath);

                writer = Files.newBufferedWriter(
                        logPath,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                );

                destination = Destination.FILE;

            } catch (IOException e) {
                System.err.println("[Logger] Failed to set file destination: " + e.getMessage());
                setDestinationStdout();
            }
        }
    }

    public static void setDebugEnabled(boolean enabled) {
        debugEnabled = enabled;
    }

    // -------------------------------------------------
    // Global enable/disable
    // -------------------------------------------------

    public static void setLoggingEnabled(boolean enabled) {
        loggingEnabled = enabled;
    }

    public static void disable() {
        loggingEnabled = false;
    }

    public static void enable() {
        loggingEnabled = true;
    }

    // -------------------------------------------------
    // Logging methods
    // -------------------------------------------------

    public static void log(String message) {
        if (!loggingEnabled) return;
        write("INFO", message);
    }

    public static void debug(String message) {
        if (!loggingEnabled || !debugEnabled) return;
        write("DEBUG", message);
    }

    private static void write(String level, String message) {
        String line = String.format(
                "%s [%s] %s%s",
                LocalDateTime.now().format(FORMATTER),
                level,
                message,
                System.lineSeparator()
        );

        synchronized (LOCK) {
            try {
                writer.write(line);
                writer.flush();
            } catch (IOException e) {
                System.err.println("[Logger] Failed to write log: " + e.getMessage());
            }
        }
    }

    private static void closeWriterIfNeeded() {
        try {
            if (destination == Destination.FILE && writer != null) {
                writer.close();
            }
        } catch (IOException ignored) {}
    }
}