package timely.validator;

/**
 * Simple server interface
 */
public interface TimelyServer {

    void setup();

    void run() throws Exception;

    void shutdown();

}
