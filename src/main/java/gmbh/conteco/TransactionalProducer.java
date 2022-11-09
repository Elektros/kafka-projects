package gmbh.conteco;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TransactionalProducer {
    public static void main(String[] args) {
        Logger logger = Logger.getLogger("MyFile");
        logger.log(Level.INFO,"Entered main");
        Properties properties = new Properties();

        try(InputStream stream = TransactionalProducer.class.getResourceAsStream("/application.properties")) {
            properties.load(stream);
            logger.log(Level.INFO,"Loaded Properties");
        } catch (IOException e) {
            logger.log(Level.INFO,"Load Properties failed");
            throw new RuntimeException(e);
        }
    }
}