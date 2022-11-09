package gmbh.conteco;

public class TransactionalProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        try(InputStream stream = TransactionalProducer.class.getResourceAsStream("/application.properties")) {
            properties.load(stream);
        }
    }
}