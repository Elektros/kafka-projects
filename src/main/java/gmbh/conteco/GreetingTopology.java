package gmbh.conteco;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public class GreetingTopology {
    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream("persons", Consumed.with(Serdes.Void(), Serdes.String()))
                .filterNot((key, value) -> value.equals("Eugen"))
                .mapValues(value -> "Hallo " + value)
                .to("greetings", Produced.with(Serdes.Void(), Serdes.String()));

        return builder.build();
    }
}
