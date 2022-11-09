package gmbh.conteco;

import net.bytebuddy.agent.builder.AgentBuilder;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.common.serialization.Serdes.Void;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

// Unit Testing mit JUnit
public class GreetingTopologyTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<Void, String> inputTopic;
    private TestOutputTopic<Void, String> outputTopic;

    @BeforeEach
    void setup() {
        Topology topology = GreetingTopology.build();
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, GreetingTopologyTest.class.getName());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        testDriver = new TopologyTestDriver(topology, properties);

        inputTopic = testDriver.createInputTopic("persons", Void().serializer(), String().serializer());
        outputTopic = testDriver.createOutputTopic("greetings", Void().deserializer(), String().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testGreeting_isNotEmpty() {
        inputTopic.pipeInput("Hans");
        assertThat(outputTopic.isEmpty()).isFalse();
    }

    @Test
    void testGreeting_ignorePerson() {
        inputTopic.pipeInput("Eugen");
        assertThat(outputTopic.isEmpty()).isTrue();

        inputTopic.pipeInput("Hans");
        inputTopic.pipeInput("Kai");

        List<String> valuesFromTopic = outputTopic.readValuesToList();
        List<String> greetings = new ArrayList<>();

        greetings.add("Hallo Hans");
        greetings.add("Hallo Kai");

        assertThat(valuesFromTopic).hasSize(2);
        assertThat(valuesFromTopic).isEqualTo(greetings);
    }
}