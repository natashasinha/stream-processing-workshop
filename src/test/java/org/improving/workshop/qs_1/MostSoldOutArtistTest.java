package org.improving.workshop.qs_1;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.improving.workshop.Streams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;

import static org.improving.workshop.utils.DataFaker.TICKETS;

public class MostSoldOutArtistTest {

    private final static Serializer<String> stringSerializer = Serdes.String().serializer();
    private final static Deserializer<String> stringDeserializer = Serdes.String().deserializer();

    private TopologyTestDriver driver;

    // inputs
    private TestInputTopic<String, Event> eventInputTopic;
    private TestInputTopic<String, Ticket> ticketInputTopic;

    //outputs
    private TestOutputTopic<String, LinkedHashMap<String, Long>> outputTopic;

    @BeforeEach
    void setup() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        MostSoldOutArtist.configureTopology(streamsBuilder);
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

        eventInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                Serdes.String().serializer(),
                Streams.SERDE_EVENT_JSON.serializer()
        );

        ticketInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_TICKETS,
                Serdes.String().serializer(),
                Streams.SERDE_TICKET_JSON.serializer()
        );

//        outputTopic = driver.createOutputTopic(
//                MostSoldOutArtist.OUTPUT_TOPIC,
//                Serdes.String().deserializer(),
//                MostSoldOutArtist.LINKED_HASH_MAP_JSON_SERDE.deserializer()
//        );
    }

    @AfterEach
    void cleanup() {
        driver.close();
    }

    @Test
    @DisplayName("Most sold-out artist calculation")
    void mostSoldOutArtistTest() {
        String event1 = "event-1";
        String event2 = "event-2";
        String artist1 = "artist-1";
        String artist2 = "artist-2";

        eventInputTopic.pipeInput(event1, new Event(event1, artist1, "venue-1", 3, "today"));
        eventInputTopic.pipeInput(event2, new Event(event2, artist2, "venue-2", 2, "today"));

        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", event1));
        ticketInputTopic.pipeInput(TICKETS.generate("customer-2", event1));
        ticketInputTopic.pipeInput(TICKETS.generate("customer-3", event1));
        ticketInputTopic.pipeInput(TICKETS.generate("customer-4", event2));
        ticketInputTopic.pipeInput(TICKETS.generate("customer-5", event2));

        var outputRecords = outputTopic.readRecordsToList();

        assertEquals(2, outputRecords.size());
        assertEquals(artist1, outputRecords.getLast().key());
    }
}
