package org.improving.workshop.qs_1;

// import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
// import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.improving.workshop.Streams;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;

import static org.improving.workshop.utils.DataFaker.TICKETS;
import static org.improving.workshop.utils.DataFaker.VENUES;
import static org.junit.jupiter.api.Assertions.*;

public class MostSoldOutArtistTest {

    private TopologyTestDriver driver;

    // inputs
    private TestInputTopic<String, org.msse.demo.mockdata.music.ticket.Ticket> ticketInput;
    private TestInputTopic<String, org.msse.demo.mockdata.music.event.Event> eventInput;
    private TestInputTopic<String, org.msse.demo.mockdata.music.artist.Artist> artistInput;

    // outputs
    private TestOutputTopic<String, MostSoldOutArtist.MostSoldOutArtistResult> outputTopic;

    @BeforeEach
    void setup() {
        // instantiate new builder
        StreamsBuilder builder = new StreamsBuilder();

        // build the topology
        MostSoldOutArtist.configureTopology(builder);

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(builder.build(), Streams.buildProperties());

        // create input topics
        ticketInput = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_TICKETS,
                Serdes.String().serializer(),
                Streams.SERDE_TICKET_JSON.serializer()
        );

        eventInput = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                Serdes.String().serializer(),
                Streams.SERDE_EVENT_JSON.serializer()
        );

        artistInput = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ARTISTS,
                Serdes.String().serializer(),
                Streams.SERDE_ARTIST_JSON.serializer()
        );

        // create output topic using the SERDE from MostSoldOutArtist
        outputTopic = driver.createOutputTopic(
                MostSoldOutArtist.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                MostSoldOutArtist.SERDE_MOST_SOLD_OUT_ARTIST_RESULT_JSON.deserializer()
        );
    }

    @AfterEach
    void cleanup() {
        driver.close();
    }

    @Test
    @DisplayName("Artist with the most sold-out events")
    void testMostSoldOutArtist() throws InterruptedException {
        // produce an Artist
        var artist = DataFaker.ARTISTS.generate();
        artistInput.pipeInput(artist.id(), artist);

        // create event and tickets
        var event = DataFaker.EVENTS.generate(artist.id(), VENUES.randomId(), 100);
        eventInput.pipeInput(event.id(), event);

        eventInput.pipeInput(event.id(), event);
        artistInput.pipeInput(artist.id(), artist);
        Thread.sleep(200); // allow time for KTable to populate

        eventInput.pipeInput(event.id(), event);
        Thread.sleep(200);


        for (int i = 0; i < 100; i++) {
            ticketInput.pipeInput("ticket-" + i, DataFaker.TICKETS.generate(event.id(), VENUES.randomId()));
            driver.advanceWallClockTime(Duration.ofSeconds(2)); // allow time for stream processing
        }

        Thread.sleep(1000); // allow time for processing

        // read results
        List<TestRecord<String, MostSoldOutArtist.MostSoldOutArtistResult>> outputRecords = outputTopic.readRecordsToList();
        assertTrue(outputRecords.size() > 0, "Expected at least one output record");

        MostSoldOutArtist.MostSoldOutArtistResult finalResult = outputRecords.get(outputRecords.size() - 1).getValue();
        assertEquals(artist.id(), finalResult.getArtistId());
        assertEquals(artist.name(), finalResult.getArtistName());
        assertTrue(finalResult.getSoldOutCount() >= 1);
    }

    @Test
    @DisplayName("No sold-out events should result in no output")
    void testNoSoldOutEvents() {
        // produce an Artist
        var artist = DataFaker.ARTISTS.generate();
        artistInput.pipeInput(artist.id(), artist);

        // create an event but not enough ticket sales
        var event = DataFaker.EVENTS.generate(VENUES.randomId(), artist.id(), 100);
        eventInput.pipeInput(event.id(), event);

        for (int i = 0; i < 50; i++) {
            ticketInput.pipeInput("ticket-" + i, DataFaker.TICKETS.generate(VENUES.randomId(), event.id()));
        }

        // read results
        List<TestRecord<String, MostSoldOutArtist.MostSoldOutArtistResult>> outputRecords = outputTopic.readRecordsToList();
        assertTrue(outputRecords.isEmpty(), "Expected no output records");
    }

    @Test
    @DisplayName("Multiple artists with sold-out events")
    void testMultipleArtists() {
        var artist1 = DataFaker.ARTISTS.generate();
        var artist2 = DataFaker.ARTISTS.generate();
        artistInput.pipeInput(artist1.id(), artist1);
        artistInput.pipeInput(artist2.id(), artist2);

        var event1 = DataFaker.EVENTS.generate(VENUES.randomId(), artist1.id(), 100);
        var event2 = DataFaker.EVENTS.generate(VENUES.randomId(), artist2.id(), 150);
        eventInput.pipeInput(event1.id(), event1);
        eventInput.pipeInput(event2.id(), event2);

        for (int i = 0; i < 100; i++) {
            ticketInput.pipeInput("ticket-" + i, DataFaker.TICKETS.generate(VENUES.randomId(), event1.id()));
        }
        for (int i = 0; i < 147; i++) {
            ticketInput.pipeInput("ticket-" + (i + 100), DataFaker.TICKETS.generate(VENUES.randomId(), event2.id()));
        }

        List<TestRecord<String, MostSoldOutArtist.MostSoldOutArtistResult>> outputRecords = outputTopic.readRecordsToList();
        assertFalse(outputRecords.isEmpty(), "Expected at least one output record");

        MostSoldOutArtist.MostSoldOutArtistResult finalResult = outputRecords.get(outputRecords.size() - 1).getValue();
        assertTrue(finalResult.getArtistId().equals(artist1.id()) || finalResult.getArtistId().equals(artist2.id()));
        assertTrue(finalResult.getSoldOutCount() >= 1);

        outputRecords.forEach(record -> {
            System.out.println("Output Record: " + record.getValue());
        });
    }
}
