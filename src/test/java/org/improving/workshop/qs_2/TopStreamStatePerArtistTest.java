package org.improving.workshop.qs_2;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.improving.workshop.Streams;
import org.improving.workshop.qs_2.TopStreamStatePerArtist.TopStreamStatePerArtistResult;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TopStreamStatePerArtistTest {

    private TopologyTestDriver driver;

    // inputs
    private TestInputTopic<String, org.msse.demo.mockdata.music.stream.Stream> streamInput;
    private TestInputTopic<String, org.msse.demo.mockdata.customer.address.Address> addressInput;
    private TestInputTopic<String, org.msse.demo.mockdata.music.artist.Artist> artistInput;

    // outputs
    private TestOutputTopic<String, TopStreamStatePerArtistResult> outputTopic;

    @BeforeEach
    void setup() {
        // instantiate new builder
        StreamsBuilder builder = new StreamsBuilder();

        // build the topology
        TopStreamStatePerArtist.configureTopology(builder);

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(builder.build(), Streams.buildProperties());

        // create input topics
        streamInput = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_STREAMS,
                Serdes.String().serializer(),
                Streams.SERDE_STREAM_JSON.serializer()
        );

        addressInput = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ADDRESSES,
                Serdes.String().serializer(),
                Streams.SERDE_ADDRESS_JSON.serializer()
        );

        artistInput = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ARTISTS,
                Serdes.String().serializer(),
                Streams.SERDE_ARTIST_JSON.serializer()
        );

        // create output topic using the SERDE from TopStreamStatePerArtist
        outputTopic = driver.createOutputTopic(
                TopStreamStatePerArtist.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                TopStreamStatePerArtist.SERDE_TOP_STREAM_STATE_PER_ARTIST_RESULT_JSON.deserializer()
        );
    }

    @AfterEach
    void cleanup() {
        driver.close();
    }

    @Test
    @DisplayName("state with the most streams for a single artist")
    void testSingleArtistTopState() {
        // produce an Artist
        var artist = DataFaker.ARTISTS.generate();
        artistInput.pipeInput(artist.id(), artist);

        // create addresses - 3 in CA, 2 in NY
        org.msse.demo.mockdata.customer.address.Address addr1 = new org.msse.demo.mockdata.customer.address.Address(
                "addr1", "cust-1", "formatCode", "sType", "1111 Happy St", "line2", "Los Angeles", "CA",
                "zip", "zip5", "countycd", 0.0, 0.0
        );
        org.msse.demo.mockdata.customer.address.Address addr2 = new org.msse.demo.mockdata.customer.address.Address(
                "addr2", "cust-2", "formatCode", "sType", "222 Sunny Ave", "line2", "San Diego", "CA",
                "zip", "zip5", "countycd", 0.0, 0.0
        );
        org.msse.demo.mockdata.customer.address.Address addr3 = new org.msse.demo.mockdata.customer.address.Address(
                "addr3", "cust-3", "formatCode", "sType", "333 Rainy Rd", "line2", "Sacramento", "CA",
                "zip", "zip5", "countycd", 0.0, 0.0
        );
        org.msse.demo.mockdata.customer.address.Address addr4 = new org.msse.demo.mockdata.customer.address.Address(
                "addr4", "cust-4", "formatCode", "sType", "444 Cloudy Blvd", "line2", "New York", "NY",
                "zip", "zip5", "countycd", 0.0, 0.0
        );
        org.msse.demo.mockdata.customer.address.Address addr5 = new org.msse.demo.mockdata.customer.address.Address(
                "addr5", "cust-5", "formatCode", "sType", "555 Windy Ln", "line2", "Albany", "NY",
                "zip", "zip5", "countycd", 0.0, 0.0
        );
        addressInput.pipeInput("cust-1", addr1);
        addressInput.pipeInput("cust-2", addr2);
        addressInput.pipeInput("cust-3", addr3);
        addressInput.pipeInput("cust-4", addr4);
        addressInput.pipeInput("cust-5", addr5);

        // produce streams
        streamInput.pipeInput("stream-1", DataFaker.STREAMS.generate("cust-1", artist.id()));
        streamInput.pipeInput("stream-2", DataFaker.STREAMS.generate("cust-2", artist.id()));
        streamInput.pipeInput("stream-3", DataFaker.STREAMS.generate("cust-3", artist.id()));
        streamInput.pipeInput("stream-4", DataFaker.STREAMS.generate("cust-4", artist.id()));
        streamInput.pipeInput("stream-5", DataFaker.STREAMS.generate("cust-5", artist.id()));

        // read results
        List<TestRecord<String, TopStreamStatePerArtistResult>> outputRecords = outputTopic.readRecordsToList();
        assertFalse(outputRecords.isEmpty(), "Expected at least one output record");

        // expected - CA with 3 streams
        TopStreamStatePerArtistResult finalResult = outputRecords.get(outputRecords.size() - 1).getValue();
        assertEquals(artist.id(), finalResult.getArtistId());
        assertEquals(artist.name(), finalResult.getArtistName());
        assertEquals("CA", finalResult.getState());
        assertEquals(Long.valueOf(3), finalResult.getStreamCount());
    }

    @Test
    @DisplayName("state with the most streams when multiple states tie")
    void testTieStates() {
        // produce an Artist
        var artist = DataFaker.ARTISTS.generate();
        artistInput.pipeInput(artist.id(), artist);

        // create addresses - 2 in CA, 2 in NY
        org.msse.demo.mockdata.customer.address.Address addr1 = new org.msse.demo.mockdata.customer.address.Address(
                "addr1", "cust-1", "formatCode", "sType", "1111 Happy St", "line2", "Los Angeles", "CA",
                "zip", "zip5", "countycd", 0.0, 0.0
        );
        org.msse.demo.mockdata.customer.address.Address addr2 = new org.msse.demo.mockdata.customer.address.Address(
                "addr2", "cust-2", "formatCode", "sType", "222 Sunny Ave", "line2", "San Diego", "CA",
                "zip", "zip5", "countycd", 0.0, 0.0
        );
        org.msse.demo.mockdata.customer.address.Address addr3 = new org.msse.demo.mockdata.customer.address.Address(
                "addr3", "cust-3", "formatCode", "sType", "333 Rainy Rd", "line2", "New York", "NY",
                "zip", "zip5", "countycd", 0.0, 0.0
        );
        org.msse.demo.mockdata.customer.address.Address addr4 = new org.msse.demo.mockdata.customer.address.Address(
                "addr4", "cust-4", "formatCode", "sType", "444 Cloudy Blvd", "line2", "Albany", "NY",
                "zip", "zip5", "countycd", 0.0, 0.0
        );
        addressInput.pipeInput("cust-1", addr1);
        addressInput.pipeInput("cust-2", addr2);
        addressInput.pipeInput("cust-3", addr3);
        addressInput.pipeInput("cust-4", addr4);

        // produce Streams
        streamInput.pipeInput("stream-1", DataFaker.STREAMS.generate("cust-1", artist.id()));
        streamInput.pipeInput("stream-2", DataFaker.STREAMS.generate("cust-2", artist.id()));
        streamInput.pipeInput("stream-3", DataFaker.STREAMS.generate("cust-3", artist.id()));
        streamInput.pipeInput("stream-4", DataFaker.STREAMS.generate("cust-4", artist.id()));

        // read results
        List<TestRecord<String, TopStreamStatePerArtistResult>> outputRecords = outputTopic.readRecordsToList();
        assertFalse(outputRecords.isEmpty(), "Expected at least one output record");

        // TODO: check that state exists
        TopStreamStatePerArtistResult finalResult = outputRecords.get(outputRecords.size() - 1).getValue();
        assertEquals(artist.id(), finalResult.getArtistId());
        assertEquals(artist.name(), finalResult.getArtistName());
        assertNotNull(finalResult.getState());
        assertNotNull(finalResult.getStreamCount());
    }
}
