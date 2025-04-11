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
import org.msse.demo.mockdata.customer.address.Address;

import java.util.List;
import java.util.Optional;

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
        StreamsBuilder builder = new StreamsBuilder();
        TopStreamStatePerArtist.configureTopology(builder);
        driver = new TopologyTestDriver(builder.build(), Streams.buildProperties());

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
        var artist = DataFaker.ARTISTS.generate();
        artistInput.pipeInput(artist.id(), artist);

        // create addresses
        Address addr1 = new Address(
                "addr1", "cust-1", "formatCode", "sType", "1111 Happy St", "line2", "Los Angeles", "CA",
                "zip", "zip5", "US", 0.0, 0.0);
        Address addr2 = new Address(
                "addr2", "cust-2", "formatCode", "sType", "222 Sunny Ave", "line2", "San Diego", "CA",
                "zip", "zip5", "US", 0.0, 0.0);
        Address addr3 = new Address(
                "addr3", "cust-3", "formatCode", "sType", "333 Rainy Rd", "line2", "Sacramento", "CA",
                "zip", "zip5", "US", 0.0, 0.0);
        Address addr4 = new Address(
                "addr4", "cust-4", "formatCode", "sType", "444 Cloudy Blvd", "line2", "New York", "NY",
                "zip", "zip5", "US", 0.0, 0.0);
        Address addr5 = new Address(
                "addr5", "cust-5", "formatCode", "sType", "555 Windy Ln", "line2", "Albany", "NY",
                "zip", "zip5", "US", 0.0, 0.0);
        addressInput.pipeInput(addr1.id(), addr1);
        addressInput.pipeInput(addr2.id(), addr2);
        addressInput.pipeInput(addr3.id(), addr3);
        addressInput.pipeInput(addr4.id(), addr4);
        addressInput.pipeInput(addr5.id(), addr5);

        // produce streams
        streamInput.pipeInput("stream-1", DataFaker.STREAMS.generate("cust-1", artist.id()));
        streamInput.pipeInput("stream-2", DataFaker.STREAMS.generate("cust-2", artist.id()));
        streamInput.pipeInput("stream-3", DataFaker.STREAMS.generate("cust-3", artist.id()));
        streamInput.pipeInput("stream-4", DataFaker.STREAMS.generate("cust-4", artist.id()));
        streamInput.pipeInput("stream-5", DataFaker.STREAMS.generate("cust-5", artist.id()));

        // read output records
        List<TestRecord<String, TopStreamStatePerArtistResult>> outputRecords = outputTopic.readRecordsToList();
        assertFalse(outputRecords.isEmpty(), "Expected at least one output record");

        // check last output record
        TopStreamStatePerArtistResult finalResult = outputRecords.get(outputRecords.size() - 1).getValue();
        assertEquals(artist.id(), finalResult.getArtistId());
        assertEquals(artist.name(), finalResult.getArtistName());
        // expected - CA with 3 streams
        assertEquals("CA", finalResult.getState());
        assertEquals(Long.valueOf(3), finalResult.getStreamCount());
    }

    @Test
    @DisplayName("state with the most streams when multiple states tie")
    void testTieStates() {
        var artist = DataFaker.ARTISTS.generate();
        artistInput.pipeInput(artist.id(), artist);

        // create addresses
        org.msse.demo.mockdata.customer.address.Address addr1 = new org.msse.demo.mockdata.customer.address.Address(
                "addr1", "cust-1", "formatCode", "sType", "1111 Happy St", "line2", "Los Angeles", "CA",
                "zip", "zip5", "US", 0.0, 0.0);
        org.msse.demo.mockdata.customer.address.Address addr2 = new org.msse.demo.mockdata.customer.address.Address(
                "addr2", "cust-2", "formatCode", "sType", "222 Sunny Ave", "line2", "San Diego", "CA",
                "zip", "zip5", "US", 0.0, 0.0);
        org.msse.demo.mockdata.customer.address.Address addr3 = new org.msse.demo.mockdata.customer.address.Address(
                "addr3", "cust-3", "formatCode", "sType", "333 Rainy Rd", "line2", "New York", "NY",
                "zip", "zip5", "US", 0.0, 0.0);
        org.msse.demo.mockdata.customer.address.Address addr4 = new org.msse.demo.mockdata.customer.address.Address(
                "addr4", "cust-4", "formatCode", "sType", "444 Cloudy Blvd", "line2", "Albany", "NY",
                "zip", "zip5", "US", 0.0, 0.0);
        addressInput.pipeInput(addr1.id(), addr1);
        addressInput.pipeInput(addr2.id(), addr2);
        addressInput.pipeInput(addr3.id(), addr3);
        addressInput.pipeInput(addr4.id(), addr4);

        // produce streams
        streamInput.pipeInput("stream-1", DataFaker.STREAMS.generate("cust-1", artist.id()));
        streamInput.pipeInput("stream-2", DataFaker.STREAMS.generate("cust-2", artist.id()));
        streamInput.pipeInput("stream-3", DataFaker.STREAMS.generate("cust-3", artist.id()));
        streamInput.pipeInput("stream-4", DataFaker.STREAMS.generate("cust-4", artist.id()));

        // read output records
        List<TestRecord<String, TopStreamStatePerArtistResult>> outputRecords = outputTopic.readRecordsToList();
        assertFalse(outputRecords.isEmpty(), "Expected at least one output record");

        // check last output record
        TopStreamStatePerArtistResult finalResult = outputRecords.get(outputRecords.size() - 1).getValue();
        assertEquals(artist.id(), finalResult.getArtistId());
        assertEquals(artist.name(), finalResult.getArtistName());
        assertNotNull(finalResult.getState());
        assertNotNull(finalResult.getStreamCount());
    }

    @Test
    @DisplayName("top state for multiple artists")
    void testMultipleArtistsTopStates() {
        var artistA = DataFaker.ARTISTS.generate();
        var artistB = DataFaker.ARTISTS.generate();
        artistInput.pipeInput(artistA.id(), artistA);
        artistInput.pipeInput(artistB.id(), artistB);

        // artistA - 5 distinct customers (3 in CA, 2 in NY)
        org.msse.demo.mockdata.customer.address.Address aAddr1 =
                new org.msse.demo.mockdata.customer.address.Address("A-addr1", "A-cust-1", "formatCode", "sType",
                        "1111 Happy St", "line2", "Los Angeles", "CA", "zip", "zip5", "US", 0.0, 0.0);
        org.msse.demo.mockdata.customer.address.Address aAddr2 =
                new org.msse.demo.mockdata.customer.address.Address("A-addr2", "A-cust-2", "formatCode", "sType",
                        "222 Sunny Ave", "line2", "San Diego", "CA", "zip", "zip5", "US", 0.0, 0.0);
        org.msse.demo.mockdata.customer.address.Address aAddr3 =
                new org.msse.demo.mockdata.customer.address.Address("A-addr3", "A-cust-3", "formatCode", "sType",
                        "333 Rainy Rd", "line2", "Sacramento", "CA", "zip", "zip5", "US", 0.0, 0.0);
        org.msse.demo.mockdata.customer.address.Address aAddr4 =
                new org.msse.demo.mockdata.customer.address.Address("A-addr4", "A-cust-4", "formatCode", "sType",
                        "444 Cloudy Blvd", "line2", "New York", "NY", "zip", "zip5", "US", 0.0, 0.0);
        org.msse.demo.mockdata.customer.address.Address aAddr5 =
                new org.msse.demo.mockdata.customer.address.Address("A-addr5", "A-cust-5", "formatCode", "sType",
                        "555 Windy Ln", "line2", "Albany", "NY", "zip", "zip5", "US", 0.0, 0.0);

        addressInput.pipeInput(aAddr1.id(), aAddr1);
        addressInput.pipeInput(aAddr2.id(), aAddr2);
        addressInput.pipeInput(aAddr3.id(), aAddr3);
        addressInput.pipeInput(aAddr4.id(), aAddr4);
        addressInput.pipeInput(aAddr5.id(), aAddr5);

        // artistB - 4 distinct customers: (3 in TX, 1 in CA)
        org.msse.demo.mockdata.customer.address.Address bAddr1 =
                new org.msse.demo.mockdata.customer.address.Address("B-addr1", "B-cust-1", "formatCode", "sType",
                        "1111 Cool St", "line2", "Houston", "TX", "zip", "zip5", "countycd", 0.0, 0.0);
        org.msse.demo.mockdata.customer.address.Address bAddr2 =
                new org.msse.demo.mockdata.customer.address.Address("B-addr2", "B-cust-2", "formatCode", "sType",
                        "222 Warm Ave", "line2", "Dallas", "TX", "zip", "zip5", "countycd", 0.0, 0.0);
        org.msse.demo.mockdata.customer.address.Address bAddr3 =
                new org.msse.demo.mockdata.customer.address.Address("B-addr3", "B-cust-3", "formatCode", "sType",
                        "333 Sunny Rd", "line2", "Austin", "TX", "zip", "zip5", "countycd", 0.0, 0.0);
        org.msse.demo.mockdata.customer.address.Address bAddr4 =
                new org.msse.demo.mockdata.customer.address.Address("B-addr4", "B-cust-4", "formatCode", "sType",
                        "444 Bright Blvd", "line2", "San Francisco", "CA", "zip", "zip5", "countycd", 0.0, 0.0);

        addressInput.pipeInput(bAddr1.id(), bAddr1);
        addressInput.pipeInput(bAddr2.id(), bAddr2);
        addressInput.pipeInput(bAddr3.id(), bAddr3);
        addressInput.pipeInput(bAddr4.id(), bAddr4);

        // artistA
        // 3 streams from CA, 2 streams from NY
        streamInput.pipeInput("stream-A1", DataFaker.STREAMS.generate("A-cust-1", artistA.id())); // CA
        streamInput.pipeInput("stream-A2", DataFaker.STREAMS.generate("A-cust-2", artistA.id())); // CA
        streamInput.pipeInput("stream-A3", DataFaker.STREAMS.generate("A-cust-3", artistA.id())); // CA
        streamInput.pipeInput("stream-A4", DataFaker.STREAMS.generate("A-cust-4", artistA.id())); // NY
        streamInput.pipeInput("stream-A5", DataFaker.STREAMS.generate("A-cust-5", artistA.id())); // NY

        // artistB:
        // 2 streams from TX, 1 stream from CA
        streamInput.pipeInput("stream-B1", DataFaker.STREAMS.generate("B-cust-1", artistB.id())); // TX
        streamInput.pipeInput("stream-B2", DataFaker.STREAMS.generate("B-cust-2", artistB.id())); // TX
        streamInput.pipeInput("stream-B3", DataFaker.STREAMS.generate("B-cust-4", artistB.id())); // CA

        // read output records
        List<TestRecord<String, TopStreamStatePerArtistResult>> outputRecords = outputTopic.readRecordsToList();
        assertTrue(outputRecords.size() >= 2, "Expected at least 2 output records");

        // DEBUG
//        System.out.println("All output records:");
//        for (TestRecord<String, TopStreamStatePerArtistResult> record : outputRecords) {
//            System.out.println("Key: " + record.key() + ", Value: " + record.value());
//        }

        // artistA - expected CA with 3 streams
        Optional<TopStreamStatePerArtistResult> lastResultForArtistA = outputRecords.stream()
                .filter(r -> r.value().getArtistId().equals(artistA.id()))
                .map(TestRecord::value)
                .reduce((first, second) -> second);

        assertTrue(lastResultForArtistA.isPresent());
        TopStreamStatePerArtistResult resultA = lastResultForArtistA.get();
        assertEquals(artistA.id(), resultA.getArtistId());
        assertEquals(artistA.name(), resultA.getArtistName());
        assertEquals("CA", resultA.getState());
        assertEquals(Long.valueOf(3), resultA.getStreamCount());

        // artistB - expected TX with 2 streams
        Optional<TopStreamStatePerArtistResult> lastResultForArtistB = outputRecords.stream()
                .filter(r -> r.value().getArtistId().equals(artistB.id()))
                .map(TestRecord::value)
                .reduce((first, second) -> second);

        assertTrue(lastResultForArtistB.isPresent());
        TopStreamStatePerArtistResult resultB = lastResultForArtistB.get();
        assertEquals(artistB.id(), resultB.getArtistId());
        assertEquals(artistB.name(), resultB.getArtistName());
        assertEquals("TX", resultB.getState());
        assertEquals(Long.valueOf(2), resultB.getStreamCount());
    }
}
