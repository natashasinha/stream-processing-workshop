package org.improving.workshop.qs_2;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.kafka.support.serializer.JsonSerde;

import static org.improving.workshop.Streams.*;

@Slf4j
public class TopStreamStatePerArtist {

    public static final String OUTPUT_TOPIC = "kafka-workshop-top-stream-state";

    // serdes for custom types
    public static final JsonSerde<EnrichedStream> SERDE_ENRICHED_STREAM = new JsonSerde<>(EnrichedStream.class);
    public static final JsonSerde<CountByState> SERDE_COUNT_BY_STATE = new JsonSerde<>(CountByState.class);
    public static final JsonSerde<TopStreamStatePerArtistResult> SERDE_TOP_STREAM_STATE_PER_ARTIST_RESULT_JSON = new JsonSerde<>(TopStreamStatePerArtistResult.class);

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();
        configureTopology(builder);
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        // read stream events from the STREAM topic
        KStream<String, org.msse.demo.mockdata.music.stream.Stream> streamEvents = builder.stream(
                TOPIC_DATA_DEMO_STREAMS,
                Consumed.with(Serdes.String(), SERDE_STREAM_JSON)
        ).peek((key, value) -> log.info("Stream Received: {}", value));

        // consume the addresses topic
        KTable<String, org.msse.demo.mockdata.customer.address.Address> rawAddressTable = builder.table(
                TOPIC_DATA_DEMO_ADDRESSES,
                Consumed.with(Serdes.String(), SERDE_ADDRESS_JSON)
        );
        // re-key the address table so that the key becomes the customer id
        KTable<String, org.msse.demo.mockdata.customer.address.Address> addressTable = rawAddressTable
                .toStream()
                .selectKey((addressId, address) -> address.customerid())
                .peek((customerId, address) -> log.info("Address Table - Key: {}, Address: {}", customerId, address))
                .toTable();

        // enrich the stream events with customer state via a join on customer id
        KStream<String, EnrichedStream> enrichedStreams = streamEvents.selectKey(
                (key, stream) -> stream.customerid()
        ).join(
                addressTable,
                (stream, address) -> new EnrichedStream(stream, address.state())
        ).peek((key, enriched) -> log.info("EnrichedStream: {}", enriched));

        // re-key the stream with composite key - artistId|state
        KStream<String, EnrichedStream> keyedByArtistState = enrichedStreams.selectKey(
                (key, enriched) -> enriched.getArtistId() + "|" + enriched.getState()
        ).peek((compositeKey, enriched) -> log.info("Composite Key: {}, Stream: {}", compositeKey, enriched));

        // group by composite key and count streams per artist per state
        KTable<String, Long> countPerArtistState = keyedByArtistState
                .groupByKey(Grouped.with(Serdes.String(), SERDE_ENRICHED_STREAM))
                .aggregate(
                        () -> 0L,
                        (key, value, aggregate) -> aggregate + 1L,  // adder
                        Materialized.with(Serdes.String(), Serdes.Long())
                );

        countPerArtistState.toStream()
                .peek((compositeKey, count) -> log.info("Count for {}: {}", compositeKey, count));

        // convert the composite key counts to a stream keyed by artist id, with value as CountByState
        KStream<String, CountByState> stateCounts = countPerArtistState.toStream().map((compositeKey, count) -> {
            String[] parts = compositeKey.split("\\|");
            String artistId = parts[0];
            String state = parts[1];
            CountByState cs = new CountByState(state, count);
            log.info("Mapped CountByState: artistId: {}, {}", artistId, cs);
            return KeyValue.pair(artistId, cs);
        });

        // group by artist id and reduce to select the state with the highest stream count per artist
        KTable<String, CountByState> topStatePerArtist = stateCounts
                .groupByKey(Grouped.with(Serdes.String(), SERDE_COUNT_BY_STATE))
                .aggregate(
                        () -> new CountByState("", 0L),
                        // aggregator (keep the one with highest count)
                        (artistId, newVal, agg) -> {
                            log.info("Comparing counts for artist {}: new state {} count {} vs current state {} count {}",
                                    artistId, newVal.getState(), newVal.getCount(), agg.getState(), agg.getCount());
                            return newVal.getCount() > agg.getCount() ? newVal : agg;
                        },
                        Materialized.with(Serdes.String(), SERDE_COUNT_BY_STATE)
                );

        // join with artist table to enrich with the artist name
        KTable<String, org.msse.demo.mockdata.music.artist.Artist> artistTable = builder.table(
                TOPIC_DATA_DEMO_ARTISTS,
                Consumed.with(Serdes.String(), SERDE_ARTIST_JSON)
        );

        KTable<String, TopStreamStatePerArtistResult> finalResult = topStatePerArtist.join(
                artistTable,
                (countByState, artist) -> {
                    TopStreamStatePerArtistResult result = new TopStreamStatePerArtistResult(
                            artist.id(),
                            artist.name(),
                            countByState.getState(),
                            countByState.getCount()
                    );
                    log.info("Final result: artistId: {}, artistName: {}, state: {}, count: {}",
                            result.getArtistId(), result.getArtistName(), result.getState(), result.getStreamCount());
                    return result;
                }
        );

        finalResult.toStream()
                .selectKey((artistId, result) -> artistId + "_" + System.currentTimeMillis())
                .to(
                        OUTPUT_TOPIC,
                        Produced.with(Serdes.String(), SERDE_TOP_STREAM_STATE_PER_ARTIST_RESULT_JSON)
                );
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EnrichedStream {
        private org.msse.demo.mockdata.music.stream.Stream stream;
        private String state;

        public String getArtistId() {
            return stream.artistid();
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CountByState {
        private String state;
        private Long count;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TopStreamStatePerArtistResult {
        private String artistId;
        private String artistName;
        private String state;
        private Long streamCount;
    }
}
