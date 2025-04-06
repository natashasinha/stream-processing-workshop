package org.improving.workshop.qs_2;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.support.serializer.JsonSerde;

import static org.improving.workshop.Streams.*;

@Slf4j
public class TopStreamStatePerArtist {

    public static final String OUTPUT_TOPIC = "kafka-workshop-top-stream-state";

    // serdes for our custom types
    public static final JsonSerde<EnrichedStream> SERDE_ENRICHED_STREAM = new JsonSerde<>(EnrichedStream.class);
    public static final JsonSerde<CountByState> SERDE_COUNT_BY_STATE = new JsonSerde<>(CountByState.class);
    public static final JsonSerde<TopStreamStatePerArtistResult> SERDE_TOP_STREAM_STATE_PER_ARTIST_RESULT_JSON = new JsonSerde<>(TopStreamStatePerArtistResult.class);

    /**
     * The Streams application as a whole can be launched like any normal Java application that has a `main()` method.
     */
    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        // read stream events from the STREAM topic
        KStream<String, org.msse.demo.mockdata.music.stream.Stream> streamEvents = builder.stream(
                TOPIC_DATA_DEMO_STREAMS,
                Consumed.with(Serdes.String(), SERDE_STREAM_JSON)
        ).peek((key, value) -> log.info("Stream Received: {}", value));

        // create a table from the ADDRESSES topic (keyed by customer id)
        KTable<String, org.msse.demo.mockdata.customer.address.Address> addressTable = builder.table(
                TOPIC_DATA_DEMO_ADDRESSES,
                Consumed.with(Serdes.String(), SERDE_ADDRESS_JSON)
        );

        // enrich the stream events with customer state via a join on customer id
        KStream<String, EnrichedStream> enrichedStreams = streamEvents.selectKey(
                (key, stream) -> stream.customerid()
        ).join(
                addressTable,
                (stream, address) -> new EnrichedStream(stream, address.state())
        );

        // re-key the stream with a composite key: artistId|state
        KStream<String, EnrichedStream> keyedByArtistState = enrichedStreams.selectKey(
                (key, enriched) -> enriched.getArtistId() + "|" + enriched.getState()
        );

        // 5. Group by the composite key and count streams per artist per state
        KGroupedStream<String, EnrichedStream> groupedByArtistState = keyedByArtistState.groupByKey(
                Grouped.with(Serdes.String(), SERDE_ENRICHED_STREAM)
        );
        KTable<String, Long> countPerArtistState = groupedByArtistState.count();

        // convert the composite key counts to a stream keyed by artist id, with value as CountByState
        KStream<String, CountByState> stateCounts = countPerArtistState.toStream().map((compositeKey, count) -> {
            String[] parts = compositeKey.split("\\|");
            String artistId = parts[0];
            String state = parts[1];
            return KeyValue.pair(artistId, new CountByState(state, count));
        });

        // group by artist id and reduce to select the state with the highest stream count per artist
        KGroupedStream<String, CountByState> groupedByArtist = stateCounts.groupByKey(
                Grouped.with(Serdes.String(), SERDE_COUNT_BY_STATE)
        );
        KTable<String, CountByState> topStatePerArtist = groupedByArtist.reduce((agg, newVal) ->
                newVal.getCount() > agg.getCount() ? newVal : agg
        );

        // join with the Artist table to enrich with the artist name
        KTable<String, org.msse.demo.mockdata.music.artist.Artist> artistTable = builder.table(
                TOPIC_DATA_DEMO_ARTISTS,
                Consumed.with(Serdes.String(), SERDE_ARTIST_JSON)
        );

        KTable<String, TopStreamStatePerArtistResult> finalResult = topStatePerArtist.join(
                artistTable,
                (countByState, artist) -> new TopStreamStatePerArtistResult(
                        artist.id(),
                        artist.name(),
                        countByState.getState(),
                        countByState.getCount()
                )
        );

        // write the final result to the output topic
        finalResult.toStream().to(
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
