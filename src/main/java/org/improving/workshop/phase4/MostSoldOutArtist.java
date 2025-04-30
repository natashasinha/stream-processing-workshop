package org.improving.workshop.phase4;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;

import static org.improving.workshop.Streams.*;

@Slf4j
public class MostSoldOutArtist {
    public static final String OUTPUT_TOPIC = "data-demo-enrichedeventsales";

    // serdes for custom types
    public static final JsonSerde<EnrichedEventSales> SERDE_ENRICHED_EVENT_SALES = new JsonSerde<>(EnrichedEventSales.class);
    public static final JsonSerde<SoldOutCount> SERDE_SOLD_OUT_COUNT = new JsonSerde<>(SoldOutCount.class);
    public static final JsonSerde<MostSoldOutArtistResult> SERDE_RESULT = new JsonSerde<>(MostSoldOutArtistResult.class);

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // start the Kafka Streams application
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        KStream<String, Ticket> ticketStream = builder.stream(
                TOPIC_DATA_DEMO_TICKETS,
                Consumed.with(Serdes.String(), SERDE_TICKET_JSON)
        ).peek((key, value) -> log.info("Ticket Received: {}", value));

        KTable<String, Event> eventTable = builder.table(
                TOPIC_DATA_DEMO_EVENTS,
                Consumed.with(Serdes.String(), SERDE_EVENT_JSON)
        );

        KTable<String, Long> ticketsSoldPerEvent = ticketStream
                .selectKey((key, ticket) -> ticket.eventid())
                .groupBy((key, ticket) -> ticket.eventid(), Grouped.with(Serdes.String(), SERDE_TICKET_JSON))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("tickets-sold-per-event")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        KTable<String, EnrichedEventSales> enrichedEventSales = ticketsSoldPerEvent
                .leftJoin(eventTable,
                        (soldTickets, event) -> {
                            EnrichedEventSales result = new EnrichedEventSales(event, soldTickets);
                            log.info("Enriched Event Sales: eventId={}, soldTickets={}, capacity={}, ratio={}",
                                    event.id(), soldTickets, event.capacity(), (double) soldTickets / event.capacity());
                            return result;
                        },
                        Materialized.with(Serdes.String(), SERDE_ENRICHED_EVENT_SALES));

        KTable<String, EnrichedEventSales> soldOutEvents = enrichedEventSales
                .filter((eventId, soldTicketInfo) -> {
                    boolean isSoldOut = (double) soldTicketInfo.getSoldTickets() / soldTicketInfo.getEvent().capacity() >= 0.95000;
                    log.info("Event {} is sold out: {}", eventId, isSoldOut);
                    return isSoldOut;
                });

        KTable<String, Long> soldOutEventsPerArtist = soldOutEvents
                .groupBy(
                        (eventId, eventSales) -> {
                            String artistId = eventSales.getEvent().artistid();
                            log.info("Grouping sold-out event {} by artist {}", eventId, artistId);
                            return KeyValue.pair(artistId, eventSales);
                        },
                        Grouped.with(Serdes.String(), SERDE_ENRICHED_EVENT_SALES))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("artist-sold-out-count")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        KStream<String, SoldOutCount> artistSoldOutCounts = soldOutEventsPerArtist.toStream()
                .map((artistId, count) -> {
                    log.info("Artist {} has {} sold-out events", artistId, count);
                    return KeyValue.pair(artistId, new SoldOutCount(artistId, count));
                });

        KTable<String, SoldOutCount> globalMaxCount = artistSoldOutCounts
                .map((artistId, soldOutCount) -> KeyValue.pair("GLOBAL", soldOutCount))
                .groupByKey(Grouped.with(Serdes.String(), SERDE_SOLD_OUT_COUNT))
                .aggregate(
                        () -> null,
                        (key, newValue, agg) -> {
                            if (agg == null || newValue.getCount() > agg.getCount()) {
                                return newValue;
                            }
                            return agg;
                        },
                        Materialized
                                .<String, SoldOutCount, KeyValueStore<Bytes, byte[]>>with(Serdes.String(), SERDE_SOLD_OUT_COUNT)
                                .withCachingDisabled()
                );

        KTable<String, Artist> artistTable = builder.table(
                TOPIC_DATA_DEMO_ARTISTS,
                Consumed.with(Serdes.String(), SERDE_ARTIST_JSON)
        );

        globalMaxCount
                .toStream()
                .peek((key, value) -> log.info("globalMaxCount: {}", value))
                .map((ignoredKey, soldOut) -> {
                    MostSoldOutArtistResult result = new MostSoldOutArtistResult(
                            "GLOBAL",
                            soldOut.getArtistId(),
                            soldOut.getCount()
                    );
                    return KeyValue.pair("GLOBAL", result);
                })
                .to(
                        OUTPUT_TOPIC,
                        Produced.with(Serdes.String(), SERDE_RESULT)
                );
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EnrichedEventSales {
        private Event event;
        private long soldTickets;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SoldOutCount {
        private String artistId;
        private Long count;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MostSoldOutArtistResult {
        private String eventId;
        private String artistId;
        private Long soldOutCount;
    }
}
