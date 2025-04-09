package org.improving.workshop.qs_1;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;

import static org.improving.workshop.Streams.*;

@Slf4j
public class MostSoldOutArtist {
    public static final String OUTPUT_TOPIC = "kafka-workshop-most-sold-out-artist";

    // serdes for custom types
    public static final JsonSerde<EnrichedEventSales> SERDE_ENRICHED_EVENT_SALES = new JsonSerde<>(EnrichedEventSales.class);
    public static final JsonSerde<SoldOutCount> SERDE_SOLD_OUT_COUNT = new JsonSerde<>(SoldOutCount.class);
    public static final JsonSerde<MostSoldOutArtistResult> SERDE_MOST_SOLD_OUT_ARTIST_RESULT_JSON = new JsonSerde<>(MostSoldOutArtistResult.class);

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // start the Kafka Streams application
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        // read Ticket events from the Ticket topic
        KStream<String, org.msse.demo.mockdata.music.ticket.Ticket> ticketStream = builder.stream(
                TOPIC_DATA_DEMO_TICKETS,
                Consumed.with(Serdes.String(), SERDE_TICKET_JSON)
        ).peek((key, value) -> log.info("Ticket Received: {}", value));

        // create a table from the Events topic
        KTable<String, org.msse.demo.mockdata.music.event.Event> eventTable = builder.table(
                TOPIC_DATA_DEMO_EVENTS,
                Consumed.with(Serdes.String(), SERDE_EVENT_JSON)
        );

        // aggregate ticket counts per event ID
        KTable<String, Long> ticketsSoldPerEvent = ticketStream
                .groupBy((key, ticket) -> ticket.eventid(), Grouped.with(Serdes.String(), SERDE_TICKET_JSON))
                .count(Materialized.<String, Long>as("tickets-sold-per-event")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        // join ticket sales with event details
        KTable<String, EnrichedEventSales> enrichedEventSales = ticketsSoldPerEvent
                .join(eventTable,
                        (soldTickets, event) -> new EnrichedEventSales(event, soldTickets),
                        Materialized.with(Serdes.String(), SERDE_ENRICHED_EVENT_SALES));

        // filter events where 95% or more of seats were sold
        KTable<String, EnrichedEventSales> soldOutEvents = enrichedEventSales
                .filter((eventId, data) -> data.getSoldTickets() >= 0.95 * data.getEvent().capacity());

        // aggregate the count by artist ID to calculate the number of sold-out events per artist
        KTable<String, Long> soldOutEventsPerArtist = soldOutEvents
                .groupBy((eventId, eventSales) -> eventSales.getEvent().artistid(),
                        Grouped.with(Serdes.String(), SERDE_ENRICHED_EVENT_SALES))
                .count(Materialized.<String, Long>as("artist-sold-out-count")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        // convert the artist-sold-out counts to a stream for processing
        KStream<String, SoldOutCount> artistSoldOutCounts = soldOutEventsPerArtist.toStream()
                .map((artistId, count) -> KeyValue.pair(artistId, new SoldOutCount(count)));

        // find the artist with the highest sold-out events count
        KGroupedStream<String, SoldOutCount> groupedByArtist = artistSoldOutCounts.groupByKey(
                Grouped.with(Serdes.String(), SERDE_SOLD_OUT_COUNT)
        );

        KTable<String, SoldOutCount> topSoldOutArtist = groupedByArtist.reduce((agg, newVal) ->
                newVal.getCount() > agg.getCount() ? newVal : agg
        );

        // join with the Artist table to enrich with the artist name
        KTable<String, Artist> artistTable = builder.table(
                TOPIC_DATA_DEMO_ARTISTS,
                Consumed.with(Serdes.String(), SERDE_ARTIST_JSON)
        );

        KTable<String, MostSoldOutArtistResult> finalResult = topSoldOutArtist.join(
                artistTable,
                (soldOutCount, artist) -> new MostSoldOutArtistResult(
                        artist.id(),
                        artist.name(),
                        soldOutCount.getCount()
                )
        );

        // write the final result to the output topic
        finalResult.toStream().to(
                OUTPUT_TOPIC,
                Produced.with(Serdes.String(), SERDE_MOST_SOLD_OUT_ARTIST_RESULT_JSON)
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
        private Long count;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MostSoldOutArtistResult {
        private String artistId;
        private String artistName;
        private Long soldOutCount;
    }
}