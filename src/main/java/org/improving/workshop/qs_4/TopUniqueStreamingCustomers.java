package org.improving.workshop.qs_4;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.improving.workshop.qs_3.TopOutsideState;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.stream.Stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.qs_3.TopOutsideStateResult;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.venue.Venue;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class TopUniqueStreamingCustomers {
    public static final JsonSerde<TopUniqueStreamingCustomersResult> topUniqueStreamingCustomersResultSerde = new JsonSerde<>(TopUniqueStreamingCustomersResult.class);


    public static void main(final String[] args){
        final StreamsBuilder builder = new StreamsBuilder();
        configureTopology(builder);
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder){
        KTable<String, Artist> artistsTable = builder
                .table(TOPIC_DATA_DEMO_ARTISTS,
                        Materialized
                                .<String,Artist>as(persistentKeyValueStore("artists"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ARTIST_JSON)
                );
        KTable<String, Address> addressKTable = builder
                .table(TOPIC_DATA_DEMO_ADDRESSES,
                        Materialized
                                .<String,Address>as(persistentKeyValueStore("address"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ADDRESS_JSON)
                );
        KTable<String,Address> rekeyedAddressByCustomer = addressKTable
                .toStream()
                .selectKey((addressId, address) -> address.customerid(), Named.as("rekey-by-customerid"))
                .toTable();

        builder
                .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
                .selectKey((streamId, stream) -> stream.artistid(), Named.as("rekey-by-artistId"))
                .join(artistsTable,
                        (artistId, stream, artist) -> new TopUniqueStreamingCustomersResult(
                                stream.id(),
                                stream.customerid(),
                                stream.artistid(),
                                null,
                                artist.name()),Joined.with(Serdes.String(),SERDE_STREAM_JSON,SERDE_ARTIST_JSON)
                )
                .selectKey((artistId, topUniqueStreamingCustomersResult) -> topUniqueStreamingCustomersResult.getCustomerId(), Named.as("rekey-by-stream-customerid"))
                .join(rekeyedAddressByCustomer,
                        (customerId, topUniqueStreamingCustomersResult, address) -> new TopUniqueStreamingCustomersResult(
                                topUniqueStreamingCustomersResult.getStreamId(),
                                topUniqueStreamingCustomersResult.getCustomerId(),
                                topUniqueStreamingCustomersResult.getArtistId(),
                                address.state(),
                                topUniqueStreamingCustomersResult.getArtistName()),Joined.with(Serdes.String(),topUniqueStreamingCustomersResultSerde,SERDE_ADDRESS_JSON)
                )
                .to(TopUniqueStreamingCustomersResult.OUTPUT_TOPIC, Produced.with(Serdes.String(),topUniqueStreamingCustomersResultSerde));

    }
}
