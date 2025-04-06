package org.improving.workshop.qs_3;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.improving.workshop.samples.PurchaseEventTicket;
import org.improving.workshop.samples.TopCustomerArtists;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.music.venue.Venue;

public class TopOutsideState {
    //private final static JsonSerde<TicketsByEventAndVenue> ticketsByEventAndVenueSerde = new JsonSerde<TicketsByEventAndVenue>();
    public static final JsonSerde<TicketsByEventAndVenue> ticketsByEventAndVenueSerde = new JsonSerde<>(TicketsByEventAndVenue.class);
    public static void main(final String[] args){
        final StreamsBuilder builder = new StreamsBuilder();
        //configureToplogy(builder);
        startStreams(builder);
    }

    static void configureToplogy(final StreamsBuilder builder){
        KTable<String, Event> eventsTable = builder
                .table(TOPIC_DATA_DEMO_EVENTS,
                        Materialized
                                .<String,Event>as(persistentKeyValueStore("events"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_EVENT_JSON)
                );
        KTable<String, Venue> venueKTable = builder
                .table(TOPIC_DATA_DEMO_VENUES,
                        Materialized
                                .<String,Venue>as(persistentKeyValueStore("venues"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_VENUE_JSON)
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
                .stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                .selectKey((ticketId, ticketRequest) -> ticketRequest.eventid(), Named.as("rekey-by-eventid"))
                .join(eventsTable,
                        (eventId, ticket, event) -> new EventTicket(ticket, event)
                )
                .selectKey((eventId, eventTicket) -> eventTicket.event.venueid(), Named.as("rekey_by_venueid"))
                .join(venueKTable,
                        (venueId, eventTicket, venue)-> new EventTicketVenue(eventTicket,venue))
                .selectKey((venueId,eventTicketVenue) -> eventTicketVenue.venue.addressid(), Named.as("rekey_by_venueAddressId"))
                .join(addressKTable,
                        (addressId, eventTicketVenue, address) -> new EventTicketVenueAddress(eventTicketVenue, address))
                .selectKey((venueAddressId,eventTicketVenueAddress) -> eventTicketVenueAddress.eventTicketVenue.eventTicket.ticket.customerid(), Named.as("rekey_by_customerid"))
                .join(rekeyedAddressByCustomer,
                        (customerid,eventTicketVenueAddress,customerAddress) -> new EventTicketVenueAddressCustAddress(eventTicketVenueAddress,customerAddress))
                .selectKey((customerid, eventTicketVenueAddressCustAddress) -> eventTicketVenueAddressCustAddress.eventTicketVenueAddress.eventTicketVenue.venue.id(), Named.as("rekey_by_venueid"))
                .filter((venueid,eventTicketVenueAddressCustAddress) -> !(eventTicketVenueAddressCustAddress.customerAddress.state().equals(eventTicketVenueAddressCustAddress.eventTicketVenueAddress.venueAddress.state())))
                .selectKey((customerid, eventTicketVenueAddressCustAddress) -> eventTicketVenueAddressCustAddress.eventTicketVenueAddress.eventTicketVenue.venue.id().concat(eventTicketVenueAddressCustAddress.eventTicketVenueAddress.eventTicketVenue.eventTicket.event.id()), Named.as("rekey_by_eventvenueid"))
                .groupByKey()
                .aggregate(
                        TicketsByEventAndVenue::new,

                        (eventVenueId, stream, ticketsByEventAndVenue) -> {
                            ticketsByEventAndVenue.increamentCount();
                            ticketsByEventAndVenue.eventId = stream.eventTicketVenueAddress.eventTicketVenue.eventTicket.event.id();
                            ticketsByEventAndVenue.venueId = stream.eventTicketVenueAddress.eventTicketVenue.venue.id();
                            ticketsByEventAndVenue.venueName = stream.eventTicketVenueAddress.eventTicketVenue.venue.name();
                            return ticketsByEventAndVenue;
                        },

                        // ktable (materialized) configuration
                        Materialized
                                .<String, TicketsByEventAndVenue>as(persistentKeyValueStore("ticket_count_by_venue_event"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(TopOutsideState.ticketsByEventAndVenueSerde)

                )


                .


    }

    @Data
    @AllArgsConstructor
    public static class EventTicket {
        private Ticket ticket;
        private Event event;
    }

    @Data
    @AllArgsConstructor
    public static class EventTicketVenue {
        private EventTicket eventTicket;
        private Venue venue;
    }

    @Data
    @AllArgsConstructor
    public static class EventTicketVenueAddress {
        private EventTicketVenue eventTicketVenue;
        private Address venueAddress;
    }

    @Data
    @AllArgsConstructor
    public static class EventTicketVenueAddressCustAddress {
        private EventTicketVenueAddress eventTicketVenueAddress;
        private Address customerAddress;
    }
    @Data
    @AllArgsConstructor
    public static class TicketsByEventAndVenue {
        private String eventId;
        private String venueId;
        private String venueName;
        private int outOfStateTicketCount;

        public void increamentCount(){
            this.outOfStateTicketCount += 1;
        }
    }
}
