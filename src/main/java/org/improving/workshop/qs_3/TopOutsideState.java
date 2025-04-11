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
    public static final JsonSerde<EventTicket> eventTicketSerde = new JsonSerde<>(EventTicket.class);
    public static final JsonSerde<TicketsByEventAndVenue> ticketsByEventAndVenueSerde = new JsonSerde<>(TicketsByEventAndVenue.class);
    public static final JsonSerde<RollingTicketCountByVenue> rollingTicketCountByVenueSerde = new JsonSerde<>(RollingTicketCountByVenue.class);
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "top-out-of-state-tickets-per-venue";

    public static final JsonSerde<SortedCounterMap> COUNTER_MAP_JSON_SERDE = new JsonSerde<>(SortedCounterMap.class);
    public static final JsonSerde<TopOutsideStateResult> topOutsideStateResultSerde = new JsonSerde<>(TopOutsideStateResult.class);
    public static void main(final String[] args){
        final StreamsBuilder builder = new StreamsBuilder();
        configureTopology(builder);
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder){
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
                .selectKey((customerid, eventTicketVenueAddressCustAddress) -> eventTicketVenueAddressCustAddress.eventTicketVenueAddress.eventTicketVenue.venue.id(), Named.as("rekey_by_venueid2"))
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
                                .<String, TicketsByEventAndVenue>as(persistentKeyValueStore("tickets-by-event-and-venue"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(ticketsByEventAndVenueSerde)
                )
                .toStream()
                .groupByKey()
                .aggregate(
                        RollingTicketCountByVenue::new,
                        (eventVenueId, stream, rollingTicketCountByVenue) -> {
                            rollingTicketCountByVenue.venueName = stream.venueName;
                            rollingTicketCountByVenue.calculateRollingAvg(stream.outOfStateTicketCount);
                            return rollingTicketCountByVenue;
                        },
                        // ktable (materialized) configuration
                        Materialized
                                .<String, RollingTicketCountByVenue>as(persistentKeyValueStore("rolling-tickets-by-venue"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(rollingTicketCountByVenueSerde)
                ).toStream()
                .groupByKey()
                .aggregate(
                        // initializer
                        SortedCounterMap::new,

                        // aggregator
                        (eventIdVenueId, stream, sortedCounterMap) -> {
                            sortedCounterMap.updateTicketsCount(stream);
                            return sortedCounterMap;
                        }
                )

                // turn it back into a stream so that it can be produced to the OUTPUT_TOPIC
                .toStream()
                // trim to only the top 3
                .mapValues(sortedCounterMap -> sortedCounterMap.top())
                // NOTE: when using ccloud, the topic must exist or 'auto.create.topics.enable' set to true (dedicated cluster required)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(),topOutsideStateResultSerde));
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
        public TicketsByEventAndVenue() {
            outOfStateTicketCount=0;
        }
        public void increamentCount(){
            this.outOfStateTicketCount += 1;
        }
    }
    @Data
    @AllArgsConstructor
    public static class RollingTicketCountByVenue{
        private String venueName;
        private int totalOutOfStateTicketsPerVenue;
        private int totalEventsForVenue;
        private double rollingAvg;
        public RollingTicketCountByVenue() {
            totalOutOfStateTicketsPerVenue=0;
            totalEventsForVenue=0;
        }
        public void calculateRollingAvg(int ticketsPerEvent){
            this.totalOutOfStateTicketsPerVenue += ticketsPerEvent;
            this.totalEventsForVenue++;
            this.rollingAvg = this.totalOutOfStateTicketsPerVenue/this.totalEventsForVenue;
        }

    }
    @Data
    @AllArgsConstructor
    public static class SortedCounterMap {
        private int maxSize;

        private LinkedHashMap<String, Double> ticketsMap;

        public SortedCounterMap() {
            this(1000);
        }

        public SortedCounterMap(int maxSize) {
            this.maxSize = maxSize;
            this.ticketsMap = new LinkedHashMap<>();
        }

        public void updateTicketsCount(RollingTicketCountByVenue rollingTicketCountByVenue) {
            ticketsMap.compute(rollingTicketCountByVenue.venueName, (k, v) -> rollingTicketCountByVenue.rollingAvg);
            // replace with sorted map
            this.ticketsMap = ticketsMap.entrySet().stream()
                    .sorted(reverseOrder(Map.Entry.comparingByValue()))
                    // keep a limit on the map size
                    .limit(maxSize)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }

        public TopOutsideStateResult top() {
            LinkedHashMap<String, Double> topOutOfStateVenue= ticketsMap.entrySet().stream()
                    .limit(1)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
            TopOutsideStateResult result = new TopOutsideStateResult();
            Map.Entry<String,Double> entry = topOutOfStateVenue.entrySet().iterator().next();

            result.setVenueName(entry.getKey());
            result.setAvgOutOfStateAttendeesPerEvent(entry.getValue());
            return result;
        }
    }
}
