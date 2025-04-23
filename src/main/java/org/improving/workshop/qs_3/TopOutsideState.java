package org.improving.workshop.qs_3;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.improving.workshop.samples.PurchaseEventTicket;
import org.improving.workshop.samples.TopCustomerArtists;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.music.venue.Venue;

@Slf4j
public class TopOutsideState {
    //private final static JsonSerde<TicketsByEventAndVenue> ticketsByEventAndVenueSerde = new JsonSerde<TicketsByEventAndVenue>();
    public static final JsonSerde<EventTicket> eventTicketSerde = new JsonSerde<>(EventTicket.class);
    public static final JsonSerde<EventTicketVenue> eventTicketVenueSerde = new JsonSerde<>(EventTicketVenue.class);
    public static final JsonSerde<EventTicketVenueAddress> eventTicketVenueAddressSerde = new JsonSerde<>(EventTicketVenueAddress.class);
    public static final JsonSerde<EventTicketVenueAddressCustAddress> eventTicketVenueAddressCustAddressSerde = new JsonSerde<>(EventTicketVenueAddressCustAddress.class);
    public static final JsonSerde<TicketsByEventAndVenue> ticketsByEventAndVenueSerde = new JsonSerde<>(TicketsByEventAndVenue.class);
    public static final JsonSerde<RollingTicketCountByVenue> rollingTicketCountByVenueSerde = new JsonSerde<>(RollingTicketCountByVenue.class);
    public static final JsonSerde<TicketsByEventAndVenueAndEventCountByVenue> ticketsByEventAndVenueAndEventCountByVenueSerde = new JsonSerde<>(TicketsByEventAndVenueAndEventCountByVenue.class);
    // MUST BE PREFIXED WITH "kafka-workshop-"
    //public static final String OUTPUT_TOPIC = "top-out-of-state-tickets-per-venue";

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
                        (eventId, ticket, event) -> new EventTicket(ticket, event),Joined.with(Serdes.String(),SERDE_TICKET_JSON,SERDE_EVENT_JSON)
                )
                //.peek((eventId, eventStatus) -> log.info("joined ticket with event. '{}'>'{}'", eventId,eventStatus.event.venueid()))
                .selectKey((eventId, eventTicket) -> eventTicket.event.venueid(), Named.as("rekey_by_venueid"))

                .join(venueKTable,
                        (venueId, eventTicket, venue)-> new EventTicketVenue(eventTicket,venue),Joined.with(Serdes.String(),eventTicketSerde,SERDE_VENUE_JSON))
                //.peek((venueId, eventTicketVenue) -> log.info("joined event-ticket with venue. '{}'>'{}'>'{}'", venueId,eventTicketVenue.eventTicket.event.id(),eventTicketVenue.eventTicket.ticket.id()))
                .selectKey((venueId,eventTicketVenue) -> eventTicketVenue.venue.addressid(), Named.as("rekey_by_venueAddressId"))

                .join(addressKTable,
                        (addressId, eventTicketVenue, address) -> new EventTicketVenueAddress(eventTicketVenue, address),Joined.with(Serdes.String(),eventTicketVenueSerde,SERDE_ADDRESS_JSON))
                .selectKey((venueAddressId,eventTicketVenueAddress) -> eventTicketVenueAddress.eventTicketVenue.eventTicket.ticket.customerid(), Named.as("rekey_by_customerid"))

                .join(rekeyedAddressByCustomer,
                        (customerid,eventTicketVenueAddress,customerAddress) -> new EventTicketVenueAddressCustAddress(eventTicketVenueAddress,customerAddress),
                        Joined.with(Serdes.String(),eventTicketVenueAddressSerde,SERDE_ADDRESS_JSON))
                .selectKey((customerid, eventTicketVenueAddressCustAddress) -> eventTicketVenueAddressCustAddress.eventTicketVenueAddress.eventTicketVenue.venue.id(), Named.as("rekey_by_venueid2"))

                .groupByKey(Grouped.with(Serdes.String(),eventTicketVenueAddressCustAddressSerde))
                .aggregate(
                        TicketsByEventAndVenueAndEventCountByVenue::new,

                        (eventVenueId, stream, ticketsByEventAndVenueAndEventCountByVenue) -> {
                            ticketsByEventAndVenueAndEventCountByVenue.ticketId = stream.eventTicketVenueAddress.eventTicketVenue.eventTicket.ticket.id();
                            ticketsByEventAndVenueAndEventCountByVenue.eventId = stream.eventTicketVenueAddress.eventTicketVenue.eventTicket.event.id();
                            ticketsByEventAndVenueAndEventCountByVenue.venueId = stream.eventTicketVenueAddress.eventTicketVenue.venue.id();
                            ticketsByEventAndVenueAndEventCountByVenue.venueName = stream.eventTicketVenueAddress.eventTicketVenue.venue.name();
                            ticketsByEventAndVenueAndEventCountByVenue.customerAddress = stream.customerAddress;
                            ticketsByEventAndVenueAndEventCountByVenue.venueAddress = stream.eventTicketVenueAddress.venueAddress;
                            ticketsByEventAndVenueAndEventCountByVenue.calculateRollingEventCount(stream.eventTicketVenueAddress.eventTicketVenue.venue.id(),stream.eventTicketVenueAddress.eventTicketVenue.eventTicket.event.id());
                            return ticketsByEventAndVenueAndEventCountByVenue;
                        },
                        // ktable (materialized) configuration
                        Materialized
                                .<String, TicketsByEventAndVenueAndEventCountByVenue>as(persistentKeyValueStore("tickets-and-events_by-venue-event-count-by-venue"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(ticketsByEventAndVenueAndEventCountByVenueSerde)
                )
                .toStream()
                .selectKey((ticketId,ticketsByEventAndVenueAndEventCountByVenue) -> ticketsByEventAndVenueAndEventCountByVenue.venueId, Named.as("rekey_by_venueid3"))

                //.filter((venueid,ticketsByEventAndVenueAndEventCountByVenue) -> !(ticketsByEventAndVenueAndEventCountByVenue.customerAddress.state().equals(ticketsByEventAndVenueAndEventCountByVenue.venueAddress.state())))
                //.selectKey((venueid, ticketsByEventAndVenueAndEventCountByVenue) -> ticketsByEventAndVenueAndEventCountByVenue.venueId.concat(ticketsByEventAndVenueAndEventCountByVenue.eventId), Named.as("rekey_by_eventvenueid"))
                //.peek((venueid, eventTicketVenueAddressCustAddress) -> log.info("selectKey. '{}'>'{}'", venueid,eventTicketVenueAddressCustAddress.eventTicketVenueAddress.eventTicketVenue.eventTicket.ticket.id()))


                .groupByKey(Grouped.with(Serdes.String(),ticketsByEventAndVenueAndEventCountByVenueSerde))

                .aggregate(
                        TicketsByEventAndVenue::new,

                        (eventVenueId, stream, ticketsByEventAndVenue) -> {
                            ticketsByEventAndVenue.increamentCount();
                            ticketsByEventAndVenue.eventId = stream.eventId;
                            ticketsByEventAndVenue.venueId = stream.venueId;
                            ticketsByEventAndVenue.venueName = stream.venueName;
                            ticketsByEventAndVenue.totalEventsForVenue = stream.totalEventsForVenue;
                            ticketsByEventAndVenue.rollingAvg = stream.rollingAvg;
                            return ticketsByEventAndVenue;
                        },
                        // ktable (materialized) configuration
                        Materialized
                                .<String, TicketsByEventAndVenue>as(persistentKeyValueStore("tickets-by-event-and-venue"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(ticketsByEventAndVenueSerde)
                )
                .toStream()
                .selectKey((eventVenueId,ticketsByEventAndVenue) -> ticketsByEventAndVenue.venueId, Named.as("rekey_by_venueId_4"))
                //.peek((eventVenueId, ticketsByEventAndVenue) -> log.info("after aggregate TicketsByEventAndVenue. '{}'>'{}'", eventVenueId,ticketsByEventAndVenue.outOfStateTicketCount))

                .groupByKey(Grouped.with(Serdes.String(),ticketsByEventAndVenueSerde))
                .aggregate(
                        RollingTicketCountByVenue::new,
                        (eventVenueId, stream, rollingTicketCountByVenue) -> {
                            rollingTicketCountByVenue.venueName = stream.venueName;
                            rollingTicketCountByVenue.totalOutOfStateTicketsPerVenue = stream.getOutOfStateTicketCount();
                            rollingTicketCountByVenue.rollingAvg=stream.rollingAvg;
                            //rollingTicketCountByVenue.calculateRollingAvg(stream.outOfStateTicketCount,stream.venueId,stream.getEventId(),stream.totalEventsForVenue);
                            return rollingTicketCountByVenue;
                        },
                        // ktable (materialized) configuration
                        Materialized
                                .<String, RollingTicketCountByVenue>as(persistentKeyValueStore("rolling-tickets-by-venue"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(rollingTicketCountByVenueSerde)
                )
                .toStream()
                //.peek((eventVenueId, rollingTicketCountByVenue) -> log.info("after aggregate rollingTicketCountByVenue '{}'-'{}'", eventVenueId,rollingTicketCountByVenue.rollingAvg))
                .selectKey((k,v) -> "Global")

                .groupByKey(Grouped.with(Serdes.String(),rollingTicketCountByVenueSerde))
                .aggregate(
                        // initializer
                        SortedCounterMap::new,

                        // aggregator
                        (eventIdVenueId, stream, sortedCounterMap) -> {
                            sortedCounterMap.updateTicketsCount(stream);
                            return sortedCounterMap;
                        },
                        Materialized
                                .<String, SortedCounterMap>as(persistentKeyValueStore("counter-map-json"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(COUNTER_MAP_JSON_SERDE)
                )

                // turn it back into a stream so that it can be produced to the OUTPUT_TOPIC
                .toStream()
                //.peek((eventVenueId, sortedCounterMap) -> log.info("after aggregate sortedCounterMap '{}'-'{}'", eventVenueId,sortedCounterMap.top().getVenueName()))
                .mapValues(sortedCounterMap -> {
                    //log.info(sortedCounterMap.toString());
                    //log.info("EOF");
                    return sortedCounterMap.top();
                })
                //.peek((eventVenueId, topOutsideStateResult) -> log.info("after aggregate topOutsideStateResult '{}'>'{}'>'{}'", eventVenueId,topOutsideStateResult.getVenueName(),topOutsideStateResult.getAvgOutOfStateAttendeesPerEvent()))
                .to(TopOutsideStateResult.OUTPUT_TOPIC, Produced.with(Serdes.String(),topOutsideStateResultSerde));

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EventTicket {
        private Ticket ticket;
        private Event event;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EventTicketVenue {
        private EventTicket eventTicket;
        private Venue venue;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EventTicketVenueAddress {
        private EventTicketVenue eventTicketVenue;
        private Address venueAddress;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EventTicketVenueAddressCustAddress {
        private EventTicketVenueAddress eventTicketVenueAddress;
        private Address customerAddress;
    }
    @Data
    @AllArgsConstructor
    public static class TicketsByEventAndVenue {
        public double rollingAvg;
        private String eventId;
        private String venueId;
        private String venueName;
        private int outOfStateTicketCount;
        private double totalEventsForVenue;
        public TicketsByEventAndVenue() {
            outOfStateTicketCount=0;
        }
        public void increamentCount(){
            this.outOfStateTicketCount += 1;
        }
    }
    @Data
    @AllArgsConstructor
    public static class TicketsByEventAndVenueAndEventCountByVenue{
        private String venueName;
        private Address customerAddress;
        private Address venueAddress;
        private String ticketId;
        private String eventId;
        private String venueId;
        private double totalOutOfStateTicketsPerVenue=0;
        private double totalEventsForVenue=0;
        private double rollingAvg;
        private List<String> venueIDs = new ArrayList<>();
        private List<String> eventIDs = new ArrayList<>();
        public TicketsByEventAndVenueAndEventCountByVenue() {
            //log.info("TicketsByEventAndVenueAndEventCountByVenue constructor");
        }
        public void calculateRollingEventCount(String venueId, String eventId){
            if (this.eventIDs.indexOf(eventId)<0) {
                this.eventIDs.add(eventId);
                this.totalEventsForVenue++;
            }
            if (this.venueIDs.indexOf(venueId)<0) {
                this.venueIDs.add(venueId);
                if(!customerAddress.state().equals(venueAddress.state()))
                    this.totalOutOfStateTicketsPerVenue++;
            }
            else
            {
                if(!customerAddress.state().equals(venueAddress.state()))
                    this.totalOutOfStateTicketsPerVenue++;
            }
            this.rollingAvg = this.totalOutOfStateTicketsPerVenue/this.totalEventsForVenue;
            log.info("calculateRollingEventCount '{}'>'{}'>'{}'>'{}'>'{}'", this.totalOutOfStateTicketsPerVenue,totalEventsForVenue,this.venueName,eventId,this.rollingAvg);
            //log.info("calculateRollingEventCount '{}'>'{}'>'{}'>'{}'", this.ticketId,this.totalEventsForVenue,this.venueName,eventId);
        }
    }
    @Data
    @AllArgsConstructor
    public static class RollingTicketCountByVenue{
        private String venueName;
        private double totalOutOfStateTicketsPerVenue=0;
        private double totalTicketsForVenue=0;
        private double totalEventsForVenue=0;
        private double rollingAvg;

        private List<String> venueIDs = new ArrayList<>();
        private List<String> eventIDs = new ArrayList<>();
        public RollingTicketCountByVenue() {
            //log.info("RollingTicketCountByVenue constructor");
        }
        /*
        public void calculateRollingAvg(int ticketsPerEvent, String venueId, String eventId, double eventCountForVenue){

            this.totalOutOfStateTicketsPerVenue = ticketsPerEvent;

            if (this.venueIDs.indexOf(venueId)<0) {
                this.venueIDs.add(venueId);
                this.totalTicketsForVenue++;
            }
            else
            {
                this.totalTicketsForVenue++;
            }

            if (this.eventIDs.indexOf(eventId)<0) {
                this.eventIDs.add(eventId);
                this.totalEventsForVenue++;
            }

            this.rollingAvg = this.totalTicketsForVenue/this.totalOutOfStateTicketsPerVenue;
            log.info("calculateRollingAvg '{}'>'{}'>'{}'>'{}'>'{}'", this.totalTicketsForVenue,eventCountForVenue,this.venueName,eventId,this.rollingAvg);
        }*/

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
