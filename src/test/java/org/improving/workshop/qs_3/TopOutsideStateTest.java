package org.improving.workshop.qs_3;


import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.improving.workshop.Streams;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.music.venue.Venue;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.springframework.kafka.support.serializer.JsonSerde;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TopOutsideStateTest {
  private final static Serializer<String> stringSerializer = Serdes.String().serializer();
  private final static Deserializer<String> stringDeserializer = Serdes.String().deserializer();
  private final static Deserializer<Long> longDeserializer = Serdes.Long().deserializer();

  private final static JsonSerde<TopOutsideStateResult> topOutOfStateSerde = new JsonSerde<>(TopOutsideStateResult.class);

  private TopologyTestDriver driver;

  private TestInputTopic<String, Event> eventInputTopic;
  private TestInputTopic<String, Address> addressInputTopic;
  private TestInputTopic<String, Venue> venuesInputTopic;
  private TestInputTopic<String, Ticket> ticketInputTopic;
  private TestOutputTopic<String, TopOutsideStateResult> outputTopic;

  @BeforeEach
  public void setup() {

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    TopOutsideState.configureTopology(streamsBuilder);

    driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

    eventInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_EVENTS,
            Serdes.String().serializer(),
            Streams.SERDE_EVENT_JSON.serializer()
    );

    addressInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_ADDRESSES,
            Serdes.String().serializer(),
            Streams.SERDE_ADDRESS_JSON.serializer()
    );

    venuesInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_VENUES,
            Serdes.String().serializer(),
            Streams.SERDE_VENUE_JSON.serializer()
    );

    ticketInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_TICKETS,
            Serdes.String().serializer(),
            Streams.SERDE_TICKET_JSON.serializer()

    );

    outputTopic = driver.createOutputTopic(
            TopOutsideStateResult.OUTPUT_TOPIC,
            Serdes.String().deserializer(),
            topOutOfStateSerde.deserializer()
    );
  }

  @AfterEach
  public void cleanup() {
    driver.close();
  }

  @Test
  @DisplayName("artist event ticket counts")
  public void testArtistEventTicketCounts() {


    Address v1_addr = new Address("v1_addr","venue-1","formatCode","sType","777 ficticious dr","ln2","Las Vegas","NV","zip","zip5","countycd",0.2221,0.5555);
    Address v2_addr = new Address("v2_addr","venue-1","formatCode","sType","888 dubious blvd","ln2","St. Paul","MN","zip","zip5","countycd",0.2222,0.5555);
    Address v3_addr = new Address("v3_addr","venue-1","formatCode","sType","999 Bogus ave","ln2","Minneapols","MN","zip","zip5","countycd",0.2223,0.5555);
    Address v4_addr = new Address("v4_addr","venue-1","formatCode","sType","1000 5th Ave","ln2","New York","NY","zip","zip5","countycd",0.2224,0.5555);
    addressInputTopic.pipeInput("v1_addr",v1_addr);
    addressInputTopic.pipeInput("v2_addr",v2_addr);
    addressInputTopic.pipeInput("v3_addr",v3_addr);
    addressInputTopic.pipeInput("v4_addr",v4_addr);

    venuesInputTopic.pipeInput("venue-1",new Venue("venue-1","v1_addr","The Sphere",100));
    venuesInputTopic.pipeInput("venue-2",new Venue("venue-2","v2_addr","Excel Energy Center",100));
    venuesInputTopic.pipeInput("venue-3",new Venue("venue-3","v3_addr","Guthrie Theater",100));
    venuesInputTopic.pipeInput("venue-4",new Venue("venue-4","v4_addr","The Met",100));

    Address c1_addr = new Address("c1_addr","customer-1","formatCode","sType","1111 happy trail","ln2","Denver","CO","zip","zip5","countycd",0.2221,0.5555);
    Address c2_addr = new Address("c2_addr","customer-2","formatCode","sType","222 Sad walk","ln2","Chicago","IL","zip","zip5","countycd",0.2222,0.5555);
    Address c3_addr = new Address("c3_addr","customer-3","formatCode","sType","333 rainy dr","ln2","New York","NY","zip","zip5","countycd",0.2223,0.5555);
    Address c4_addr = new Address("c4_addr","customer-4","formatCode","sType","444 sunny ln","ln2","Minneapolis","MN","zip","zip5","countycd",0.2224,0.5555);
    Address c5_addr = new Address("c5_addr","customer-5","formatCode","sType","555 rocky rd","ln2","Las Vegas","NV","zip","zip5","countycd",0.2224,0.5555);
    addressInputTopic.pipeInput("c1_addr",c1_addr);
    addressInputTopic.pipeInput("c2_addr",c2_addr);
    addressInputTopic.pipeInput("c3_addr",c3_addr);
    addressInputTopic.pipeInput("c4_addr",c4_addr);
    addressInputTopic.pipeInput("c5_addr",c5_addr);


    // Given an event for artist-1
    eventInputTopic.pipeInput("exciting-event-1", new Event("exciting-event-1", "artist-1", "venue-1", 10, "today"));
    eventInputTopic.pipeInput("exciting-event-2", new Event("exciting-event-2", "artist-1", "venue-1", 10, "today"));
    eventInputTopic.pipeInput("exciting-event-3", new Event("exciting-event-3", "artist-1", "venue-2", 10, "today"));
    eventInputTopic.pipeInput("exciting-event-4", new Event("exciting-event-4", "artist-1", "venue-2", 10, "today"));
    eventInputTopic.pipeInput("exciting-event-5", new Event("exciting-event-5", "artist-1", "venue-3", 10, "today"));
    eventInputTopic.pipeInput("exciting-event-6", new Event("exciting-event-6", "artist-1", "venue-3", 10, "today"));
    eventInputTopic.pipeInput("exciting-event-7", new Event("exciting-event-7", "artist-1", "venue-4", 10, "today"));
    eventInputTopic.pipeInput("exciting-event-8", new Event("exciting-event-8", "artist-1", "venue-4", 10, "today"));
    
    
    // Customer-1 tickets
    Ticket t = new Ticket("ticket-1","customer-1","exciting-event-3",100.0);

    ticketInputTopic.pipeInput("ticket-1",t);
    ticketInputTopic.pipeInput("ticket-2",new Ticket("ticket-2","customer-1","exciting-event-5",100.0));
    ticketInputTopic.pipeInput("ticket-3",new Ticket("ticket-3","customer-1","exciting-event-7",100.0));
    ticketInputTopic.pipeInput("ticket-4",new Ticket("ticket-4","customer-1","exciting-event-4",100.0));
    ticketInputTopic.pipeInput("ticket-5",new Ticket("ticket-5","customer-1","exciting-event-8",100.0));
    ticketInputTopic.pipeInput("ticket-26",new Ticket("ticket-26","customer-1","exciting-event-1",100.0));
    
    // Customer-2 tickets
    ticketInputTopic.pipeInput("ticket-6",new Ticket("ticket-6","customer-2","exciting-event-1",100.0));
    ticketInputTopic.pipeInput("ticket-7",new Ticket("ticket-7","customer-2","exciting-event-5",100.0));
    ticketInputTopic.pipeInput("ticket-8",new Ticket("ticket-8","customer-2","exciting-event-7",100.0));
    ticketInputTopic.pipeInput("ticket-9",new Ticket("ticket-9","customer-2","exciting-event-6",100.0));
    
    // Customer-3 tickets
    ticketInputTopic.pipeInput("ticket-10",new Ticket("ticket-10","customer-3","exciting-event-1",100.0));
    ticketInputTopic.pipeInput("ticket-11",new Ticket("ticket-11","customer-3","exciting-event-5",100.0));
    ticketInputTopic.pipeInput("ticket-12",new Ticket("ticket-12","customer-3","exciting-event-7",100.0));
    ticketInputTopic.pipeInput("ticket-13",new Ticket("ticket-13","customer-3","exciting-event-4",100.0));
    ticketInputTopic.pipeInput("ticket-14",new Ticket("ticket-14","customer-3","exciting-event-6",100.0));
    
    // Customer-4 tickets
    ticketInputTopic.pipeInput("ticket-15",new Ticket("ticket-15","customer-4","exciting-event-1",100.0));
    ticketInputTopic.pipeInput("ticket-16",new Ticket("ticket-16","customer-4","exciting-event-3",100.0));
    ticketInputTopic.pipeInput("ticket-17",new Ticket("ticket-17","customer-4","exciting-event-7",100.0));
    ticketInputTopic.pipeInput("ticket-18",new Ticket("ticket-18","customer-4","exciting-event-4",100.0));
    ticketInputTopic.pipeInput("ticket-19",new Ticket("ticket-19","customer-4","exciting-event-6",100.0));
    
    // Customer-5 tickets
    ticketInputTopic.pipeInput("ticket-20",new Ticket("ticket-20","customer-5","exciting-event-1",100.0));
    ticketInputTopic.pipeInput("ticket-21",new Ticket("ticket-21","customer-5","exciting-event-5",100.0));
    ticketInputTopic.pipeInput("ticket-22",new Ticket("ticket-22","customer-5","exciting-event-2",100.0));
    ticketInputTopic.pipeInput("ticket-23",new Ticket("ticket-23","customer-5","exciting-event-4",100.0));
    ticketInputTopic.pipeInput("ticket-24",new Ticket("ticket-24","customer-5","exciting-event-6",100.0));
    ticketInputTopic.pipeInput("ticket-25",new Ticket("ticket-25","customer-5","exciting-event-8",100.0));
    

    // When reading the output records
    var outputRecords = outputTopic.readRecordsToList();

    // Then the expected number of records were received
    //assertEquals(1, outputRecords.getLast());

    //expected output: {"venueName":"Guthrie Theater", "avgOutOfStateAttendeesPerEvent":3.5}
    //assertEquals("Guthrie Theater", outputRecords.getLast().value().getVenueName());
    assertEquals(3.5, outputRecords.getLast().value().getAvgOutOfStateAttendeesPerEvent());
  }
}