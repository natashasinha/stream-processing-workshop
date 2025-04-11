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

  private final static JsonSerde<TopOutsideStateResult> topOutOfStateSerde = new JsonSerde<TopOutsideStateResult>();

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
            stringSerializer,
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
    venuesInputTopic.pipeInput("venue-1",new Venue("venue-1","v1_addr","The Sphere",100));
    venuesInputTopic.pipeInput("venue-2",new Venue("venue-2","v2_addr","Excel Energy Center",100));
    venuesInputTopic.pipeInput("venue-3",new Venue("venue-3","v3_addr","Guthrie Theater",100));
    venuesInputTopic.pipeInput("venue-4",new Venue("venue-4","v4_addr","The Met",100));

    Address v1_addr = new Address("v1_addr","venue-1","formatCode","sType","777 ficticious dr","ln2","Las Vegas","NV","zip","zip5","countycd",0.2221,0.5555);
    Address v2_addr = new Address("v2_addr","venue-1","formatCode","sType","888 dubious blvd","ln2","St. Paul","MN","zip","zip5","countycd",0.2222,0.5555);
    Address v3_addr = new Address("v3_addr","venue-1","formatCode","sType","999 Bogus ave","ln2","Minneapols","MN","zip","zip5","countycd",0.2223,0.5555);
    Address v4_addr = new Address("v4_addr","venue-1","formatCode","sType","1000 5th Ave","ln2","New York","NY","zip","zip5","countycd",0.2224,0.5555);
    addressInputTopic.pipeInput("v1_addr",v1_addr);
    addressInputTopic.pipeInput("v2_addr",v2_addr);
    addressInputTopic.pipeInput("v3_addr",v3_addr);
    addressInputTopic.pipeInput("v4_addr",v4_addr);

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
    eventInputTopic.pipeInput("exciting-event-6", new Event("exciting-event-5", "artist-1", "venue-3", 10, "today"));
    eventInputTopic.pipeInput("exciting-event-7", new Event("exciting-event-5", "artist-1", "venue-4", 10, "today"));
    eventInputTopic.pipeInput("exciting-event-8", new Event("exciting-event-5", "artist-1", "venue-4", 10, "today"));

    // Customer-1 tickets
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-1", "exciting-event-1"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-1", "exciting-event-3"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-1", "exciting-event-5"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-1", "exciting-event-7"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-1", "exciting-event-4"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-1", "exciting-event-8"));
    // Customer-2 tickets
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-2", "exciting-event-1"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-2", "exciting-event-5"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-2", "exciting-event-7"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-2", "exciting-event-6"));
    // Customer-3 tickets
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-3", "exciting-event-1"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-3", "exciting-event-5"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-3", "exciting-event-7"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-3", "exciting-event-4"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-3", "exciting-event-6"));
    // Customer-4 tickets
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-4", "exciting-event-1"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-4", "exciting-event-3"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-4", "exciting-event-7"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-4", "exciting-event-4"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-4", "exciting-event-6"));
    // Customer-5 tickets
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-5", "exciting-event-1"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-5", "exciting-event-5"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-5", "exciting-event-2"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-5", "exciting-event-4"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-5", "exciting-event-6"));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-5", "exciting-event-8"));

    // When reading the output records
    var outputRecords = outputTopic.readRecordsToList();

    // Then the expected number of records were received
    assertEquals(1, outputRecords.size());

    //expected output: {"venueName":"Guthrie Theater", "avgOutOfStateAttendeesPerEvent":3.5}
    assertEquals("Guthrie Theater", outputRecords.get(0).value().getVenueName());
    assertEquals(3.5, outputRecords.get(0).value().getAvgOutOfStateAttendeesPerEvent());
  }
}