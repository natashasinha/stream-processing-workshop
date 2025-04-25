package org.improving.workshop.qs_4;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.customer.address.Address;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopUniqueStreamingCustomersResult {
    public static final String OUTPUT_TOPIC = "kafka-workshop-top-unique-streaming-customer-for-artist";
    private Artist artist;
    private Stream stream;
    private Address address;
}


