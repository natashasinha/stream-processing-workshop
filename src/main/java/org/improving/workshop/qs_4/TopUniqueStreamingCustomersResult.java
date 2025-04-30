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
    public static final String OUTPUT_TOPIC = "data-demo-TopUniqueStreamingCustomersResults";
    private String streamId;
    private String customerId;
    private String artistId;
    private String customerState;
    private String artistName;
}


