package org.improving.workshop.qs_3;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopOutsideStateResult {
    public static final String OUTPUT_TOPIC = "kafka-workshop-average-out-of-state-attendees-per-venue-per-event";
    private String venueName;
    private Double avgOutOfStateAttendeesPerEvent;
}


