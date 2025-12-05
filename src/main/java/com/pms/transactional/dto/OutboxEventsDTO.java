package com.pms.transactional.dto;

import java.util.UUID;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class OutboxEventsDTO{
    private UUID transactionOutboxId;
    private UUID transactionId;
    private String payload;
    private String status;
    private LocalDateTime createdAt;
}