package com.pms.transactional.exceptions;

import java.util.UUID;

public class PoisonPillException extends Exception {

    private final UUID eventId;

    public PoisonPillException(UUID eventId, String message, Throwable cause) {
        super(message, cause);
        this.eventId = eventId;
    }

    public UUID getEventId() {
        return eventId;
    }
}
