package com.pms.transactional.service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.pms.transactional.entities.OutboxEventEntity;
import com.pms.transactional.exceptions.PoisonPillException;

public class ProcessingResult {

    private final List<UUID> successfulIds;
    private final PoisonPillException poisonPill;
    private final boolean systemFailureOccurred;

    private ProcessingResult(List<UUID> successfulIds, PoisonPillException poisonPill,
            boolean systemFailureOccurred) {
        this.successfulIds = successfulIds != null ? successfulIds : new ArrayList<>();
        this.poisonPill = poisonPill;
        this.systemFailureOccurred = systemFailureOccurred;
    }

    public static ProcessingResult success(List<UUID> successfulIds) {
        return new ProcessingResult(successfulIds, null, false);
    }

    public static ProcessingResult withPoisonPill(List<UUID> successfulIds, PoisonPillException poisonPill) {
        return new ProcessingResult(successfulIds, poisonPill, false);
    }

    public static ProcessingResult systemFailure(List<UUID> successfulIds) {
        return new ProcessingResult(successfulIds, null, true);
    }

    public List<UUID> getSuccessfulIds() {
        return successfulIds;
    }

    public PoisonPillException getPoisonPill() {
        return poisonPill;
    }

    public boolean hasPoisonPill() {
        return poisonPill != null;
    }

    public boolean hasSystemFailure() {
        return systemFailureOccurred;
    }

    public boolean isFullSuccess() {
        return !hasPoisonPill() && !hasSystemFailure();
    }
}
