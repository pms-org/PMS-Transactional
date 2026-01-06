package com.pms.transactional.service;

import java.util.List;
import java.util.UUID;

import com.pms.transactional.entities.OutboxEventEntity;

public record ProcessingResult(List<UUID> successfulIds,OutboxEventEntity poisonPill,boolean systemFailure){

    public static ProcessingResult success(List<UUID> ids){
        return new ProcessingResult(ids, null, false);
    }

    public static ProcessingResult poisonPill(List<UUID> ids, OutboxEventEntity bad){
        return new ProcessingResult(ids, bad, false);
    }

    public static ProcessingResult systemFailure(List<UUID> ids){
        return new ProcessingResult(ids, null, true);
    }
}

