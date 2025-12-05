package com.pms.transactional.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.pms.transactional.dao.OutboxEventsDao;

@Service
@EnableScheduling
public class OutboxPoller{

    @Autowired
    OutboxEventsDao outboxDao;

    @Scheduled(fixedRate=1000)
    public void pollAndPublish(){
        
    }

     
}