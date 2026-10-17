package com.example.kafka.common.repository;

import com.example.kafka.common.entity.OutboxMessage;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OutboxMessageRepository extends JpaRepository<OutboxMessage, String> {

    @Query("SELECT o FROM OutboxMessage o WHERE o.status = 'PENDING' ORDER BY o.createdAt ASC")
    List<OutboxMessage> findPendingMessages(Pageable pageable);
}
