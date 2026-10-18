package com.example.kafka.common.repository;

import com.example.kafka.common.entity.OutboxMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface OutboxMessageRepository extends JpaRepository<OutboxMessage, Long> {

    /**
     * Locks the oldest pending rows. SKIP LOCKED lets several service instances relay in parallel
     * without publishing the same row twice.
     */
    @Query(value = "SELECT * FROM outbox_messages ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<OutboxMessage> lockNextBatch(@Param("limit") int limit);
}
