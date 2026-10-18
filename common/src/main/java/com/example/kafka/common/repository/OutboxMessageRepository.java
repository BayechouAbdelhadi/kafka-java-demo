package com.example.kafka.common.repository;

import com.example.kafka.common.entity.OutboxMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface OutboxMessageRepository extends JpaRepository<OutboxMessage, Long> {

    /**
     * Tries to take the relay mutex for the current transaction. Empty if another relay holds it;
     * released on commit or rollback.
     */
    @Query(value = "SELECT id FROM outbox_relay_lock WHERE id = 1 FOR UPDATE SKIP LOCKED", nativeQuery = true)
    Optional<Integer> tryLockRelay();

    /**
     * Locks the oldest pending rows. SKIP LOCKED lets several service instances relay in parallel
     * without publishing the same row twice.
     */
    @Query(value = "SELECT * FROM outbox_messages ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<OutboxMessage> lockNextBatch(@Param("limit") int limit);
}
