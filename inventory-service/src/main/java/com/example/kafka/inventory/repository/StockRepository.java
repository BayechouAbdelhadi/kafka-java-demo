package com.example.kafka.inventory.repository;

import com.example.kafka.inventory.entity.Stock;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Collection;
import java.util.List;

public interface StockRepository extends JpaRepository<Stock, String> {

    /** Row-locks the stocks (in a stable order to avoid deadlocks) so concurrent orders can't oversell. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT s FROM Stock s WHERE s.productId IN :ids ORDER BY s.productId")
    List<Stock> lockAll(@Param("ids") Collection<String> productIds);
}
