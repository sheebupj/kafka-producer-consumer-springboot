package com.paremal.kafka.jpa;

import com.paremal.kafka.entity.FailureRecord;
import org.springframework.data.repository.CrudRepository;

public interface FailureRecordRepository extends CrudRepository<FailureRecord,Integer> {
}
