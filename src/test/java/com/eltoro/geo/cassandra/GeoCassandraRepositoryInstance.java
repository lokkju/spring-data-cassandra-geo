package com.eltoro.geo.cassandra;

import org.springframework.data.cassandra.repository.Query;

import java.util.List;

/**
 * Created by ljacobsen on 4/14/15.
 */
public interface GeoCassandraRepositoryInstance extends GeoCassandraRepository<GeoCassandraTableInstance>
{
    @Override
    @Query("SELECT s2_hash_key,s2_cell_id, data FROM GeoCassandraTableInstance WHERE s2_hash_key = ?0 AND s2_cell_id = ?1")
    public List<GeoCassandraTableInstance> findByCellId(Long s2HashKey, Long s2CellId);

    @Override
    @Query("SELECT s2_hash_key,s2_cell_id, data FROM GeoCassandraTableInstance WHERE s2_hash_key = ?0 AND s2_cell_id >= ?1 AND s2_cell_id <= ?2")
    public abstract List<GeoCassandraTableInstance> findByRange(Long s2HashKey, Long s2CellIdMin, Long s2CellIdMax);

}
