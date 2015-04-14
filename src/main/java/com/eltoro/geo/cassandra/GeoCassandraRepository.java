package com.eltoro.geo.cassandra;

import com.eltoro.geo.models.S2HashRange;
import org.springframework.data.cassandra.repository.CassandraRepository;

import java.util.List;

/**
 * Created by ljacobsen on 4/14/15.
 */
public interface GeoCassandraRepository<T> extends CassandraRepository<T>
{
    public abstract <S extends T> List<S> findByCellId(Long s2HashKey, Long s2CellId);
    public abstract <S extends T> List<S> findByRange(Long s2HashKey, Long s2CellIdMin, Long s2CellIdMax);

}
