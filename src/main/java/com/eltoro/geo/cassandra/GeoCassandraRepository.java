package com.eltoro.geo.cassandra;

import com.eltoro.geo.models.NoSqlGeoEntity;
import com.eltoro.geo.models.S2HashRange;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.repository.NoRepositoryBean;

import java.util.List;

/**
 * Created by ljacobsen on 4/14/15.
 */
@NoRepositoryBean
public interface GeoCassandraRepository<T extends NoSqlGeoEntity> extends CassandraRepository<T>
{
    public <S extends T> List<S> findByS2HashKeyAndS2CellId(Long s2HashKey, Long s2CellId);
    public <S extends T> List<S> findByRange(Long s2HashKey, Long s2CellIdMin, Long s2CellIdMax);

}
