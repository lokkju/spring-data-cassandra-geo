package com.eltoro.geo.cassandra;

import com.eltoro.geo.models.NoSqlGeoEntity;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;

/**
 * Created by ljacobsen on 4/14/15.
 */
@Table
public class GeoCassandraTableInstance implements NoSqlGeoEntity
{
    @PrimaryKeyColumn( ordinal = 0, type = PrimaryKeyType.PARTITIONED)
    private long s2_hash_key;

    @PrimaryKeyColumn( ordinal = 0)
    private long s2_cell_id;

    private String data;

    public GeoCassandraTableInstance(long s2_hash_key, long s2_cell_id, String data){
        this.s2_hash_key = s2_hash_key;
        this.s2_cell_id = s2_cell_id;
        this.data = data;
    }

    @Override
    public long getS2HashKey()
    {
        return s2_hash_key;
    }

    @Override
    public long getS2CellId()
    {
        return s2_cell_id;
    }

    @Override
    public void setS2HashKey( long s2HashKey )
    {
        s2_hash_key = s2HashKey;
    }

    @Override
    public void setS2CellId( long s2CellId )
    {
        s2_cell_id = s2CellId;
    }
}
