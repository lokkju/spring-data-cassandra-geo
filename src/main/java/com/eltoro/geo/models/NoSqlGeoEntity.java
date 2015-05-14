package com.eltoro.geo.models;

/**
 * Created by ljacobsen on 4/13/15.
 */
public interface NoSqlGeoEntity
{
    public long getS2HashKey();
    public long getS2CellId();
    public void setS2HashKey(long s2HashKey);
    public void setS2CellId(long s2CellId);
}
