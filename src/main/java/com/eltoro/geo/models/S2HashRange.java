package com.eltoro.geo.models;

import com.eltoro.geo.NoSqlGeoClient;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ljacobsen on 4/13/15.
 */
public class S2HashRange
{
    @Getter
    @Setter
    private long rangeMin;

    @Getter
    @Setter
    private long rangeMax;

    public S2HashRange( long range1, long range2 )
    {
        this.rangeMin = Math.min( range1, range2 );
        this.rangeMax = Math.max( range1, range2 );
    }

    public boolean tryMerge( S2HashRange range )
    {
        if ( range.getRangeMin() - this.rangeMax <= NoSqlGeoClient.MERGE_THRESHOLD
                && range.getRangeMin() - this.rangeMax > 0 ) {
            this.rangeMax = range.getRangeMax();
            return true;
        }

        if ( this.rangeMin - range.getRangeMax() <= NoSqlGeoClient.MERGE_THRESHOLD
                && this.rangeMin - range.getRangeMax() > 0 ) {
            this.rangeMin = range.getRangeMin();
            return true;
        }

        return false;
    }

    /*
     * Try to split the range to multiple ranges based on the hash key.
     *
     * e.g., for the following range:
     *
     * min: 123456789
     * max: 125678912
     *
     * when the hash key length is 3, we want to split the range to:
     *
     * 1
     * min: 123456789
     * max: 123999999
     *
     * 2
     * min: 124000000
     * max: 124999999
     *
     * 3
     * min: 125000000
     * max: 125678912
     *
     * For this range:
     *
     * min: -125678912
     * max: -123456789
     *
     * we want:
     *
     * 1
     * min: -125678912
     * max: -125000000
     *
     * 2
     * min: -124999999
     * max: -124000000
     *
     * 3
     * min: -123999999
     * max: -123456789
     */
    public List<S2HashRange> trySplit( int s2geohashKeyLength )
    {
        List<S2HashRange> result = new ArrayList<S2HashRange>();

        long minHashKey = NoSqlGeoClient.generateHashKey( rangeMin, s2geohashKeyLength );
        long maxHashKey = NoSqlGeoClient.generateHashKey( rangeMax, s2geohashKeyLength );

        long denominator = (long) Math.pow( 10,
                String.valueOf( rangeMin ).length() - String.valueOf( minHashKey ).length() );

        if ( minHashKey == maxHashKey ) {
            result.add( this );
        } else {
            for ( long l = minHashKey; l <= maxHashKey; l++ ) {
                if ( l > 0 ) {
                    result.add( new S2HashRange( l == minHashKey ? rangeMin : l * denominator,
                            l == maxHashKey ? rangeMax : (l + 1) * denominator - 1 ) );
                } else {
                    result.add( new S2HashRange( l == minHashKey ? rangeMin : (l - 1) * denominator + 1,
                            l == maxHashKey ? rangeMax : l * denominator ) );
                }
            }
        }

        return result;
    }
}
