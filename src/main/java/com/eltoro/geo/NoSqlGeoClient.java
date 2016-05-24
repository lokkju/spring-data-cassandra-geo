package com.eltoro.geo;

import com.eltoro.geo.models.NoSqlGeoEntity;
import com.eltoro.geo.models.S2HashRange;
import com.google.common.base.Function;
import com.google.common.geometry.*;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.apachecommons.CommonsLog;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by ljacobsen on 4/11/15.
 */
@CommonsLog
public abstract class NoSqlGeoClient<T extends NoSqlGeoEntity>
{

    // Public constants
    public static final long MERGE_THRESHOLD = 2;
    public static final int DEFAULT_HASHKEY_LENGTH = 6;
    public static final int DEFAULT_MIN_LEVEL = 1;
    public static final int DEFAULT_MAX_LEVEL = 30;
    public static final int DEFAULT_MAX_CELLS = 200;

    private static final int DEFAULT_THREAD_POOL_SIZE = 10;

    @Getter
    @Setter
    protected int hashKeyLength = DEFAULT_HASHKEY_LENGTH;

    @Getter
    @Setter
    protected int minLevel = DEFAULT_MIN_LEVEL;

    @Getter
    @Setter
    protected int maxLevel = DEFAULT_MAX_LEVEL;

    @Getter
    @Setter
    protected int maxCells = DEFAULT_MAX_CELLS;

    @Setter
    protected ListeningExecutorService executorService;

    public ListeningExecutorService getExecutorService() {
        synchronized (this) {
            if (executorService == null) {
                executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool( DEFAULT_THREAD_POOL_SIZE ));
            }
        }
        return executorService;
    }

    /**
     * Merge continuous cells in cellUnion and return a list of merged GeohashRanges.
     *
     * @param cellUnion
     *            Container for multiple cells.
     *
     * @return A list of merged GeohashRanges.
     */
    private List<S2HashRange> mergeCells(S2CellUnion cellUnion) {

        List<S2HashRange> ranges = new ArrayList<S2HashRange>();
        for (S2CellId c : cellUnion.cellIds()) {
            S2HashRange range = new S2HashRange(c.rangeMin().id(), c.rangeMax().id());

            boolean wasMerged = false;
            for (S2HashRange r : ranges) {
                if (r.tryMerge(range)) {
                    wasMerged = true;
                    break;
                }
            }

            if (!wasMerged) {
                ranges.add(range);
            }
        }

        return ranges;
    }

    public static long generateHashKey(long s2GeoHash, int hashKeyLength) {
        if (s2GeoHash < 0) {
            // Counteract "-" at beginning of geohash.
            hashKeyLength++;
        }
        String s2geohashString = String.valueOf(s2GeoHash);
        long denominator = (long) Math.pow(10, s2geohashString.length() - hashKeyLength);
        return s2GeoHash / denominator;
    }
    public static S2CellUnion findCellIds(S2LatLngRect latLngRect) {

        ConcurrentLinkedQueue<S2CellId> queue = new ConcurrentLinkedQueue<S2CellId>();
        ArrayList<S2CellId> cellIds = new ArrayList<S2CellId>();

        for (S2CellId c = S2CellId.begin(0); !c.equals(S2CellId.end(0)); c = c.next()) {
            if (containsGeodataToFind(c, latLngRect)) {
                queue.add(c);
            }
        }

        processQueue(queue, cellIds, latLngRect);
        assert queue.size() == 0;
        queue = null;

        if (cellIds.size() > 0) {
            S2CellUnion cellUnion = new S2CellUnion();
            cellUnion.initFromCellIds(cellIds); // This normalize the cells.
            // cellUnion.initRawCellIds(cellIds); // This does not normalize the cells.
            cellIds = null;

            return cellUnion;
        }

        return null;
    }

    private static boolean containsGeodataToFind(S2CellId c, S2LatLngRect latLngRect) {
        if (latLngRect != null) {
            return latLngRect.intersects(new S2Cell(c));
        }

        return false;
    }

    private static void processQueue(ConcurrentLinkedQueue<S2CellId> queue, ArrayList<S2CellId> cellIds,
            S2LatLngRect latLngRect) {
        for (S2CellId c = queue.poll(); c != null; c = queue.poll()) {

            if (!c.isValid()) {
                break;
            }

            processChildren(c, latLngRect, queue, cellIds);
        }
    }

    private static void processChildren(S2CellId parent, S2LatLngRect latLngRect,
            ConcurrentLinkedQueue<S2CellId> queue, ArrayList<S2CellId> cellIds) {
        List<S2CellId> children = new ArrayList<S2CellId>(4);

        for (S2CellId c = parent.childBegin(); !c.equals(parent.childEnd()); c = c.next()) {
            if (containsGeodataToFind(c, latLngRect)) {
                children.add(c);
            }
        }

        /*
         * TODO: Need to update the strategy!
         *
         * Current strategy:
         * 1 or 2 cells contain cellIdToFind: Traverse the children of the cell.
         * 3 cells contain cellIdToFind: Add 3 cells for result.
         * 4 cells contain cellIdToFind: Add the parent for result.
         *
         * ** All non-leaf cells contain 4 child cells.
         */
        if (children.size() == 1 || children.size() == 2) {
            for (S2CellId child : children) {
                if (child.isLeaf()) {
                    cellIds.add(child);
                } else {
                    queue.add(child);
                }
            }
        } else if (children.size() == 3) {
            cellIds.addAll(children);
        } else if (children.size() == 4) {
            cellIds.add(parent);
        } else {
            assert false; // This should not happen.
        }
    }
    protected <S extends T> ListenableFuture<List<Iterable<S>>> rawPolygonQuery(final S2Polygon polygon) {
        S2RegionCoverer coverer = new S2RegionCoverer();
        coverer.setMinLevel( minLevel );
        coverer.setMaxLevel( maxLevel );
        coverer.setMaxCells( maxCells );
        S2CellUnion s2CellUnion = coverer.getCovering(polygon);
        List<S2HashRange> ranges = mergeCells(s2CellUnion);
        ArrayList<ListenableFuture<Iterable<S>>> listenableFutures = new ArrayList<ListenableFuture<Iterable<S>>>(  );
        for (S2HashRange unsplitRange: ranges) {
            for ( final S2HashRange range : unsplitRange.trySplit(hashKeyLength)) {
                final ListenableFuture<Iterable<S>> listenableFuture = this.findRange( range );
                listenableFutures.add( listenableFuture );
            }
        }
        log.debug( "Handling Polygon Query with " + listenableFutures.size() + " requests" );
        return Futures.allAsList(listenableFutures);
    }
    protected static S2LatLngRect getBoundingLatLngRectangle(S2LatLng centerPoint, double radiusInMeters) {
        double latReferenceUnit = centerPoint.latDegrees() > 0.0 ? -1.0 : 1.0;
        S2LatLng latReferenceLatLng = S2LatLng.fromDegrees(centerPoint.latDegrees() + latReferenceUnit,
                centerPoint.lngDegrees());
        double lngReferenceUnit = centerPoint.lngDegrees() > 0.0 ? -1.0 : 1.0;
        S2LatLng lngReferenceLatLng = S2LatLng.fromDegrees(centerPoint.latDegrees(), centerPoint.lngDegrees()
                + lngReferenceUnit);

        double latForRadius = radiusInMeters / centerPoint.getEarthDistance(latReferenceLatLng);
        double lngForRadius = radiusInMeters / centerPoint.getEarthDistance(lngReferenceLatLng);

        S2LatLng minLatLng = S2LatLng.fromDegrees(centerPoint.latDegrees() - latForRadius,
                centerPoint.lngDegrees() - lngForRadius);
        S2LatLng maxLatLng = S2LatLng.fromDegrees(centerPoint.latDegrees() + latForRadius,
                centerPoint.lngDegrees() + lngForRadius);

        return new S2LatLngRect(minLatLng, maxLatLng);
    }
    public <S extends T> ListenableFuture<List<T>> polygonQuery(final S2Polygon polygon) {
        Function<List<Iterable<T>>, List<T>> filterFunction =
               new Function<List<Iterable<T>>, List<T>>() {
                 public List<T> apply(List<Iterable<T>> rawResults) {
                    ArrayList<T> filteredResults = new ArrayList<T>(  );
                    for(Iterable<T> iterableEntity: rawResults) {
                        for (T entity : iterableEntity) {
                            if(polygon.contains( new S2Cell( new S2CellId(entity.getS2CellId()) ) )) {
                                filteredResults.add( entity );
                            }
                        }
                    }
                    return filteredResults;
                 }
               };
        return Futures.transform( rawPolygonQuery( polygon ), filterFunction );
    }
    /*
    static <S extends T> ListenableFuture<List<T>> radiusQuery(final S2LatLng centerPoint, final int radiusInMeters) {
        S2LatLngRect latLngRect = getBoundingLatLngRectangle( centerPoint, radiusInMeters );
        // TODO: make latLngRect to S2Polygon
        S2CellUnion cellUnion = findCellIds(latLngRect);
        List<S2HashRange> ranges = mergeCells(cellUnion);

        Function<List<Iterable<T>>, List<T>> filterFunction =
               new Function<List<Iterable<T>>, List<T>>() {
                 static List<T> apply(List<Iterable<T>> rawResults) {
                    ArrayList<T> filteredResults = new ArrayList<T>(  );
                    for(Iterable<T> iterableEntity: rawResults) {
                        for (T entity : iterableEntity) {
                            S2LatLng latLng = new S2LatLng(new S2Cell(new S2CellId( entity.getS2CellId() )).getCenter());
                            if (centerPoint != null && radiusInMeters > 0 && centerPoint.getEarthDistance(latLng) <= radiusInMeters) {
                                filteredResults.add( entity );
                            }
                        }
                    }
                    return filteredResults;
                 }
               };
        return Futures.transform( rawPolygonQuery( polygon ), filterFunction );
    }
    */

    public abstract <S extends T> ListenableFuture<S> save(S entity);
    public abstract <S extends T> ListenableFuture<Iterable<S>> save(Iterable<S> entities);
    public abstract <S extends T> ListenableFuture<Iterable<S>> find(Long cellId);
    public abstract <S extends T> ListenableFuture<Iterable<S>> findRange(S2HashRange range);
    public abstract void delete(T entity);
    public abstract void delete(Iterable<? extends T> entities);
}
