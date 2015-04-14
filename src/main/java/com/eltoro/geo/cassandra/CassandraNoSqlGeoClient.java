package com.eltoro.geo.cassandra;

import com.eltoro.geo.NoSqlGeoClient;
import com.eltoro.geo.models.S2HashRange;
import com.google.common.util.concurrent.ListenableFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.support.BasicMapId;
import org.springframework.scheduling.annotation.Async;
import org.testng.Assert;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by ljacobsen on 4/14/15.
 */
public class CassandraNoSqlGeoClient<T> extends NoSqlGeoClient<T>
{
    @Autowired
    private GeoCassandraRepository<T> repository;

    public <S extends T> ListenableFuture<S> save(final S entity) {
        return getExecutorService().submit( new Callable<S>()
        {
            @Override
            public S call() throws Exception
            {
                return repository.save( entity );
            }
        } );
    }

    @Override
    public <S extends T> ListenableFuture<Iterable<S>> save(final Iterable<S> entities) {
        return getExecutorService().submit( new Callable<Iterable<S>>()
        {
            @Override
            public Iterable<S> call() throws Exception
            {
                return repository.save( entities );
            }
        } );
    }

    @Override
    public <S extends T> ListenableFuture<Iterable<S>> find(final Long cellId) {
        return getExecutorService().submit( new Callable<Iterable<S>>()
        {
            @Override
            public Iterable<S> call() throws Exception
            {
                return repository.findByCellId( generateHashKey(cellId, getHashKeyLength()), cellId );
            }
        } );
    }

    @Override
    public <S extends T> ListenableFuture<Iterable<S>> findRange( final S2HashRange range){
        Assert.assertEquals( generateHashKey( range.getRangeMin(), getHashKeyLength() ),generateHashKey( range.getRangeMax(), getHashKeyLength() ) );
        return getExecutorService().submit( new Callable<Iterable<S>>()
        {
            @Override
            public Iterable<S> call() throws Exception
            {
                return repository.findByRange( generateHashKey( range.getRangeMin(), getHashKeyLength() ), range.getRangeMin(), range.getRangeMax() );
            }
        } );
    }

    @Override
    public void delete(T entity){
        repository.delete( entity );
    }
    @Override
    public void delete(Iterable<? extends T> entities){
        repository.delete( entities );
    }
}
