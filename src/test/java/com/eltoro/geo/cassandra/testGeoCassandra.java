package com.eltoro.geo.cassandra;

import com.datastax.driver.core.Session;
import com.eltoro.geo.models.S2HashRange;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.spring.*;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.config.KeyspaceAttributes;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.KeyspaceOption;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.data.cassandra.config.CassandraClusterFactoryBean;
import org.springframework.data.cassandra.config.CassandraSessionFactoryBean;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.config.java.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by ljacobsen on 4/14/15.
 */
@Test
@ContextConfiguration
@TestExecutionListeners(inheritListeners = false,value={CassandraUnitDependencyInjectionTestExecutionListener.class,DependencyInjectionTestExecutionListener.class })
@EmbeddedCassandra
@CassandraDataSet
public class testGeoCassandra extends AbstractTestNGSpringContextTests
{
    @Autowired
    private GeoCassandraRepositoryInstance repositoryInstance;

    @Autowired
    private CassandraNoSqlGeoClient<GeoCassandraTableInstance> noSqlGeoClient;

    @Autowired
    private CassandraOperations m_cassandraOperations;

    @Autowired
    private Session m_session;

    @Test
    public void testRangeQuery() {
        List<GeoCassandraTableInstance> celldata = repositoryInstance.findByRange( -852015L, -8520151733974859775L, -8520151733974859770L );
        Assert.assertTrue( celldata.size() > 0 );

    }

    @Test
    public void testNoSqlGeoClient() throws ExecutionException, InterruptedException
    {
        ListenableFuture<Iterable<GeoCassandraTableInstance>> listenableFuture = noSqlGeoClient.findRange( new S2HashRange(  -8520151733974859775L, -8520151733974859770L ) );
        Iterable<GeoCassandraTableInstance> geoCassandraTableInstances = listenableFuture.get();
        Assert.assertTrue( geoCassandraTableInstances.iterator().hasNext() );
    }

    @Configuration
    @EnableCassandraRepositories(basePackageClasses = { GeoCassandraRepositoryInstance.class })
    public static class CassandraConfiguration extends AbstractCassandraConfiguration
    {
        public static final String KEYSPACE_NAME = "cassandra_unit_keyspace";
        public static final int PORT = 9142;

        @Override
        protected String getKeyspaceName() {
            return KEYSPACE_NAME;
        }

        @Override
        protected int getPort() {
            return PORT;
        }

        protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
            ArrayList<CreateKeyspaceSpecification> list = new ArrayList<CreateKeyspaceSpecification>(  );
            CreateKeyspaceSpecification specification = CreateKeyspaceSpecification.createKeyspace().name(getKeyspaceName());
            specification.with( KeyspaceOption.REPLICATION, KeyspaceAttributes.newSimpleReplication( 1L ) );
            list.add(specification);
            return list;
        }

        @Bean
        public CassandraNoSqlGeoClient<GeoCassandraTableInstance> getNoSqlGeoClient() {
            return new CassandraNoSqlGeoClient<GeoCassandraTableInstance>();
        }

    }
}
