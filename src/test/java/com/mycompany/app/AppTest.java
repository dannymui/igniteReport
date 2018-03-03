package com.mycompany.app;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class AppTest
{
	@Test
	public void enum_update() {
		   CacheConfiguration< String, SampleObject > config = new CacheConfiguration<>();
		    config.setName( "testCache" );
		    config.setCacheMode( CacheMode.PARTITIONED );
		    config.setAtomicityMode( CacheAtomicityMode.ATOMIC );
		    config.setManagementEnabled( true );
		    config.setStatisticsEnabled( true );
		    config.setQueryEntities( new ArrayList<>() );
		    
		    QueryEntity entity = new QueryEntity( String.class.getName(), SampleObject.class.getName() );
		    entity.setKeyType( String.class.getName() );
		    entity.setValueType( SampleObject.class.getName() );
		    entity.setTableName( "sqlTable" );
		    entity.setKeyFieldName( "key" );
		    entity.setFields( new LinkedHashMap<>() );
		    entity.setIndexes( new ArrayList<>() );
		    
		    entity.getFields().put( "status", SampleEnum.class.getName() );
		    entity.getFields().put( "key", String.class.getName() );
		    config.getQueryEntities().add( entity );

		    IgniteConfiguration igniteConfig = new IgniteConfiguration();
		    igniteConfig.setClientMode( false );
		    igniteConfig.setCacheConfiguration( config );
		    Ignite ignite = Ignition.start( igniteConfig );

		    IgniteCache< String, SampleObject > cache = ignite.cache( "testCache");
		    cache.put( "first", new SampleObject( SampleEnum.FIRST, "first") );

		    Assert.assertEquals( SampleEnum.FIRST, cache.get( "first").status );
		    
		    SqlFieldsQuery query = new SqlFieldsQuery( "update sqlTable set status = ? where key = ? ");
		    query.setArgs( SampleEnum.SECOND, "first", SampleEnum.SECOND );
		    cache.query( query );
		    Assert.assertEquals( SampleEnum.SECOND, cache.get( "first").status );
	}

}
