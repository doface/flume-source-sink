package com.doface.flume.source;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.hibernate.CacheMode;
import com.hibernate.Query;
import com.hibernate.Session;
import com.hibernate.SessionFactory;
import com.hibernate.boot.registry.StandardServiceRegistryBuilder;
import com.hibernate.cfg.Configuration;
import com.hibernate.service.ServiceRegistry;
import com.hibernate.transform.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.Context;

/**
 * Helper class to manage hibernate sessions and perform queries
 * 
 * @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
 *
 */
public class HibernateHelper {

	private static final Logger LOG = LoggerFactory
			.getLogger(HibernateHelper.class);

	private static SessionFactory factory;
	private Session session;
	private ServiceRegistry serviceRegistry;
	private Configuration config;
	private SQLSourceHelper sqlSourceHelper;

	/**
	 * Constructor to initialize hibernate configuration parameters
	 * @param sqlSourceHelper Contains the configuration parameters from flume config file
	 */
	public HibernateHelper(SQLSourceHelper sqlSourceHelper) {

		this.sqlSourceHelper = sqlSourceHelper;
		Context context = sqlSourceHelper.getContext();

		/* check for mandatory propertis */
		sqlSourceHelper.checkMandatoryProperties();

		Map<String,String> hibernateProperties = context.getSubProperties("hibernate.");
		Iterator<Map.Entry<String,String>> it = hibernateProperties.entrySet().iterator();
		
		config = new Configuration();
		Map.Entry<String, String> e;
		
		while (it.hasNext()){
			e = it.next();
			config.setProperty("hibernate." + e.getKey(), e.getValue());
		}

	}

	/**
	 * Connect to database using hibernate
	 */
	public void establishSession() {

		LOG.info("Opening hibernate session");

		serviceRegistry = new StandardServiceRegistryBuilder()
				.applySettings(config.getProperties()).build();
		factory = config.buildSessionFactory(serviceRegistry);
		session = factory.openSession();
		session.setCacheMode(CacheMode.IGNORE);
		
		session.setDefaultReadOnly(sqlSourceHelper.isReadOnlySession());
	}

	/**
	 * Close database connection
	 */
	public void closeSession() {

		LOG.info("Closing hibernate session");

		session.close();
		factory.close();
	}

	/**
	 * Execute the selection query in the database
	 * @return The query result. Each Object is a cell content. <p>
	 * The cell contents use database types (date,int,string...), 
	 * keep in mind in case of future conversions/castings.
	 * @throws InterruptedException 
	 */
	@SuppressWarnings("unchecked")
	public List<List<Object>> executeQuery(String lastFieldIndex) throws InterruptedException {
		
		List<List<Object>> rowsList = new ArrayList<List<Object>>() ;
		Query query;
		
		if (!session.isConnected()){
			resetConnection();
		}
				
		if (sqlSourceHelper.isCustomQuerySet()){
			
			query = session.createSQLQuery(sqlSourceHelper.buildQuery());
			
			if (sqlSourceHelper.getMaxRows() != 0){
				query = query.setMaxResults(sqlSourceHelper.getMaxRows());
			}			
		}
		else
		{
			query = session
					.createSQLQuery(sqlSourceHelper.getQuery())
					.setFirstResult(Integer.parseInt(sqlSourceHelper.getCurrentIndex()));
			
			if (sqlSourceHelper.getMaxRows() != 0){
				query = query.setMaxResults(sqlSourceHelper.getMaxRows());
			}
		}
		
		try {
			rowsList = query.setFetchSize(sqlSourceHelper.getMaxRows()).setResultTransformer(Transformers.TO_LIST).list();
		}catch (Exception e){
			LOG.error("Exception thrown, resetting connection.",e);
			resetConnection();
		}
		if (!rowsList.isEmpty()){
			if (sqlSourceHelper.getLastFieldIndex() != null && !"".equals(sqlSourceHelper.getLastFieldIndex())) {
				int rows = rowsList.size() - 1;
				int cols = 0;
				if (lastFieldIndex != null && !"".equals(lastFieldIndex)) {
					cols = Integer.parseInt(lastFieldIndex);
				}else {
					cols = rowsList.get(0).size() - 1;
				}
				Object colData = rowsList.get(rows).get(cols);
				sqlSourceHelper.setCurrentIndex(colData.toString());
			}
		}
		
		return rowsList;
	}

	private void resetConnection() throws InterruptedException{
		if(session.isOpen()){
			session.close();
			factory.close();
		} else {
			establishSession();
		}
		
	}
}
