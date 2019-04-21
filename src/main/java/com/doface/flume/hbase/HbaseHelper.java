package com.doface.flume.hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import com.doface.flume.source.SQLSourceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseHelper {
	private static final Logger LOG = LoggerFactory
			.getLogger(HbaseHelper.class);

	private Connection conn;
	private Statement stat;
	private SQLSourceHelper sqlSourceHelper;
	private String driver = "";
	private String url = "";

	/**
	 * Constructor to initialize hibernate configuration parameters
	 * @param sqlSourceHelper Contains the configuration parameters from flume config file
	 */
	public HbaseHelper(SQLSourceHelper sqlSourceHelper) {

		this.sqlSourceHelper = sqlSourceHelper;
		Context context = sqlSourceHelper.getContext();

		/* check for mandatory propertis */
		sqlSourceHelper.checkMandatoryProperties();

		Map<String,String> hibernateProperties = context.getSubProperties("hibernate.");
		Iterator<Map.Entry<String,String>> it = hibernateProperties.entrySet().iterator();
		
		Map.Entry<String, String> e;
		
		while (it.hasNext()){
			e = it.next();
			if ("connection.driver_class".equalsIgnoreCase(e.getKey())) {
				driver = e.getValue();
			}else if ("connection.url".equalsIgnoreCase(e.getKey())) {
				url = e.getValue();
			}
		}
		
        

	}

	public void establishSession() {
		try {
        	try {
				Class.forName(driver);
			} catch (ClassNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			conn = DriverManager.getConnection(url);
			stat = conn.createStatement();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	/**
	 * Close database connection
	 */
	public void closeSession() {

		LOG.info("Closing hibernate session");

		if (stat != null) {
			try {
				stat.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Execute the selection query in the database
	 * @return The query result. Each Object is a cell content. <p>
	 * The cell contents use database types (date,int,string...), 
	 * keep in mind in case of future conversions/castings.
	 * @throws InterruptedException 
	 */
	public List<List<Object>> executeQuery(String lastFieldIndex) throws InterruptedException {
		List<List<Object>> rowsList = new ArrayList<List<Object>>() ;
		try {
			if (conn == null || conn.isClosed()){
				resetConnection();
			}
					
			ResultSet rs = stat.executeQuery(sqlSourceHelper.buildQuery());
			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();
			
			// 遍历ResultSet中的每条数据
			while (rs.next()) {
				List<Object> row = new ArrayList<Object>();
				// 遍历每一列
				for (int i = 1; i <= columnCount; i++) {
					String columnName = metaData.getColumnLabel(i);
					String value = rs.getString(columnName);
					row.add(value);
				}
				rowsList.add(row);
			}
			rs.close();
		}catch(Exception e) {
			e.printStackTrace();
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
				sqlSourceHelper.setCurrentIndex(rowsList.get(rows).get(cols).toString());
			}
		}
		
		return rowsList;
	}

	private void resetConnection() throws InterruptedException{
		if(conn != null) {
			try {
				if (conn.isClosed()){
					establishSession();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} else {
			establishSession();
		}
		
	}
}
