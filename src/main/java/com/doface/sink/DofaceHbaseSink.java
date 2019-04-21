package com.doface.sink;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class DofaceHbaseSink extends AbstractSink implements Configurable {
    private Logger LOG = LoggerFactory.getLogger(DofaceHbaseSink.class);
    private String connectionUrl;
    private String connectionDriver;
    private String tableName;
    private String insertFields;
    private String primaryKey;
    private String primaryKeyIndexs;
    private String user;
    private String password;
    private PreparedStatement preparedStatement;
    private PreparedStatement preparedStatementDel;
    private Connection conn;
    private int batchSize;
    private Statement stat;

    public DofaceHbaseSink() {
        LOG.info("DofaceHbaseSink start...");
    }

    public void configure(Context context) {
    	connectionUrl = context.getString("connection.url");
        Preconditions.checkNotNull(connectionUrl, "connection.url must be set!!");
    	connectionDriver = context.getString("connection.driver");
        Preconditions.checkNotNull(connectionDriver, "connection.driver must be set!!");
        tableName = context.getString("tableName");
        insertFields = context.getString("insert_fields");
        primaryKey = context.getString("primaryKey", "");
        primaryKeyIndexs = "";
        Preconditions.checkNotNull(tableName, "tableName must be set!!");
        user = context.getString("connection.user");
        password = context.getString("connection.password");
        batchSize = context.getInteger("batchSize", 10000);
        Preconditions.checkNotNull(batchSize > 0, "batchSize must be a positive number!!");
    }

	/**
	 * 关闭Hbase-Phoenix连接
	 * @throws SQLException
	 */
	public void close() throws SQLException {
        if(stat!=null){
            stat.close();
        }
        if(conn!=null){
            conn.close();
        }
    }
	
    @Override
    public void start() {
        super.start();
        try {
            //调用Class.forName()方法加载驱动程序
            Class.forName(connectionDriver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        String url = connectionUrl;
        //调用DriverManager对象的getConnection()方法，获得一个Connection对象

        try {
            conn = DriverManager.getConnection(url);
            stat = conn.createStatement();
          //创建一个Statement对象
            String sql = "upsert into " + tableName +
                    " (" + insertFields + ") values (";
            String[] pks = primaryKey.split(",");
            Map<String, Integer> mapPk = new HashMap<String, Integer>();
            for (int i = 0;i < pks.length;i++) {
            	mapPk.put(pks[i], i);
            }
            String[] fields = insertFields.split(",");
            for (int i = 0;i < fields.length;i++) {
            	if (i == 0) {
            		sql = sql + "?";
            	}else {
            		sql = sql + ",?";
            	}
            }
            sql = sql + ")";
            preparedStatement = conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    @Override
    public void stop() {
        super.stop();
        try {
			close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }

    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;
        String content;

        List<Map<String, Object>> infos = Lists.newArrayList();
        transaction.begin();
        try {
            for (int i = 0; i < batchSize; i++) {
                event = channel.take();
                if (event != null) {//对事件进行处理
                    //event 的 body 为   "exec tail$i , abel"
                    content = new String(event.getBody());
                    content = content.replaceAll("\"\"", "\"");
                    content = content.substring(1, content.length() - 1);
                    Map<String, Object> info = new HashMap<String, Object>();
                    if (content.contains("$$$$$")) {
                    	String[] fields = insertFields.split(",");
                    	Map<String, Object> map = (Map<String, Object>)JSON.parse(content.split("\\$\\$\\$\\$\\$")[1]);
                    	for (int f = 0;f < fields.length;f++) {
                    		info.put(fields[f], map.get(fields[f]));
                    	}
                    }else{
                        info.put("1", content);
                    }
                    infos.add(info);
                } else {
                    result = Status.BACKOFF;
                    break;
                }
            }

            if (infos.size() > 0) {
            	preparedStatement.clearBatch();
        		String[] arrFields = insertFields.split(",");
        		int i = 0;
        		for (Map<String, Object> map : infos) {
        			i = i + 1;
        			for (int f = 0;f < arrFields.length;f++) {
        				String key = arrFields[f];
        				Object value = map.get(key);
        				if (value instanceof Date) {
        					preparedStatement.setDate(f + 1, (Date) value);
        				}else if (value instanceof String) {
        					preparedStatement.setString(f + 1, (String) value);
        				}else {
        					preparedStatement.setObject(f + 1, value);
        				}
        			}
                	preparedStatement.addBatch();
                	//每100条提交一次
                	if (i % 100 == 0) {
                        preparedStatement.executeBatch();
                        conn.commit();
                        preparedStatement.clearBatch();
                	}
        		}
        		//解决最后100余数的问题
                if (i % 100 != 0) {
	                preparedStatement.executeBatch();
	                conn.commit();
                }
                System.out.println("向数据表" + tableName + "中，成功插入" + infos.size() + " 条数据！");
            }
            transaction.commit();
            
        } catch (Exception e) {
        	try {
                transaction.rollback();
            } catch (Exception e2) {
                LOG.error("Exception in rollback. Rollback might not have been" +
                        "successful.", e2);
            }
            LOG.error("Failed to commit transaction." +
                    "Transaction rolled back.", e);
            Throwables.propagate(e);
        } finally {
            transaction.close();
        }
        return result;
    }
}
