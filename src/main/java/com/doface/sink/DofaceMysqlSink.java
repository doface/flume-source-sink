package com.doface.sink;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yangyibo on 17/1/5.
 */
public class DofaceMysqlSink extends AbstractSink implements Configurable {

    private Logger LOG = LoggerFactory.getLogger(DofaceMysqlSink.class);
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

    public DofaceMysqlSink() {
        LOG.info("DofaceMysqlSink start...");
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
        Preconditions.checkNotNull(user, "connection.user must be set!!");
        password = context.getString("connection.password");
        Preconditions.checkNotNull(password, "connection.password must be set!!");
        batchSize = context.getInteger("batchSize", 10000);
        Preconditions.checkNotNull(batchSize > 0, "batchSize must be a positive number!!");
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
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
            //创建一个Statement对象
            String sql = "insert into " + tableName +
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
            	if (mapPk.containsKey(fields[i])) {
            		primaryKeyIndexs = primaryKeyIndexs + i + ",";
            	}
            }
            primaryKeyIndexs = primaryKeyIndexs.substring(0, primaryKeyIndexs.length() - 1);
            sql = sql + ")";
            preparedStatement = conn.prepareStatement(sql);
            String delSql = "delete from " + tableName + " where 1 = 1 ";
            String[] pki = primaryKeyIndexs.split(",");
            for (int i = 0;i < pki.length;i++) {
            	delSql = delSql + " and " + fields[Integer.parseInt(pki[i])] + " = ?";
            }
            System.out.println(delSql);
            preparedStatementDel = conn.prepareStatement(delSql);
            
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    @Override
    public void stop() {
        super.stop();
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
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
                    	Map<String, Object> map1 = new HashMap<String, Object>();
                    	for (String key : map.keySet()) {
                    		map1.put(key.toLowerCase(), map.get(key));
                    	}
                    	for (int f = 0;f < fields.length;f++) {
                    		info.put(String.valueOf(f + 1), map1.get(fields[f].trim().toLowerCase()));
                    	}
                    }else{
                        info.put("1", content);
                    }
//                    System.out.println(content);
//                    System.out.println(info);
                    infos.add(info);
                } else {
                    result = Status.BACKOFF;
                    break;
                }
            }

            if (infos.size() > 0) {
                preparedStatement.clearBatch();
                preparedStatementDel.clearBatch();
//                System.out.println("==============================" + primaryKeyIndexs);
                String[] pki = primaryKeyIndexs.split(",");
                int iRow = 0;
                for (Map<String, Object> temp : infos) {
                	iRow = iRow + 1;
                	for (String s : temp.keySet()) {
                		int ispk = -1;
                		//根据主键的顺序号构建删除重复数据的statement
                		for (int i = 0;i < pki.length;i++) {
                			if (Integer.parseInt(s) == Integer.parseInt(pki[i]) + 1) {
                				ispk = i;
                				break;
                			}
                		}
                		if (temp.get(s) instanceof Timestamp) {
                			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                			java.util.Date d = df.parse(temp.get(s).toString());
                	        String time = df.format(d); 
                	        Timestamp ts = Timestamp.valueOf(time);
                			preparedStatement.setTimestamp(Integer.parseInt(s), ts);
                			if (ispk != -1) {
                				preparedStatementDel.setTimestamp(ispk + 1, ts);
                			}
                		}else if (temp.get(s) instanceof Date) {
                			preparedStatement.setDate(Integer.parseInt(s), Date.valueOf(temp.get(s).toString()));
                			if (ispk != -1) {
                				preparedStatementDel.setDate(ispk + 1, Date.valueOf(temp.get(s).toString()));
                			}
                		}else if (temp.get(s) instanceof Integer) {
                			preparedStatement.setInt(Integer.parseInt(s), Integer.valueOf(temp.get(s).toString()));
                			if (ispk != -1) {
                				preparedStatementDel.setInt(ispk + 1, Integer.valueOf(temp.get(s).toString()));
                			}
                		}else if (temp.get(s) instanceof Long) {
                			preparedStatement.setLong(Integer.parseInt(s), Long.valueOf(temp.get(s).toString()));
                			if (ispk != -1) {
                				preparedStatementDel.setLong(ispk + 1, Long.valueOf(temp.get(s).toString()));
                			}
                		}else if (temp.get(s) instanceof BigDecimal) {
                			preparedStatement.setBigDecimal(Integer.parseInt(s), BigDecimal.valueOf(Long.valueOf(temp.get(s).toString())));
                			if (ispk != -1) {
                				preparedStatementDel.setBigDecimal(ispk + 1, BigDecimal.valueOf(Long.valueOf(temp.get(s).toString())));
                			}
                		}else if (temp.get(s) instanceof Blob) {
                			preparedStatement.setBlob(Integer.parseInt(s), (Blob) temp.get(s));
                			if (ispk != -1) {
                				preparedStatementDel.setBlob(ispk + 1, (Blob) temp.get(s));
                			}
                		}else if (temp.get(s) instanceof Clob) {
                			preparedStatement.setClob(Integer.parseInt(s), (Clob) temp.get(s));
                			if (ispk != -1) {
                				preparedStatementDel.setClob(ispk + 1, (Clob) temp.get(s));
                			}
                		}else {
                			if (temp.get(s) == null) {
                				preparedStatement.setObject(Integer.parseInt(s), temp.get(s));
	                			if (ispk != -1) {
	                				preparedStatementDel.setObject(ispk + 1, temp.get(s));
	                			}
                			}else {
	                			preparedStatement.setString(Integer.parseInt(s), temp.get(s).toString());
	                			if (ispk != -1) {
	                				preparedStatementDel.setString(ispk + 1, temp.get(s).toString());
	                			}
                			}
                		}
                		
                	}
                	preparedStatementDel.addBatch();
                	preparedStatement.addBatch();
                	//每100条提交一次
                	if (iRow % 100 == 0) {
                		preparedStatementDel.executeBatch();
                        preparedStatement.executeBatch();
                        conn.commit();
                        preparedStatementDel.clearBatch();
                        preparedStatement.clearBatch();
                	}
                }
                //解决最后100余数的问题
                if (iRow % 100 != 0) {
	                preparedStatementDel.executeBatch();
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
