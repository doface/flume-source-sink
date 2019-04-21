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
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yangyibo on 17/1/5.
 */
public class DofaceOracleSink extends AbstractSink implements Configurable {

    private Logger LOG = LoggerFactory.getLogger(DofaceOracleSink.class);
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
    private String insertSql;
    private Map<String, String> fieldType;

    public DofaceOracleSink() {
        LOG.info("DofaceOracleSink start...");
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
        batchSize = context.getInteger("batchSize", 100);
        Preconditions.checkNotNull(batchSize > 0, "batchSize must be a positive number!!");
        fieldType = new HashMap<String, String>();
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
        ResultSet rs = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
            DatabaseMetaData dbmd = conn.getMetaData(); 
            rs = dbmd.getColumns(conn.getCatalog(), "%", tableName.toUpperCase(), "%");
            while(rs.next()) { 
            	String columnName = rs.getString("COLUMN_NAME"); 
            	String columnType = rs.getString("TYPE_NAME");
            	fieldType.put(columnName, columnType);
            }
            
            //创建一个Statement对象
            String sql = "insert into " + tableName +
                    " (" + insertFields + ") values (";
            insertSql = sql;
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
            	if (fieldType.containsKey(fields[i].trim().toUpperCase())) {
            		fieldType.put(String.valueOf(i + 1), fieldType.get(fields[i].trim().toUpperCase()));
            	}else {
            		System.out.println(fields[i].trim().toUpperCase() + " 字段未包含在数据结果集中！请核实insert_fields");
            	}
            }
            primaryKeyIndexs = primaryKeyIndexs.substring(0, primaryKeyIndexs.length() - 1);
            sql = sql + ")";
            preparedStatement = conn.prepareStatement(sql);
            String delSql = "delete from " + tableName + " where 1 = 1 ";
            String[] pki = primaryKeyIndexs.split(",");
            for (int i = 0;i < pki.length;i++) {
            	String ft = fieldType.get(fields[Integer.parseInt(pki[i])].trim().toUpperCase()).toString();
            	if (ft.contains("TIMESTAMP") || ft.contains("DATE")) {
            		delSql = delSql + " and to_date(" + fields[Integer.parseInt(pki[i])] + ",'YYYY-MM-DD HH24:MI:SS')=to_date(?,'YYYY-MM-DD HH24:MI:SS')";
            	}else {
            		delSql = delSql + " and " + fields[Integer.parseInt(pki[i])] + " = ?";
            	}
            }
            preparedStatementDel = conn.prepareStatement(delSql);
            
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }finally {
        	if (rs != null) {
        		try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
        	}
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
        if (preparedStatementDel != null) {
            try {
            	preparedStatementDel.close();
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
                    	for (int f = 0;f < fields.length;f++) {
                    		info.put(String.valueOf(f + 1), map.get(fields[f]));
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
                preparedStatementDel.clearBatch();
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
//                		System.out.println(s + "=====" + String.valueOf(temp.get(s)));
                		String fieldValue = String.valueOf(temp.get(s));
                		if (temp.get(s) == null) {
                			fieldValue = "";
                		}
                		if (fieldValue == null || "".equals(fieldValue)) {
                			preparedStatement.setObject(Integer.parseInt(s), null);
                			if (ispk != -1) {
                				preparedStatementDel.setString(ispk + 1, "");
                			}
                			continue;
                		}
                		if (fieldType.get(s).contains("TIMESTAMP")) {
                			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                			java.util.Date d = df.parse(fieldValue);
                	        String time = df.format(d); 
                	        Timestamp ts = Timestamp.valueOf(time);
                			preparedStatement.setTimestamp(Integer.parseInt(s), ts);
                			if (ispk != -1) {
                				preparedStatementDel.setString(ispk + 1, time);
                			}
//                			System.out.println(s + "===== Timestamp");
                		}else if (fieldType.get(s).contains("DATE")) {
                            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            java.util.Date d = SwordDateUtils.parseDate(fieldValue).getTime();
                            String time = df.format(d); 
                            preparedStatement.setDate(Integer.parseInt(s),new Date(d.getTime()));
                			if (ispk != -1) {
                				preparedStatementDel.setString(ispk + 1, time);
                			}
//                			System.out.println(s + "===== Date");
                		}else if (fieldType.get(s).contains("NUMBER")) {
                			if (fieldType.get(s).contains(",")) {
	                			preparedStatement.setDouble(Integer.parseInt(s), Double.valueOf(fieldValue));
	                			if (ispk != -1) {
	                				preparedStatementDel.setDouble(ispk + 1, Double.valueOf(fieldValue));
	                			}
                			}else {
                				preparedStatement.setInt(Integer.parseInt(s), Integer.valueOf(fieldValue));
	                			if (ispk != -1) {
	                				preparedStatementDel.setInt(ispk + 1, Integer.valueOf(fieldValue));
	                			}
                			}
//                			System.out.println(s + "===== Integer");
                		}else if (fieldType.get(s).contains("LONG")) {
                			preparedStatement.setLong(Integer.parseInt(s), Long.valueOf(fieldValue));
                			if (ispk != -1) {
                				preparedStatementDel.setLong(ispk + 1, Long.valueOf(fieldValue));
                			}
//                			System.out.println(s + "===== Long");
                		}else if (temp.get(s) instanceof BigDecimal) {
                			preparedStatement.setBigDecimal(Integer.parseInt(s), BigDecimal.valueOf(Long.valueOf(fieldValue)));
                			if (ispk != -1) {
                				preparedStatementDel.setBigDecimal(ispk + 1, BigDecimal.valueOf(Long.valueOf(fieldValue)));
                			}
//                			System.out.println(s + "===== BigDecimal");
                		}else if (temp.get(s) instanceof Blob) {
                			preparedStatement.setBlob(Integer.parseInt(s), (Blob) temp.get(s));
                			if (ispk != -1) {
                				preparedStatementDel.setBlob(ispk + 1, (Blob) temp.get(s));
                			}
//                			System.out.println(s + "===== Blob");
                		}else if (temp.get(s) instanceof Clob) {
                			preparedStatement.setClob(Integer.parseInt(s), (Clob) temp.get(s));
                			if (ispk != -1) {
                				preparedStatementDel.setClob(ispk + 1, (Clob) temp.get(s));
                			}
//                			System.out.println(s + "===== Clob");
                		}else {
                			preparedStatement.setString(Integer.parseInt(s), fieldValue);
                			if (ispk != -1) {
                				preparedStatementDel.setString(ispk + 1, fieldValue);
                			}
//                			System.out.println(s + "===== String");
                		}
                		
                	}
                	preparedStatement.addBatch();
                	preparedStatementDel.addBatch();
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
