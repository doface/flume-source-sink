package com.doface.sink;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.conf.Configurable;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class DofaceHiveSink extends AbstractSink implements Configurable {
	private Logger LOG = LoggerFactory.getLogger(DofaceHiveSink.class);
    private String connectionUrl;
    private String connectionDriver;
    private String tableName;
    private String insertFields;
    private String primaryKey;
    private String user;
    private Connection conn;
    private int batchSize;
    private Statement stat;
    private String fgflx;
    private String blx;
    private List<String> tableFields;
    
    public DofaceHiveSink() {
        LOG.info("DofaceHiveSink start...");
    }

    public void configure(Context context) {
    	connectionUrl = context.getString("connection.url");
        Preconditions.checkNotNull(connectionUrl, "connection.url must be set!!");
    	connectionDriver = context.getString("connection.driver");
        Preconditions.checkNotNull(connectionDriver, "connection.driver must be set!!");
        tableName = context.getString("tableName");
        insertFields = context.getString("insert_fields");
        fgflx = context.getString("fgflx");
        blx = context.getString("blx");
        primaryKey = context.getString("primaryKey", "");
        Preconditions.checkNotNull(tableName, "tableName must be set!!");
        user = context.getString("connection.user");
        batchSize = context.getInteger("batchSize", 10000);
        Preconditions.checkNotNull(batchSize > 0, "batchSize must be a positive number!!");
        tableFields = new ArrayList<String>();
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
            conn = DriverManager.getConnection(url, user, "");
            stat = conn.createStatement();
            //获取hive表的表结构
            ResultSet res = stat.executeQuery("describe " + tableName);
            while (res.next()) {
            	tableFields.add(res.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }
    
    /**
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
    public void stop() {
        super.stop();
        try {
			close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }
    
    /**
     * @param infos 数据信息
     * 创建写入到hive的文件
     */
    public void createFile(String filePath , List<Map<String, Object>> infos) {
		 try {
			 File file = new File(filePath); // 先New出一个文件来
			 FileOutputStream fos = new FileOutputStream(file); // 然后再New出一个文件输出流，	
	         StringBuffer sb = new StringBuffer();
	         for(Map<String, Object> arg : infos){
	        	 for (int i = 0;i < tableFields.size();i++) {
	        		 String key = tableFields.get(i);
	        		 String value = (String) arg.get(key);
	 				 if(fgflx != null) {
	 					sb.append(value).append(fgflx);
	 				 }else {
	 					sb.append(value).append("\t");
	 				 }
	        	 }
	 			 sb.deleteCharAt(sb.length() - 4);//去掉最后一个分隔符
	 			 sb.append("\n");
	 		 }
	         sb.deleteCharAt(sb.length() - 2);//去掉最后一个换行符
	         byte[] contents =  sb.toString().getBytes();
	         fos.write(contents);
	         fos.close();
	         fos.close();
		 } catch (IOException e) {
			 LOG.error(" creatFile error: ",e);
		 }       
		 LOG.info("文件创建成功！");   	
    }
    
    public String loadDataHive(String hivePath,String key,String tableName) {
    	String sql = "load data inpath '" + hivePath + "' into table "+tableName+"";	
    	if(!"".equals(key)) {
    		sql = sql +" "+ key;
    	}
    	System.out.println("SQL语句：" + sql);
    	try {
			stat.execute(sql);
		} catch (SQLException e) {
			LOG.error(" load data error: ",e);
		}
		LOG.info("loadData到Hive表成功！");
    	return "ok";
    }
    
	@SuppressWarnings({ "unchecked", "static-access" })
	@Override
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
                    if (content.contains("$$$$$")) {
                    	Map<String, Object> map = (Map<String, Object>)JSON.parse(content.split("\\$\\$\\$\\$\\$")[1]);
                    	infos.add(map);
                    }else{
                    	Map<String, Object> map = new HashMap<String, Object>();
                    	map.put("content", content);
                    	infos.add(map);
                    }
                } else {
                    result = Status.BACKOFF;
                    break;
                }
            }            
            if (infos.size() > 0) {
            	String ret = "";
            	DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        		Calendar calendar = Calendar.getInstance();
        		String dateName = df.format(calendar.getTime());
        		if("fqb".equals(blx) && !"".equals(primaryKey)) {
        			//获取插入表主键primakey在insertFields中的位置
                    String[] primarys = primaryKey.split(",");
                    Map<String, String> primaryIndex = new HashMap<String, String>();
                    int pcount = 0;
                	for (int p = 0;p < primarys.length;p++) {
            			primaryIndex.put(primarys[p], primarys[p]);
            			pcount++;
                	}
                    if(pcount == 0) {
                    	LOG.error("没有找到分区字段！",primaryKey);
      				   	result = Status.BACKOFF;
                        return result;
                    }
                    Map<String, Object> newInfoMap = new HashMap<String, Object>();
                    //分割infos成一个大的Map，其中key是主键的分区字段，value是infos中对应的列表数据
    	            for (Map<String, Object> info : infos) {
    	            	String partion = "partition (";
    	            	//循环所有的分区主键
    	            	for (String key : primaryIndex.keySet()) {
    	            		if (info.containsKey(key)) {
    	            			String value = (String) info.get(key);
    	            			if(value != null && !"".equals(value) && value.replaceAll("[\u4e00-\u9fa5]*[a-z]*[A-Z]*\\d*-*_*\\s*", "").length() == 0){
    	            				if ("partition (".equals(partion)) {
        	            				partion = partion + key + "='" + value + "'";
        	            			}else {
        	            				partion = partion + "," + key + "='" + value + "'";
        	            			}
	            			    }else {
	            				   LOG.error("分区字段不允许为空或包含特殊字符！",value);
	            				   result = Status.BACKOFF;
	                               return result;
	            			    }          	            			
    	            		}
    	            	}
    	            	partion = partion + ")";
    	            	System.out.println(partion);
    	            	List<Map<String, Object>> newInfos = new ArrayList<Map<String, Object>>();
    	            	if (newInfoMap.containsKey(partion)) {
    	            		newInfos = (List<Map<String, Object>>) newInfoMap.get(partion);
    	            	}
    	            	newInfos.add(info);
    	            	newInfoMap.put(partion, newInfos);
    	            }
                    //循环加载文件写入到hive
                    String filePath = "/tmp/" + tableName + "-" + dateName;
                    int count = 0;
                    for(String key : newInfoMap.keySet()) {
                    	//创建写入到hive的文件
                    	createFile(filePath + "-" + count  + ".txt", (List<Map<String, Object>>)newInfoMap.get(key));
                    	executecmd("hadoop fs -put " + filePath + "-" + count  + ".txt" + " /tmp/");
                    	executecmd("rm -rf " + filePath + "-" + count  + ".txt");
                    	//load结果
	                	ret = loadDataHive(filePath + "-" + count  + ".txt", key, tableName);
	                	if (!"ok".equals(ret)) {
	                    	LOG.error(ret);
	                    }
                    	count++;
                    }
            	}else {
            		String filePath = "/tmp/" + tableName + ".txt";
            		executecmd("rm -rf " + filePath);
                    //创建写入到hive的文件
                	createFile(filePath, infos);
                	executecmd("hadoop fs -put " + filePath + " /tmp/");
                	//load结果
                	ret = loadDataHive(filePath,"",tableName);
                	if (!"ok".equals(ret)) {
                    	LOG.error(ret);
                    }
            	}
        		LOG.info("向数据表" + tableName + "中，成功插入" + infos.size() + " 条数据！");
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
	public String executecmd(String cmd) throws Exception {
		StringBuilder result = new StringBuilder();

        Process process = null;
        BufferedReader bufrIn = null;
        BufferedReader bufrError = null;

        try {
            // 执行命令, 返回一个子进程对象（命令在子进程中执行）
            process = Runtime.getRuntime().exec(cmd);

            // 方法阻塞, 等待命令执行完成（成功会返回0）
            process.waitFor();

            // 获取命令执行结果, 有两个结果: 正常的输出 和 错误的输出（PS: 子进程的输出就是主进程的输入）
            bufrIn = new BufferedReader(new InputStreamReader(process.getInputStream(), "UTF-8"));
            bufrError = new BufferedReader(new InputStreamReader(process.getErrorStream(), "UTF-8"));

            // 读取输出
            String line = null;
            while ((line = bufrIn.readLine()) != null) {
                result.append(line).append('\n');
            }
            while ((line = bufrError.readLine()) != null) {
                result.append(line).append('\n');
            }

        } finally {
            closeStream(bufrIn);
            closeStream(bufrError);

            // 销毁子进程
            if (process != null) {
                process.destroy();
            }
        }

        // 返回执行结果
        return result.toString();
	}

	private static void closeStream(Closeable stream) {
        if (stream != null) {
            try {
                stream.close();
            } catch (Exception e) {
                // nothing
            }
        }
    }
}
