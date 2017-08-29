package com.hortonworks.support;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;


/**
 * Created by jchen on 21/08/2017.
 */
public class create_dir {
    private static final Logger LOGGER = Logger.getLogger(create_dir.class.getName());


    public static final String KEYTAB_FILE_KEY = "hdfs.keytab.file";
    public static final String USER_NAME_KEY = "hdfs.kerberos.principal";


    private static FileSystem fs;
    private static String sysUri;
    private static String sysrootDir;
    private static int syswidth;
    private static int sysdepth;
    private static ForkJoinPool forkJoinPool;

    private static boolean product = false;
    private static class CreateTask extends RecursiveTask<Void>
    {
        private int width;
        private Path parent;
        private int depth;
        private int cur;

        public CreateTask(Path parent,int width, int depth,int cur){
            this.width=width;
            this.parent=parent;
            this.depth=depth;
            this.cur=cur;
        }
        @Override
        protected Void compute() {
            try {

                if (cur >= depth) {
                    LOGGER.info(parent.toString());
                    Path path = new Path(parent, "test");
                    if (product) {
                        fs.mkdirs(parent);
                        fs.createNewFile(path);
                    }
                } else {
                    List<CreateTask> list = new ArrayList();
                    for (int i = 1; i <= width; i++) {
                        CreateTask subTask = new CreateTask(new Path(parent, String.valueOf(i)), width, depth, cur + 1);
                        list.add(subTask);
                    }
                    invokeAll(list);
                }
            }catch (Exception ex){
                ex.printStackTrace();
            }

            return null;
        }
    }


    public static void login(Configuration hdfsConfig) throws IOException {

        if (UserGroupInformation.isSecurityEnabled()) {
            String keytab = hdfsConfig.get(KEYTAB_FILE_KEY);
            if (keytab == null) {
                keytab="/etc/security/keytabs/hdfs.headless.keytab";
                hdfsConfig.set(KEYTAB_FILE_KEY, keytab);
                LOGGER.warn("using keytab:"+keytab);
            }
            String userName = hdfsConfig.get(USER_NAME_KEY);
            if (userName == null) {
                userName="hdfs@CLUSTER.MVQ";
                LOGGER.warn("using principal:"+userName);
                hdfsConfig.set(USER_NAME_KEY, userName);
            }
            SecurityUtil.login(hdfsConfig, KEYTAB_FILE_KEY, USER_NAME_KEY);
        }
    }

    public static void main(String[] args) {

        PropertyConfigurator.configure("conf/log4j.properties");
        for (String arg:args){
            LOGGER.info(arg);
        }

        int index = 0;

        sysUri=args[index++];
        sysrootDir=args[index++];
        forkJoinPool = new ForkJoinPool(Integer.parseInt(args[index++]));
        syswidth=Integer.parseInt(args[index++]);
        sysdepth=Integer.parseInt(args[index++]);
        if (args.length > (index)){
            if (Integer.parseInt(args[index])  > 0){
                product=true;
            }
        }

        try {

            if (product) {
                Configuration configuration = new Configuration();
                Properties properties = new Properties();
                try {
                    properties.load(new FileReader("conf/auth.properties"));
                    Iterator<Map.Entry<Object, Object>> it = properties.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry<Object, Object> entry = it.next();
                        Object key = entry.getKey();
                        Object value = entry.getValue();
                        LOGGER.info(key.toString() + ":" + value.toString());
                        configuration.set(key.toString(), value.toString());
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }

                try {
                    login(configuration);
                    fs = FileSystem.get(URI.create(sysUri), configuration);
                    CreateTask task = new CreateTask(new Path(sysrootDir), syswidth, sysdepth, 0);
                    Future<Void> wait = forkJoinPool.submit(task);
                    wait.get();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            forkJoinPool.shutdown();
        }
    }
}
