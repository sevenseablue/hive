package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.log4j.Logger;

import java.util.*;

public class UserDbLoc {
    private static final Logger log = Logger.getLogger(UserDbLoc.class);
    private static Map<String, HashSet<String>> user2Locs = new HashMap<>();
    private static long updateTime = System.currentTimeMillis() - 60*1000;

    public static String getPath(String path){
        if(path.startsWith("viewfs://") || path.startsWith("hdfs://")){
            int i1 = path.indexOf("//");
            int i2 = path.indexOf("/", i1+2);
            if (i2>0){
                path = path.substring(i2);
            }
        }

        return path;
    }

    public static void updateDirs(){
        if ((System.currentTimeMillis() - updateTime) < 60*1000){
            return;
        }
        Map<String, HashSet<String>> user2LocsPend = new HashMap<>();
        //TODO 1. imetastoreclient, getalldatabases, getdatabase(name)
        //     2. thrifthivemetastore add api getalldatabase
        //     3. rawstore, objectstore
        RawStore rawStore = HiveMetaStore.HMSHandler.getRawStore();
        List<Database> databaseList = rawStore.getAllDatabase();
        databaseList.stream().forEach(x-> {
            HashSet<String> dirs = user2LocsPend.getOrDefault(x.getOwnerName(), new HashSet<>());
            dirs.add(getPath(x.getLocationUri()));
            user2LocsPend.put(x.getOwnerName(), dirs);
        });

        for(Map.Entry<String, HashSet<String>> entry:user2LocsPend.entrySet()){
            entry.getValue().add("/user/"+entry.getKey());
        }

        updateTime = System.currentTimeMillis();
        user2Locs = user2LocsPend;
        log.info(String.format("####HdfsDir####update####%s", user2Locs));

    }

    public static boolean existsUserLocs(String user, String loc) {
        if (existsUserLocWithoutUpdate(user, loc)){
            return true;
        }else{
            updateDirs();
            return existsUserLocWithoutUpdate(user, loc);
        }
    }

    public static boolean existsUserLocWithoutUpdate(String user, String loc) {
        String loc1 = getPath(loc);
        if (user2Locs.get(user) != null){
            HashSet<String> locs = user2Locs.get(user);
            for(String locT: locs){
                locT = locT.endsWith("/")?locT:locT+"/";
                locT = locT+".*";
                if (loc1.matches(locT)){
                    return true;
                }
            }
        }
        return false;
    }

    public static void main(String[] args) {
        System.out.println(UserDbLoc.getPath("viewfs://qunarcluster/user"));
    }

}