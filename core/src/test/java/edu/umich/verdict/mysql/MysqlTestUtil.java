package edu.umich.verdict.mysql;

import edu.umich.verdict.VerdictConf;

/**
 * @author chenmo.cm
 * @date 2018/5/14 上午12:32
 */
public class MysqlTestUtil {
    static VerdictConf getVerdictConf() {
        VerdictConf conf = new VerdictConf();
        conf.setDbms("mysql");
        conf.setHost("127.0.0.1");
        conf.setPort("3306");
        conf.setDbmsSchema("tpch1g");
        conf.setUser("admin");
        conf.set("loglevel", "debug");
        return conf;
    }
}
