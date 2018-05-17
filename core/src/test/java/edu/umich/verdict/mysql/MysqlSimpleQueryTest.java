package edu.umich.verdict.mysql;

import java.io.FileNotFoundException;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;
import edu.umich.verdict.TestBase;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

/**
 * @author chenmo.cm
 * @date 2018/5/14 上午12:51
 */
public class MysqlSimpleQueryTest extends TestBase {

    @BeforeClass
    public static void connect() throws VerdictException, SQLException, FileNotFoundException, ClassNotFoundException {
        VerdictConf conf = MysqlTestUtil.getVerdictConf();

        vc = VerdictJDBCContext.from(conf);
    }

    @Test
    public void query_0() throws VerdictException {
        //String sql = "create 1% sample of lineitem)";
        //vc.executeJdbcQuery(sql);

        String sql = "select l_returnflag, l_linestatus, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price from lineitem where l_shipdate <= '1998-12-01' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus";
        vc.executeJdbcQuery(sql);
    }

    @Test
    public void query_1() throws VerdictException {
        //String sql = "create 1% sample of lineitem)";
        //vc.executeJdbcQuery(sql);

        String sql = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as "
            + "sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - "
            + "l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as "
            + "avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= "
            + "'1998-12-01' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus";
        vc.executeJdbcQuery(sql);
    }

}
