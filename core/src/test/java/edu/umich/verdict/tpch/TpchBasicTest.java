package edu.umich.verdict.tpch;

import org.junit.Test;
import edu.umich.verdict.TestBase;
import edu.umich.verdict.exceptions.VerdictException;

/**
 * @author chenmo.cm
 * @date 2018/5/13 下午11:36
 */
public abstract class TpchBasicTest extends TestBase {

    @Test
    public void tpchQuery2() throws VerdictException {
        String sql = "select\n" + " s_acctbal, s_name, n_name, p_partkey, p_mfgr,\n"
                     + " s_address, s_phone, s_comment\n" + "from\n" + " part, supplier, partsupp, nation, region\n"
                     + "where\n" + " p_partkey = ps_partkey\n" + " and s_suppkey = ps_suppkey\n" + " and p_size = 15\n"
                     + " and p_type like '%BRASS'\n" + " and s_nationkey = n_nationkey\n"
                     + " and n_regionkey = r_regionkey\n" + " and r_name = 'EUROPE'\n" + " and ps_supplycost = (\n"
                     + "  select\n" + "   min(ps_supplycost)\n" + "  from\n" + "   partsupp,\n" + "   supplier,\n"
                     + "   nation,\n" + "   region,\n" + "   part\n" + "  where\n" + "   p_partkey = ps_partkey\n"
                     + "   and s_suppkey = ps_suppkey\n" + "   and s_nationkey = n_nationkey\n"
                     + "   and n_regionkey = r_regionkey\n" + "   and r_name = 'EUROPE'\n" + "  )\n" + "order by\n"
                     + " s_acctbal desc,\n" + " n_name,\n" + " s_name,\n" + " p_partkey\n" + "limit 100;\n";
        vc.executeJdbcQuery(sql);
    }
}
