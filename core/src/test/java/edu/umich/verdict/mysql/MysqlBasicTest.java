package edu.umich.verdict.mysql;

import java.io.FileNotFoundException;
import java.sql.SQLException;

import edu.umich.verdict.BasicTest;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;
import org.junit.BeforeClass;

/**
 * @author chenmo.cm
 * @date 2018/5/13 下午10:46
 */
public class MysqlBasicTest extends BasicTest {
    @BeforeClass
    public static void connect() throws VerdictException, SQLException, FileNotFoundException, ClassNotFoundException {
        VerdictConf conf = MysqlTestUtil.getVerdictConf();

        vc = VerdictJDBCContext.from(conf);
    }
}
