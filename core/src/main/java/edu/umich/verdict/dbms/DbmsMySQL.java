package edu.umich.verdict.dbms;

import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author chenmo.cm
 * @date 2018/5/9 下午12:15
 * @email chenmo.cm@alibaba-inc.com
 */
public class DbmsMySQL extends DbmsJDBC {

    public DbmsMySQL(VerdictContext vc, String dbName, String host, String port, String schema, String user,
                     String password, String jdbcClassName) throws VerdictException {
        super(vc, dbName, host, port, schema, user, password, jdbcClassName);
    }

    @Override
    public String getQuoteString() {
        return "`";
    }

    @Override
    protected String randomPartitionColumn() {
        int pcount = partitionCount();
        return String.format("MOD(ROUND(RAND(UNIX_TIMESTAMP())*%d), %d) AS %s",
            pcount, pcount, partitionColumnName());
    }

    @Override
    protected String randomNumberExpression(SampleParam param) {
        String expr = "RAND(UNIX_TIMESTAMP())";
        return expr;
    }

    @Override
    protected String modOfRand(int mod) {
        return String.format("mod(abs(rand(unix_timestamp())), %d)", mod);
    }

    @Override
    public String modOfHash(String col, int mod) {
        return String.format("crc32(cast(%s%s%s as string)) %% %d", getQuoteString(), col, getQuoteString(), mod);
    }

    @Override
    public String modOfHash(List<String> columns, int mod) {
        String concatStr = "";
        for (int i = 0; i < columns.size(); ++i) {
            String col = columns.get(i);
            String castStr = String.format("cast(%s%s%s as char)", getQuoteString(), col, getQuoteString());
            if (i < columns.size() - 1) {
                castStr += ",";
            }
            concatStr += castStr;
        }
        return String.format("mod(crc32(concat_ws('%s', %s)), %d)", HASH_DELIM, concatStr, mod);
    }

    @Override
    public Dataset<Row> getDataset() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    String composeUrl(String dbms, String host, String port, String schema, String user, String password)
        throws VerdictException {
        StringBuilder url = new StringBuilder();
        url.append(String.format("jdbc:%s://%s:%s", dbms, host, port));

        if (schema != null) {
            url.append(String.format("/%s", schema));
        }

        boolean first = true;

        if (!vc.getConf().ignoreUserCredentials() && user != null && user.length() != 0) {
            first = addQuestionMark(url, first);
            url.append(String.format("user=%s", user));
        }
        if (!vc.getConf().ignoreUserCredentials() && password != null && password.length() != 0) {
            first = addQuestionMark(url, first);
            url.append(String.format("password=%s", password));
        }

        // pass other configuration options.
        for (Map.Entry<String, String> pair : vc.getConf().getConfigs().entrySet()) {
            String key = pair.getKey();
            String value = pair.getValue();

            if (key.startsWith("verdict") || key.equals("user") || key.equals("password")) {
                continue;
            }

            first = addQuestionMark(url, first);
            url.append(String.format("%s=%s", key, value));
        }

        return url.toString();
    }

    private boolean addQuestionMark(StringBuilder url, boolean first) {
        if (first) {
            url.append("?");
            first = false;
        } else {
            url.append("&");
        }

        return first;
    }

    @Override
    public void createMetaTablesInDBMS(TableUniqueName originalTableName, TableUniqueName sizeTableName,
                                       TableUniqueName nameTableName) throws VerdictException {
        VerdictLogger.debug(this, "Creates meta tables if not exist.");
        String sql = String.format("CREATE TABLE IF NOT EXISTS %s", sizeTableName) + " (schemaname VARCHAR(120), "
            + " tablename VARCHAR(120), " + " samplesize BIGINT, " + " originaltablesize BIGINT)";
        executeUpdate(sql);

        sql = String.format("CREATE TABLE IF NOT EXISTS %s", nameTableName) + " (originalschemaname VARCHAR(120), "
            + " originaltablename VARCHAR(120), " + " sampleschemaaname VARCHAR(120), "
            + " sampletablename VARCHAR(120), " + " sampletype VARCHAR(120), " + " samplingratio float, "
            + " columnnames VARCHAR(120))";
        executeUpdate(sql);
        VerdictLogger.debug(this, "Meta tables created.");

        VerdictLogger.debug(this, "Meta tables created.");
        vc.getMeta().refreshTables(sizeTableName.getDatabaseName());
    }

    @Override
    public void insertEntry(TableUniqueName tableName, List<Object> values) throws VerdictException{
        StringBuilder sql = new StringBuilder(1000);
        sql.append(String.format("insert into %s values ", tableName));
        sql.append("(");
        String with = "'";
        sql.append(Joiner.on(", ").join(StringManipulations.quoteString(values, with)));
        sql.append(")");
        executeUpdate(sql.toString());
    }
}
