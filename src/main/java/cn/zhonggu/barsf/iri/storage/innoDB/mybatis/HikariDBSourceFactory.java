package cn.zhonggu.barsf.iri.storage.innoDB.mybatis;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.ibatis.datasource.DataSourceFactory;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * Created by ZhuDH on 2018/3/29.
 */
public class HikariDBSourceFactory implements DataSourceFactory {
    private static HikariDataSource onlyOneDs;
    private static HikariConfig config;
    @Override
    public void setProperties(Properties props) {
        config = new HikariConfig(props);

        System.out.println("hds info: timeout in:" + config.getConnectionTimeout() + " max pool size:" + config.getMaximumPoolSize() + " min idle:" + config.getMinimumIdle());

    }

    @Override
    public DataSource getDataSource() {
        if (onlyOneDs == null) {
            onlyOneDs = new HikariDataSource(config);
        }
        return onlyOneDs;
    }
}
