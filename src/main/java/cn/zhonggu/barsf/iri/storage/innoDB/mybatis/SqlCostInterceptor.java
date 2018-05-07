package cn.zhonggu.barsf.iri.storage.innoDB.mybatis;

import com.mysql.jdbc.Statement;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.*;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ZhuDH on 2018/4/20.
 */
@Intercepts({
//        @Signature(type = StatementHandler.class, method = "query", args = {Statement.class, ResultHandler.class}),
        @Signature(type = StatementHandler.class, method = "update", args = {Statement.class}),
//        @Signature(type = StatementHandler.class, method = "batch", args = {Statement.class})
//        @Signature(type = Executor.class,method = "commit",args = {boolean.class}),
        @Signature(type = Executor.class,method = "update",args = {MappedStatement.class,Object.class})
})
public class SqlCostInterceptor implements Interceptor {
    private static long lastTime = System.currentTimeMillis();
    private static AtomicInteger processCount = new AtomicInteger(0);

    @Override
    public Object intercept(Invocation invocation) throws Throwable {

        long startTime = System.currentTimeMillis();
        try {
            return invocation.proceed();
        } finally {
            long endTime = System.currentTimeMillis();
            if (endTime - lastTime > 1000) {
                System.out.println("提交频率 : [" + ((double) processCount.get()) / (endTime - lastTime) + "r/ms ] ");
                lastTime = endTime;
                processCount.set(0);
            }
            long sqlCost = endTime - startTime;
            System.out.println("提交耗时 : [" + sqlCost + "ms ] ");
        }
    }

    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {

    }
}
