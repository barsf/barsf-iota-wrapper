package cn.zhonggu.barsf.iri.storage.innoDB.mybatis.subProvider;

import cn.zhonggu.barsf.iri.modelWrapper.Kv;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.modelMapper.KvMapper;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

/**
 * Created by ZhuDH on 2018/4/11.
 */
public class KvProvider {
    private static volatile KvProvider instance = null;
    private final SqlSessionFactory sessionFactory;

    private KvProvider() {
        this.sessionFactory = DbHelper.getSingletonSessionFactory();
    }

    public static KvProvider getInstance() {
        synchronized (KvProvider.class) {
            if (instance == null) {
                instance = new KvProvider();
            }
        }
        return instance;
    }


    public Kv getValue(String key) {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            KvMapper mapper = session.getMapper(KvMapper.class);
            return mapper.selectByPrimaryKey(key);
        }
    }

    public int saveValue(String key, String value, SqlSession session) {
        if (session == null) {
            try (final SqlSession innerSession = this.sessionFactory.openSession(true)) {
                KvMapper mapper = innerSession.getMapper(KvMapper.class);
                return mapper.insertOrUpdate(new Kv(key, value));
            }
        } else {
            KvMapper mapper = session.getMapper(KvMapper.class);
            return mapper.insertOrUpdate(new Kv(key, value));
        }
    }
}
