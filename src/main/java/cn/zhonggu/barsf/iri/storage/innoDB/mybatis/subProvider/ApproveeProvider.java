package cn.zhonggu.barsf.iri.storage.innoDB.mybatis.subProvider;

import cn.zhonggu.barsf.iri.modelWrapper.ApproveeWrapper;
import cn.zhonggu.barsf.iri.modelWrapper.TransactionWrapper;
import cn.zhonggu.barsf.iri.storage.innoDB.NeededException;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.modelMapper.TransactionMapper;
import com.iota.iri.model.Approvee;
import com.iota.iri.model.Hash;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.utils.Pair;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tk.mybatis.mapper.entity.Example;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper.converterIndexableToStr;


/**
 * Created by ZhuDH on 2018/3/30.
 */
public class ApproveeProvider implements SubPersistenceProvider {
    private static Logger log = LoggerFactory.getLogger(ApproveeProvider.class);

    private static volatile ApproveeProvider instance = null;
    private final SqlSessionFactory sessionFactory;

    private ApproveeProvider() {
        this.sessionFactory = DbHelper.getSingletonSessionFactory();
    }

    public static ApproveeProvider getInstance() {
        synchronized (ApproveeProvider.class) {
            if (instance == null) {
                instance = new ApproveeProvider();
            }
        }
        return instance;
    }

    public boolean save(Persistable thing, Indexable index, SqlSession outSession, boolean mergeMode, AtomicLong al) {
        // saveBatch使用(无意义), 不报错即为true
        return true;
    }

    public boolean save(Persistable thing, Indexable index, SqlSession outSession, boolean mergeMode) {
        // 使用数据库关系,  直接从trans中获取, 因此也不需要占用这个保存数据
        return save(thing, index, outSession, mergeMode, new AtomicLong());
    }

    public boolean save(byte[] thing, Indexable index, SqlSession outSession, boolean mergeMode) throws Exception {
        // 未曾使用
        throw new NeededException();
    }

    public boolean save(Persistable thing, Indexable index) {
        throw new NeededException();
    }

    public void delete(Indexable index) throws Exception {
    }

    public boolean update(Persistable model, Indexable index, String item) throws Exception {
        // 当前版本没有对tag的直接更新
        throw new NeededException();
    }

    public boolean update(Persistable model, Indexable index, String item, boolean merge, SqlSession session) throws Exception {
        // 当前版本没有对Approvee的直接更新
        throw new NeededException();
    }

    public boolean exists(Indexable key) throws Exception {
        // 只有trans/milestone使用
        throw new NeededException();
    }

    public boolean exists(Indexable key, SqlSession session) throws Exception {
        throw new NeededException();
    }

    public Pair<Indexable, Persistable> latest(Class<?> indexModel) throws Exception {
        // 只有里程碑使用
        throw new NeededException();
    }

    
    public Set<Indexable> keysWithMissingReferences(Class<?> otherClass) throws Exception {
        // 至今未用
        throw new NeededException();
    }

    
    public Persistable getFromTransaction(Indexable index) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
            Example selectHash = new Example(TransactionWrapper.class);
            selectHash.selectProperties("hash");
            selectHash.createCriteria().andEqualTo("trunk", converterIndexableToStr(index))
                    .orEqualTo("branch", converterIndexableToStr(index));
            List<TransactionWrapper> hashOfTransByTag = mapper.selectByExample(selectHash);
            if (hashOfTransByTag.size() > 0) {
                return new ApproveeWrapper(hashOfTransByTag.stream().map(t -> new Hash(t.getHash())).collect(Collectors.toList())).toApprovee();
            } else {
                return Approvee.class.newInstance();
            }
        }
    }

    
    public boolean mayExist(Indexable index) throws Exception {
        return exists(index);
    }

    
    public long count() throws Exception {
        // 只有trans使用
        throw new NeededException();
    }

    
    public Set<Indexable> keysStartingWith(byte[] value) {
        throw new NeededException();
    }

    
    public Persistable seek(byte[] key) throws Exception {
        // 只有trans使用
        throw new NeededException();
    }

    
    public Pair<Indexable, Persistable> next(Indexable index) throws Exception {
        // rescrn操作使用, 实际什么都不用做
        return new Pair<>(null, null);
    }

    
    public Pair<Indexable, Persistable> previous(Indexable index) throws Exception {
        // 只有milestone使用
        throw new NeededException();
    }

    
    public Pair<Indexable, Persistable> first(Class<?> indexModel) throws Exception {
        // rescrn操作使用, 实际什么都不用做
        return new Pair<>(null, null);
    }
}
