package cn.zhonggu.barsf.iri.storage.innoDB.mybatis.subProvider;

import cn.zhonggu.barsf.iri.modelWrapper.BundleWrapper;
import cn.zhonggu.barsf.iri.modelWrapper.TransactionWrapper;
import cn.zhonggu.barsf.iri.storage.innoDB.NeededException;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.modelMapper.TransactionMapper;
import com.iota.iri.model.Bundle;
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
public class BundleProvider implements SubPersistenceProvider {
    private static Logger log = LoggerFactory.getLogger(BundleProvider.class);


    private static volatile BundleProvider instance = null;
    private final SqlSessionFactory sessionFactory;

    private BundleProvider() {
        this.sessionFactory = DbHelper.getSingletonSessionFactory();
    }

    public static BundleProvider getInstance() {
        synchronized (BundleProvider.class) {
            if (instance == null) {
                instance = new BundleProvider();
            }
        }
        return instance;
    }

    public boolean save(Persistable thing, Indexable index, SqlSession outSession, boolean mergeMode, AtomicLong al) {

//        if (outSession == null) {
//            return save(thing, index);
//        }
//
//        BundleMapper mapper = outSession.getMapper(BundleMapper.class);
//        Bundle target = (Bundle) thing;
//        target.filling(index);
//        mapper.insertOrUpdate(target, mergeMode);

        // saveBatch使用(无意义), 不报错即为true
        return true;
    }

    public boolean save(byte[] thing, Indexable index, SqlSession outSession, boolean mergeMode) throws IllegalAccessException, InstantiationException {
//        BundleMapper mapper = outSession.getMapper(BundleMapper.class);
//        Bundle target = Bundle.class.newInstance();
////        target.read(thing);
//        target.filling(index, thing);
//        mapper.insertOrUpdate(target);

        // 未曾使用
        throw new NeededException();
    }

    public boolean save(Persistable thing, Indexable index, SqlSession outSession, boolean mergeMode) {
        // 使用数据库关系,  直接从trans中获取, 因此也不需要占用这个保存数据
        return save(thing, index, outSession, mergeMode, new AtomicLong());
    }

    public boolean save(Persistable thing, Indexable index) {
        //update使用, 不应该被调用
        throw new NeededException();
    }

    @Override
    public void delete(Indexable index) throws Exception {
        // 只用于缓存清理, 什么都不做就好了
    }

    public boolean update(Persistable model, Indexable index, String item, boolean merge) throws Exception {

      // 当前版本没有对bundle的更新
        throw new NeededException();
    }

    public boolean update(Persistable model, Indexable index, String item) throws Exception {
        throw new NeededException();
    }

    @Override
    public boolean exists(Indexable key) throws Exception {
//        try (final SqlSession session = this.sessionFactory.openSession(true)) {
//            BundleMapper mapper = session.getMapper(BundleMapper.class);
//            return mapper.existsWithPrimaryKey(converterIndexableToStr(key));
//        }
        //
        // 只有trans会查询exists
        throw new NeededException();
    }

    public boolean exists(Indexable key, SqlSession session) throws Exception {
//        BundleMapper mapper = session.getMapper(BundleMapper.class);
//        return mapper.existsWithPrimaryKey(converterIndexableToStr(key));
        // 只有trans会查询exists
        throw new NeededException();
    }

    @Override
    public Pair<Indexable, Persistable> latest(Class<?> indexModel) throws Exception {
//        try (final SqlSession session = this.sessionFactory.openSession(true)) {
//            BundleMapper mapper = session.getMapper(BundleMapper.class);
//            Bundle value = mapper.latest();
//            if (value != null) {
//                // todo 这里没有使用类型创建对象
//                Indexable index = (Indexable) indexModel.newInstance();
//                if (StringUtils.isNumeric(value.getHash())) {
//                    index = new IntegerIndex(Integer.parseInt(value.getHash()));
//                } else {
//                    index = new Hash(value.getHash());
//                }
//                return new Pair<>(index, value);
//            }
//        }
//
//        return new Pair<>(null, null);
        // 只有里程碑使用
        throw new NeededException();
    }

    @Override
    public Set<Indexable> keysWithMissingReferences(Class<?> otherClass) throws Exception {
        // 至今未用
        throw new NeededException();
    }

    @Override
    public Persistable get(Indexable index) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);

            Example selectHash = new Example(TransactionWrapper.class);
            selectHash.selectProperties("hash");
            selectHash.createCriteria().andEqualTo("bundle", converterIndexableToStr(index));
            List<TransactionWrapper> hashOfTransByBundle = mapper.selectByExample(selectHash);
            if (hashOfTransByBundle.size() > 0) {
                return new BundleWrapper(hashOfTransByBundle.stream().map(t -> new Hash(t.getHash())).collect(Collectors.toList())).toBundle();
            } else {
                return Bundle.class.newInstance();
            }
        }
    }

    @Override
    public boolean mayExist(Indexable index) throws Exception {
        // 未用
        throw new NeededException();
    }

    @Override
    public long count() throws Exception {
        // 只有trans使用
        throw new NeededException();
    }

    @Override
    public Set<Indexable> keysStartingWith(byte[] value) {
        throw new NeededException();
    }

    @Override
    public Persistable seek(byte[] key) throws Exception {
        // 只有trans使用
        throw new NeededException();
    }

    @Override
    public Pair<Indexable, Persistable> next(Indexable index) throws Exception {
//        try (final SqlSession session = this.sessionFactory.openSession(true)) {
//            BundleMapper mapper = session.getMapper(BundleMapper.class);
//            Bundle value = mapper.next(converterIndexableToStr(index));
//            if (value != null) {
//                // todo 这里没有使用类型创建对象
//                Indexable indexNext;
//                if (StringUtils.isNumeric(value.getHash())) {
//                    indexNext = new IntegerIndex(Integer.parseInt(value.getHash()));
//                } else {
//                    indexNext = new Hash(value.getHash());
//                }
//                return new Pair<>(indexNext, value);
//            }
//        }
//        return new Pair<>(null, null);

        // rescrn操作使用, 实际什么都不用做
        return new Pair<>(null, null);
    }

    @Override
    public Pair<Indexable, Persistable> previous(Indexable index) throws Exception {
//        try (final SqlSession session = this.sessionFactory.openSession(true)) {
//            BundleMapper mapper = session.getMapper(BundleMapper.class);
//            Bundle value = mapper.previous(converterIndexableToStr(index));
//            if (value != null) {
//                // todo 这里没有使用类型创建对象
//                Indexable indexNext;
//                if (StringUtils.isNumeric(value.getHash())) {
//                    indexNext = new IntegerIndex(Integer.parseInt(value.getHash()));
//                } else {
//                    indexNext = new Hash(value.getHash());
//                }
//                return new Pair<>(indexNext, value);
//            }
//        }
//        return new Pair<>(null, null);
        // 只有milestone使用
        throw new NeededException();
    }

    @Override
    public Pair<Indexable, Persistable> first(Class<?> indexModel) throws Exception {
//        try (final SqlSession session = this.sessionFactory.openSession(true)) {
//            BundleMapper mapper = session.getMapper(BundleMapper.class);
//            Bundle value = mapper.first();
//            if (value != null) {
//                // todo 这里没有使用类型创建对象
//                Indexable index = (Indexable) indexModel.newInstance();
//                if (StringUtils.isNumeric(value.getHash())) {
//                    index = new IntegerIndex(Integer.parseInt(value.getHash()));
//                } else {
//                    index = new Hash(value.getHash());
//                }
//                return new Pair<>(index, value);
//            }
//        }
//        return new Pair<>(null, null);
        // rescrn操作使用, 实际什么都不用做
        return new Pair<>(null, null);
    }
}
