package cn.zhonggu.barsf.iri.storage.innoDB.mybatis.subProvider;

import cn.zhonggu.barsf.iri.modelWrapper.StateDiffWrapper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.modelMapper.StateDiffMapper;
import com.iota.iri.model.Hash;
import com.iota.iri.model.IntegerIndex;
import com.iota.iri.model.StateDiff;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.utils.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper.converterIndexableToStr;


/**
 * Created by ZhuDH on 2018/3/30.
 */
public class StateDiffProvider implements SubPersistenceProvider {
    private static Logger LOG = LoggerFactory.getLogger(StateDiffProvider.class);

    private static volatile StateDiffProvider instance = null;
    private final SqlSessionFactory sessionFactory;

    private StateDiffProvider() {
        this.sessionFactory = DbHelper.getSingletonSessionFactory();
    }

    public static StateDiffProvider getInstance() {
        synchronized (StateDiffProvider.class) {
            if (instance == null) {
                instance = new StateDiffProvider();
            }
        }
        return instance;
    }

    public boolean save(Persistable thing, Indexable index, SqlSession outSession, boolean mergeMode) {
        return save(thing, index, outSession, mergeMode, new AtomicLong());
    }

    public boolean save(Persistable thing, Indexable index, SqlSession outSession, boolean mergeMode, AtomicLong al) {

        if (outSession == null) {
            return save(thing, index);
        }

        StateDiffMapper mapper = outSession.getMapper(StateDiffMapper.class);
        StateDiffWrapper target = new StateDiffWrapper((StateDiff) thing);
        target.filling(index);
        mapper.insertOrUpdate(target);
        if (al != null) {
            al.set(target.bytes().length);
        }
        // 不报错即为true
        return true;
    }

    public boolean save(byte[] thing, Indexable index, SqlSession outSession, boolean mergeMode) throws Exception {

        StateDiffMapper mapper = outSession.getMapper(StateDiffMapper.class);
        StateDiffWrapper target = StateDiffWrapper.class.newInstance();
//        target.read(thing);
        target.filling(index, thing);
        mapper.insertOrUpdate(target);

        // 不报错即为true
        return true;
    }

    public boolean save(Persistable thing, Indexable index) {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            StateDiffMapper mapper = session.getMapper(StateDiffMapper.class);
            StateDiffWrapper target = new StateDiffWrapper(((StateDiff) thing));
            target.filling(index);
            mapper.insertOrUpdate(target);
        }
        return true;
    }

    
    public void delete(Indexable index) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            int effect = session.delete(converterIndexableToStr(index));
        }
    }

    
    public boolean update(Persistable model, Indexable index, String item) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            StateDiffMapper mapper = session.getMapper(StateDiffMapper.class);
            ((StateDiffWrapper) model).filling(index);
            int effect = mapper.updateByPrimaryKey((StateDiffWrapper) model);
            return true;
        }
    }

    public boolean update(Persistable model, Indexable index, String item, boolean merge, SqlSession session) throws Exception {
        StateDiffMapper mapper = session.getMapper(StateDiffMapper.class);
        ((StateDiffWrapper) model).filling(index);
        int effect = mapper.updateByPrimaryKey((StateDiffWrapper) model);
        return true;
    }

    
    public boolean exists(Indexable key) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            StateDiffMapper mapper = session.getMapper(StateDiffMapper.class);
            StateDiffWrapper object = mapper.selectByPrimaryKey(converterIndexableToStr(key));
            return object != null;
        }
    }

    public boolean exists(Indexable key, SqlSession session) throws Exception {
        StateDiffMapper mapper = session.getMapper(StateDiffMapper.class);
        StateDiffWrapper object = mapper.selectByPrimaryKey(converterIndexableToStr(key));
        return object != null;
    }

    
    public Pair<Indexable, Persistable> latest(Class<?> indexModel) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            StateDiffMapper mapper = session.getMapper(StateDiffMapper.class);
            StateDiffWrapper value = mapper.latest();
            if (value != null) {
                // todo 这里没有使用类型创建对象
                Indexable index = (Indexable) indexModel.newInstance();
                if (StringUtils.isNumeric(value.getHash())) {
                    index = new IntegerIndex(Integer.parseInt(value.getHash()));
                } else {
                    index = new Hash(value.getHash());
                }
                return new Pair<>(index, value.toStateDiff());
            }
        }

        return new Pair<>(null, null);
    }

    
    public Set<Indexable> keysWithMissingReferences(Class<?> otherClass) throws Exception {
        return null;
    }

    
    public Persistable get(Indexable index) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            StateDiffMapper mapper = session.getMapper(StateDiffMapper.class);
            StateDiffWrapper object = mapper.selectByPrimaryKey(converterIndexableToStr(index));
            return object != null ? object.toStateDiff() : StateDiff.class.newInstance();
        }
    }

    
    public boolean mayExist(Indexable index) throws Exception {
        return exists(index);
    }

    
    public long count() throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            StateDiffMapper mapper = session.getMapper(StateDiffMapper.class);
            return mapper.count();
        }
    }

    
    public Set<Indexable> keysStartingWith(byte[] value) {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            StateDiffMapper mapper = session.getMapper(StateDiffMapper.class);
            // todo  可能需要特殊修改的地方,  实际上只有milestone使用integerindex
            Set<String> rvInStr = mapper.keysStartingWith(new Hash(value).toString());
            if (rvInStr == null) {
                return Collections.emptySet();
            }
            return rvInStr.stream().map(Hash::new).collect(Collectors.toSet());
        }
    }

    
    public Persistable seek(byte[] key) throws Exception {
        Indexable out;
        Set<Indexable> hashes = keysStartingWith(key);
        if (hashes.size() == 1) {
            out = (Indexable) hashes.toArray()[0];
        } else if (hashes.size() > 1) {
            // todo 这里的 new SecureRandom()可能导致程序运行缓慢
            out = (Indexable) hashes.toArray()[new SecureRandom().nextInt(hashes.size())];
        } else {
            out = null;
        }
        return get(out);
    }

    
    public Pair<Indexable, Persistable> next(Indexable index) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            StateDiffMapper mapper = session.getMapper(StateDiffMapper.class);
            StateDiffWrapper value = mapper.next(converterIndexableToStr(index));
            if (value != null) {
                // todo 这里没有使用类型创建对象
                Indexable indexNext;
                if (StringUtils.isNumeric(value.getHash())) {
                    indexNext = new IntegerIndex(Integer.parseInt(value.getHash()));
                } else {
                    indexNext = new Hash(value.getHash());
                }
                return new Pair<>(indexNext, value.toStateDiff());
            }
        }
        return new Pair<>(null, null);
    }

    
    public Pair<Indexable, Persistable> previous(Indexable index) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            StateDiffMapper mapper = session.getMapper(StateDiffMapper.class);
            StateDiffWrapper value = mapper.previous(converterIndexableToStr(index));
            if (value != null) {
                // todo 这里没有使用类型创建对象
                Indexable indexNext;
                if (StringUtils.isNumeric(value.getHash())) {
                    indexNext = new IntegerIndex(Integer.parseInt(value.getHash()));
                } else {
                    indexNext = new Hash(value.getHash());
                }
                return new Pair<>(indexNext, value.toStateDiff());
            }
        }
        return new Pair<>(null, null);
    }

    
    public Pair<Indexable, Persistable> first(Class<?> indexModel) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            StateDiffMapper mapper = session.getMapper(StateDiffMapper.class);
            StateDiffWrapper value = mapper.first();
            if (value != null) {
                // todo 这里没有使用类型创建对象
                Indexable index = (Indexable) indexModel.newInstance();
                if (StringUtils.isNumeric(value.getHash())) {
                    index = new IntegerIndex(Integer.parseInt(value.getHash()));
                } else {
                    index = new Hash(value.getHash());
                }
                return new Pair<>(index, value.toStateDiff());
            }
        }
        return new Pair<>(null, null);
    }
}
