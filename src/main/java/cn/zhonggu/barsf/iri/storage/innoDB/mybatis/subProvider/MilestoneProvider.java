package cn.zhonggu.barsf.iri.storage.innoDB.mybatis.subProvider;

import cn.zhonggu.barsf.iri.modelWrapper.MilestoneWrapper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.modelMapper.MilestoneMapper;
import com.iota.iri.model.Hash;
import com.iota.iri.model.IntegerIndex;
import com.iota.iri.model.Milestone;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.utils.Pair;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper.converterIndexableToStr;


/**
 * Created by ZhuDH on 2018/3/30.
 */
public class MilestoneProvider implements SubPersistenceProvider {
    private static Logger LOG = LoggerFactory.getLogger(MilestoneProvider.class);
    private static volatile MilestoneProvider instance = null;
    private final SqlSessionFactory sessionFactory;

    private MilestoneProvider() {
        this.sessionFactory = DbHelper.getSingletonSessionFactory();
    }

    public static MilestoneProvider getInstance() {
        synchronized (MilestoneProvider.class) {
            if (instance == null) {
                instance = new MilestoneProvider();
            }
        }
        return instance;
    }

    public boolean save(Persistable thing, Indexable index, SqlSession outSession, boolean mergeMode) {
        if (outSession == null) {
            return save(thing, index);
        }
        MilestoneMapper mapper = outSession.getMapper(MilestoneMapper.class);
        MilestoneWrapper target = new MilestoneWrapper((Milestone) thing);
        mapper.insertOrUpdate(target);
        // 不报错即为true
        return true;
    }

    public boolean save(Persistable thing, Indexable index) {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            MilestoneMapper mapper = session.getMapper(MilestoneMapper.class);
            MilestoneWrapper target = new MilestoneWrapper((Milestone) thing);
            mapper.insertOrUpdate(target);
        }
        return true;
    }

    public boolean save(byte[] thing, Indexable index, SqlSession outSession, boolean mergeMode) throws Exception {

        MilestoneMapper mapper = outSession.getMapper(MilestoneMapper.class);
        Milestone target = (Milestone.class.newInstance());
        target.read(thing);
        mapper.insertOrUpdate(new MilestoneWrapper(target));
        // 不报错即为true
        return true;
    }

    @Override
    public void delete(Indexable index) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            MilestoneMapper mapper = session.getMapper(MilestoneMapper.class);
            int effect = mapper.deleteByPrimaryKey(Long.parseLong(converterIndexableToStr(index)));
        }
    }

    @Override
    public boolean update(Persistable model, Indexable index, String item) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            MilestoneMapper mapper = session.getMapper(MilestoneMapper.class);
            int effect = mapper.updateByPrimaryKey((MilestoneWrapper) model);
            return true;
        }
    }

    public boolean exists(Indexable key, SqlSession session) throws Exception {
        if (session == null){
            return exists(key);
        }

        MilestoneMapper mapper = session.getMapper(MilestoneMapper.class);
        return mapper.existsWithPrimaryKey(converterIndexableToStr(key));
    }

    @Override
    public boolean exists(Indexable key) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            MilestoneMapper mapper = session.getMapper(MilestoneMapper.class);
            return mapper.existsWithPrimaryKey(converterIndexableToStr(key));
        }
    }


    @Override
    public Pair<Indexable, Persistable> latest(Class<?> indexModel) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            MilestoneMapper mapper = session.getMapper(MilestoneMapper.class);
            MilestoneWrapper value = mapper.latest();
            if (value != null) {
                Indexable index = new IntegerIndex(value.getIndex());
                return new Pair<>(index, value.toMilestone());
            }
        }

        return new Pair<>(null, null);
    }

    @Override
    public Set<Indexable> keysWithMissingReferences(Class<?> otherClass) throws Exception {
        return null;
    }

    @Override
    public Persistable get(Indexable index) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            MilestoneMapper mapper = session.getMapper(MilestoneMapper.class);
            MilestoneWrapper object = mapper.selectByPrimaryKey(converterIndexableToStr(index));
            return object != null ? object.toMilestone() : Milestone.class.newInstance();
        }
    }

    @Override
    public boolean mayExist(Indexable index) throws Exception {
        return exists(index);
    }

    @Override
    public long count() throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            MilestoneMapper mapper = session.getMapper(MilestoneMapper.class);
            return mapper.count();
        }
    }

    @Override
    public Set<Indexable> keysStartingWith(byte[] value) {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            MilestoneMapper mapper = session.getMapper(MilestoneMapper.class);
            Set<String> rvInStr = mapper.keysStartingWith(new Hash(value).toString());
            if (rvInStr == null) {
                return Collections.emptySet();
            }
            return rvInStr.stream().map(Hash::new).collect(Collectors.toSet());
        }
    }

    @Override
    public Persistable seek(byte[] key) throws Exception {
        Indexable out;
        Set<Indexable> hashes = keysStartingWith(key);
        if (hashes.size() == 1) {
            out = (Indexable) hashes.toArray()[0];
        } else if (hashes.size() > 1) {
            out = (Indexable) hashes.toArray()[new SecureRandom().nextInt(hashes.size())];
        } else {
            out = null;
        }
        return get(out);
    }

    @Override
    public Pair<Indexable, Persistable> next(Indexable index) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            MilestoneMapper mapper = session.getMapper(MilestoneMapper.class);
            MilestoneWrapper value = mapper.next(converterIndexableToStr(index));
            if (value != null) {
                Indexable indexNext = new IntegerIndex(value.getIndex());
                return new Pair<>(indexNext, value.toMilestone());
            }
        }
        return new Pair<>(null, null);
    }

    @Override
    public Pair<Indexable, Persistable> previous(Indexable index) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            MilestoneMapper mapper = session.getMapper(MilestoneMapper.class);
            MilestoneWrapper value = mapper.previous(converterIndexableToStr(index));
            if (value != null) {
                Indexable indexNext;
                indexNext = new IntegerIndex(value.getIndex());
                return new Pair<>(indexNext, value.toMilestone());
            }
        }
        return new Pair<>(null, null);
    }

    @Override
    public Pair<Indexable, Persistable> first(Class<?> indexModel) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            MilestoneMapper mapper = session.getMapper(MilestoneMapper.class);
            MilestoneWrapper value = mapper.first();
            if (value != null) {
                // todo 这里没有使用类型创建对象
                Indexable index = new IntegerIndex(value.getIndex());
                return new Pair<>(index, value.toMilestone());
            }
        }
        return new Pair<>(null, null);
    }


}
