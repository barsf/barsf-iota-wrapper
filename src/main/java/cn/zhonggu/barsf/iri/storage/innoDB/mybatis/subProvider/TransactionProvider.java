package cn.zhonggu.barsf.iri.storage.innoDB.mybatis.subProvider;

import cn.zhonggu.barsf.iri.modelWrapper.TransactionWrapper;
import cn.zhonggu.barsf.iri.modelWrapper.TransactionEx;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.modelMapper.TransactionMapper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.modelMapper.TransactionTrytesMapper;
import com.iota.iri.model.Hash;
import com.iota.iri.model.IntegerIndex;
import com.iota.iri.model.Transaction;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.utils.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static cn.zhonggu.barsf.iri.storage.innoDB.InnoDBPersistenceProvider.totalTransaction;
import static cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper.converterIndexableToStr;
import static com.iota.iri.controllers.TransactionViewModel.FILLED_SLOT;

/**
 * Created by ZhuDH on 2018/3/30.
 */
public class TransactionProvider implements SubPersistenceProvider {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionProvider.class);

    private static volatile TransactionProvider instance = null;
    private final SqlSessionFactory sessionFactory;

    private TransactionProvider() {
        this.sessionFactory = DbHelper.getSingletonSessionFactory();
    }

    public static TransactionProvider getInstance() {
        synchronized (TransactionProvider.class) {
            if (instance == null) {
                instance = new TransactionProvider();
            }
        }
        return instance;
    }

    public boolean save(Persistable thing, Indexable index, SqlSession outSession, boolean mergeMode) throws Exception {
        if (outSession == null) {
            return save(thing, index);
        }

        TransactionMapper mapper = outSession.getMapper(TransactionMapper.class);
        TransactionWrapper target = new TransactionWrapper((Transaction) thing);
        target.filling(index);
        mapper.insertOrUpdate(target);
        TransactionTrytesMapper exMapper = outSession.getMapper(TransactionTrytesMapper.class);
        exMapper.insertOrUpdate(target.getInnerTrytes());

        // 不报错即为true
        return true;
    }

    public boolean saveUnUpdate(Persistable thing, Indexable index, SqlSession session) {
        try {
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
            TransactionWrapper target = new TransactionWrapper((Transaction) thing);
            target.filling(index);
            mapper.insert(target);
            TransactionTrytesMapper exMapper = session.getMapper(TransactionTrytesMapper.class);
            exMapper.insert(target.getInnerTrytes());

        } catch (Exception e) {
            LOG.error("", e);
            return false;
        }
        return true;
    }

    public boolean saveInBatch(List<Pair<Indexable, Persistable>> transactionCacheLis, boolean updateOnDpk, SqlSession session) {
        ArrayList<TransactionEx> InnerTrytesList = new ArrayList<>(transactionCacheLis.size());
        for (Pair<Indexable, Persistable> cachedTrans : transactionCacheLis) {
            TransactionWrapper thing = new TransactionWrapper((Transaction) cachedTrans.hi);
            Indexable index = cachedTrans.low;
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
            thing.filling(index);
            if (updateOnDpk) {
                mapper.insertOrUpdate(thing);
            } else {
                mapper.insert(thing);
            }

            InnerTrytesList.add(thing.getInnerTrytes());
        }

        if (InnerTrytesList.size() != transactionCacheLis.size()) {
            throw new RuntimeException("should't happened");
        }

        for (TransactionEx transactionEx : InnerTrytesList) {
            TransactionTrytesMapper exMapper = session.getMapper(TransactionTrytesMapper.class);
            if (updateOnDpk) {
                exMapper.insertOrUpdate(transactionEx);
            } else {
                exMapper.insert(transactionEx);
            }
        }
        synchronized (this) {
            totalTransaction += transactionCacheLis.size();
        }
        return true;
    }

    @Override
    public void delete(Indexable index) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(false)) {
            String pk = converterIndexableToStr(index);
            try {
                TransactionMapper mapper = session.getMapper(TransactionMapper.class);
                int effect = mapper.deleteByPrimaryKey(pk);
                TransactionTrytesMapper exMapper = session.getMapper(TransactionTrytesMapper.class);
                effect = exMapper.deleteByPrimaryKey(pk);

                session.commit();
            } catch (Exception e) {
                LOG.error("", e);
                session.rollback();
            }
        }
    }

    public boolean save(Persistable thing, Indexable index) throws Exception {
        if (exists(index)) {
            update(thing, index, "save");
        } else {
            return saveUnUpdate(thing, index, this.sessionFactory.openSession(true));
        }

        return true;
    }


    @Override
    public boolean update(Persistable model, Indexable index, String item) throws Exception {
        LOG.info("ZZZ " + Thread.currentThread().getName() + " updateSelective :" + item);
        TransactionProvider transactionProvider = TransactionProvider.getInstance();
        Transaction target = (Transaction) model;
        TransactionWrapper emptyTrasn = TransactionWrapper.emptyTrans(converterIndexableToStr(index));

        if (item.contains("sender")) {
            emptyTrasn.sender = target.sender;
        }
        if (item.contains("arrivalTime")) {
            emptyTrasn.arrivalTime = target.arrivalTime;
        }
        if (item.contains("solid")) {
            emptyTrasn.solid = target.solid;
        }
        if (item.contains("validity")) {
            emptyTrasn.validity = target.validity;
        }
        if (item.contains("height")) {
            emptyTrasn.height = target.height;
        }
        if (item.contains("snapshot")) {
            emptyTrasn.snapshot = target.snapshot;
        }
        transactionProvider.updateByPrimaryKeySelective(emptyTrasn, null);

        return true;
    }

    @Override
    public boolean exists(Indexable key) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
            return mapper.existsWithPrimaryKey(converterIndexableToStr(key));
        }
    }

    public boolean exists(Indexable key, SqlSession session) throws Exception {
        TransactionMapper mapper = session.getMapper(TransactionMapper.class);
        return mapper.existsWithPrimaryKey(converterIndexableToStr(key));
    }

    @Override
    public Pair<Indexable, Persistable> latest(Class<?> indexModel) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
            TransactionWrapper value = mapper.latest();
            if (value != null) {
                TransactionTrytesMapper exMapper = session.getMapper(TransactionTrytesMapper.class);
                TransactionEx innerTrytes = exMapper.selectByPrimaryKey(value.getHash());
                if (!value.getHash().equals(innerTrytes.getHash())) {
                    throw new RuntimeException("should't happened");
                }
                value.setInnerTrytes(innerTrytes);

                // todo 这里没有使用类型创建对象,可以改写
                Indexable index = (Indexable) indexModel.newInstance();
                if (StringUtils.isNumeric(value.getHash())) {
                    index = new IntegerIndex(Integer.parseInt(value.getHash()));
                } else {
                    index = new Hash(value.getHash());
                }
                return new Pair<>(index, value.toTransaction());
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
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
            TransactionWrapper object = mapper.selectByPrimaryKey(converterIndexableToStr(index));
            if (object != null) {
                TransactionTrytesMapper exMapper = session.getMapper(TransactionTrytesMapper.class);
                TransactionEx innerTrytes = exMapper.selectByPrimaryKey(object.getHash());
                if (innerTrytes == null || !object.getHash().equals(innerTrytes.getHash())) {
                    throw new RuntimeException("should't happened");
                }

                if (object.type != FILLED_SLOT) {
                    throw new RuntimeException(object.getHash());
                }
                object.setInnerTrytes(innerTrytes);
            }

//            //todo 数据库同步的数据怕是需要这个操作
//            session.getMapper()

            return object != null ? object.toTransaction() : Transaction.class.newInstance();
        }
    }

    public Persistable get(String pk) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
            TransactionWrapper object = mapper.selectByPrimaryKey(pk);
            if (object != null) {
                TransactionTrytesMapper exMapper = session.getMapper(TransactionTrytesMapper.class);
                TransactionEx innerTrytes = exMapper.selectByPrimaryKey(object.getHash());
                if (!object.getHash().equals(innerTrytes.getHash())) {
                    throw new RuntimeException("should't happened");
                }
                object.setInnerTrytes(innerTrytes);
            }
            return null;
        }
    }

    @Override
    public boolean mayExist(Indexable index) throws Exception {
        return exists(index);
    }


    @Override
    public long count() throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
            return mapper.count();
        }
    }

    @Override
    public Set<Indexable> keysStartingWith(byte[] value) {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
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
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
            TransactionWrapper value = mapper.next(converterIndexableToStr(index));
            if (value != null) {
                TransactionTrytesMapper exMapper = session.getMapper(TransactionTrytesMapper.class);
                TransactionEx innerTrytes = exMapper.selectByPrimaryKey(value.getHash());
                if (!value.getHash().equals(innerTrytes.getHash())) {
                    throw new RuntimeException("should't happened");
                }
                value.setInnerTrytes(innerTrytes);

                // todo 这里没有使用类型创建对象
                Indexable indexNext;
                if (StringUtils.isNumeric(value.getHash())) {
                    indexNext = new IntegerIndex(Integer.parseInt(value.getHash()));
                } else {
                    indexNext = new Hash(value.getHash());
                }
                return new Pair<>(indexNext, value.toTransaction());
            }
        }
        return new Pair<>(null, null);
    }

    @Override
    public Pair<Indexable, Persistable> previous(Indexable index) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
            TransactionWrapper value = mapper.previous(converterIndexableToStr(index));
            if (value != null) {
                TransactionTrytesMapper exMapper = session.getMapper(TransactionTrytesMapper.class);
                TransactionEx innerTrytes = exMapper.selectByPrimaryKey(value.getHash());
                if (!value.getHash().equals(innerTrytes.getHash())) {
                    throw new RuntimeException("should't happened");
                }
                value.setInnerTrytes(innerTrytes);

                // todo 这里没有使用类型创建对象
                Indexable indexNext;
                if (StringUtils.isNumeric(value.getHash())) {
                    indexNext = new IntegerIndex(Integer.parseInt(value.getHash()));
                } else {
                    indexNext = new Hash(value.getHash());
                }
                return new Pair<>(indexNext, value.toTransaction());
            }
        }
        return new Pair<>(null, null);
    }

    @Override
    public Pair<Indexable, Persistable> first(Class<?> indexModel) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
            TransactionWrapper value = mapper.first();
            if (value != null) {
                TransactionTrytesMapper exMapper = session.getMapper(TransactionTrytesMapper.class);
                TransactionEx innerTrytes = exMapper.selectByPrimaryKey(value.getHash());
                if (!value.getHash().equals(innerTrytes.getHash())) {
                    throw new RuntimeException("should't happened");
                }
                value.setInnerTrytes(innerTrytes);

                // todo 这里没有使用类型创建对象
                Indexable index = (Indexable) indexModel.newInstance();
                if (StringUtils.isNumeric(value.getHash())) {
                    index = new IntegerIndex(Integer.parseInt(value.getHash()));
                } else {
                    index = new Hash(value.getHash());
                }
                return new Pair<>(index, value.toTransaction());
            }
        }
        return new Pair<>(null, null);
    }


    // 查询已经被确认 但是process = false的任务
    public List<TransactionWrapper> selectNeedProcessTrans(int batchSize, SqlSession outerSession) {
        if (outerSession != null) {
            TransactionMapper mapper = outerSession.getMapper(TransactionMapper.class);
            return mapper.selectNeedProcess(batchSize);
        }

        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
            return mapper.selectNeedProcess(batchSize);
        }
    }

    public int updateByPrimaryKeySelective(TransactionWrapper tac, SqlSession outerSession) {
        if (outerSession != null) {
            TransactionMapper mapper = outerSession.getMapper(TransactionMapper.class);
            return mapper.updateByPrimaryKeySelective(tac);
        }

        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
            return mapper.updateByPrimaryKeySelective(tac);
        }
    }

}
