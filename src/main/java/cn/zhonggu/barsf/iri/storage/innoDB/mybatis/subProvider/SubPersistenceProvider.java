package cn.zhonggu.barsf.iri.storage.innoDB.mybatis.subProvider;

import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.utils.Pair;

import java.util.Set;

public interface SubPersistenceProvider {
    boolean save(Persistable model, Indexable index) throws Exception;
    void delete(Indexable index) throws Exception;

    boolean exists(Indexable key) throws Exception;

    boolean update(Persistable model, Indexable index, String item) throws Exception;

    Pair<Indexable, Persistable> latest(Class<?> indexModel) throws Exception;

    Set<Indexable> keysWithMissingReferences(Class<?> otherClass) throws Exception;

    Persistable get(Indexable index) throws Exception;

    boolean mayExist(Indexable index) throws Exception;

    long count() throws Exception;

    Set<Indexable> keysStartingWith(byte[] value);

    Persistable seek(byte[] key) throws Exception;

    Pair<Indexable, Persistable> next(Indexable index) throws Exception;
    Pair<Indexable, Persistable> previous(Indexable index) throws Exception;

    Pair<Indexable, Persistable> first(Class<?> indexModel) throws Exception;
}
