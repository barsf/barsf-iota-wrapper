package cn.zhonggu.barsf.iri.storage.innoDB.mybatis.subProvider;

import cn.zhonggu.barsf.iri.modelWrapper.AddressWrapper;
import cn.zhonggu.barsf.iri.storage.innoDB.NeededException;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.modelMapper.AddressMapper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.modelMapper.TransactionMapper;
import com.iota.iri.model.Address;
import com.iota.iri.model.Hash;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.utils.Pair;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper.converterIndexableToStr;


/**
 * Created by ZhuDH on 2018/3/30.
 * <p>
 */
public class AddressProvider implements SubPersistenceProvider {
    private static Logger log = LoggerFactory.getLogger(AddressProvider.class);

    private static volatile AddressProvider instance = null;
    private final SqlSessionFactory sessionFactory;

    private AddressProvider() {
        this.sessionFactory = DbHelper.getSingletonSessionFactory();
    }

    public static AddressProvider getInstance() {
        synchronized (AddressProvider.class) {
            if (instance == null) {
                instance = new AddressProvider();
            }
        }
        return instance;
    }

    public boolean save(Persistable thing, Indexable index, SqlSession outSession) {
        // 地址处理使用
        AddressMapper addressMapper = outSession.getMapper(AddressMapper.class);
        ((AddressWrapper) thing).setHash(converterIndexableToStr(index));
        return addressMapper.insertOrUpdate((AddressWrapper) thing) > 0;
    }


    public boolean save(byte[] thing, Indexable index, SqlSession outSession) throws Exception {
        // 未曾使用
        throw new NeededException();
    }

    public boolean save(Persistable thing, Indexable index) {
        throw new NeededException();
    }

    public void delete(Indexable index) throws Exception {
    }

    public boolean update(Persistable model, Indexable index, String item) throws Exception {
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
        return null;
    }

    /* 提供给原生iri使用,从transaction表中获取address数据  不会返回空值 */
    public Persistable getFromTransaction(Indexable index) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
            List<String> hashOfTransByTag = mapper.selectHashesByAddress(converterIndexableToStr(index));
            if (hashOfTransByTag.size() > 0) {
                return new AddressWrapper(hashOfTransByTag.stream().map(Hash::new).collect(Collectors.toList())).toAddress();
            } else {
                return Address.class.newInstance();
            }
        }
    }

    /* 从t_address表中获取address数据  会返回空值 */
    public AddressWrapper get(String addressHash, SqlSession session) {
        AddressMapper mapper = session.getMapper(AddressMapper.class);
        return mapper.selectByPrimaryKey(addressHash);
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

    public int updateByPrimaryKeySelective(AddressWrapper add, SqlSession outerSession) {
        if (outerSession != null) {
            AddressMapper mapper = outerSession.getMapper(AddressMapper.class);
            return mapper.updateByPrimaryKeySelective(add);
        }

        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            AddressMapper mapper = session.getMapper(AddressMapper.class);
            return mapper.updateByPrimaryKeySelective(add);
        }
    }

    public void saveUnUpdate(Persistable thing, Indexable index, SqlSession session) {
        AddressMapper mapper = session.getMapper(AddressMapper.class);
        ((AddressWrapper) thing).setHash(converterIndexableToStr(index));
        mapper.insert((AddressWrapper) thing);
    }
}
