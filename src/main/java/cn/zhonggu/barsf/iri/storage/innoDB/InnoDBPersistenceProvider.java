package cn.zhonggu.barsf.iri.storage.innoDB;

import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.subProvider.*;
import com.iota.iri.model.*;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.storage.PersistenceProvider;
import com.iota.iri.utils.Pair;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper.converterIndexableToStr;
import static cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper.initDbSource;


/*
 * Created by ZhuDH on 2018/3/21.
 */
public class InnoDBPersistenceProvider implements PersistenceProvider {
    private static final Logger log = LoggerFactory.getLogger(InnoDBPersistenceProvider.class);
    private static final int TRANS_CACHE_MAX_SIZE = 600;
    private static final int CACHE_MAX_WAITING_TIME = 6000;//ms
    private static long lastCacheStoreTime = System.currentTimeMillis();//ms

    public static long lastCost = 0;

    // subDbProviders
    private TransactionProvider transactionProvider;
    private final String myBatisPropPath;
    private boolean available;
    private MilestoneProvider milestoneProvider;
    private StateDiffProvider stateDiffProvider;
    private AddressProvider addressProvider;
    private ApproveeProvider approveeProvider;
    private BundleProvider bundleProvider;
    private TagProvider tagProvider;

    private List<Pair<Indexable, Persistable>> transactionCacheLis = Collections.synchronizedList(new ArrayList<>(1300));

    public InnoDBPersistenceProvider(String confPath) {
        this.myBatisPropPath = confPath;
    }

    public static long totalTransaction = 0;

    @Override
    public void init() throws Exception {
        log.info("Initializing Google Cloud Mysql Database Backend... ");
        initDbSource(myBatisPropPath);
        initSubProider();
        log.info("Mysql persistence provider initialized.");

//        initJDBC();
    }

    private void initSubProider() {
        this.transactionProvider = TransactionProvider.getInstance();
        this.milestoneProvider = MilestoneProvider.getInstance();
        this.stateDiffProvider = StateDiffProvider.getInstance();
        this.addressProvider = AddressProvider.getInstance();
        this.approveeProvider = ApproveeProvider.getInstance();
        this.bundleProvider = BundleProvider.getInstance();
        this.tagProvider = TagProvider.getInstance();
    }


    /*
     * 判断是否可用
     */
    @Override
    public boolean isAvailable() {
        return this.available;
    }


    @Override
    public void shutdown() {
        // do nothing
    }

    @Override
    public boolean save(Persistable thing, Indexable index) throws Exception {
        if (thing instanceof Transaction) {
            return transactionProvider.save(thing, index);
        } else if (thing instanceof Milestone) {
            return milestoneProvider.save(thing, index);
        } else if (thing instanceof StateDiff) {
            return stateDiffProvider.save(thing, index);
        } else if (thing instanceof Address) {
            return addressProvider.save(thing, index);
        } else if (thing instanceof Approvee) {
            return approveeProvider.save(thing, index);
        } else if (thing instanceof Bundle) {
            return bundleProvider.save(thing, index);
        } else if (thing instanceof Tag) {
            return tagProvider.save(thing, index);
        }
        throw new RuntimeException("Unknown type!");
    }


    @Override
    public void delete(Class<?> model, Indexable index) throws Exception {
        if (model == Transaction.class) {
            transactionProvider.delete(index);
        } else if (model == Milestone.class) {
            milestoneProvider.delete(index);
        } else if (model == StateDiff.class) {
            stateDiffProvider.delete(index);
        } else if (model == Address.class) {
            addressProvider.delete(index);
        } else if (model == Approvee.class) {
            approveeProvider.delete(index);
        } else if (model == Bundle.class) {
            bundleProvider.delete(index);
        } else if (model == Tag.class) {
            tagProvider.delete(index);
        }
        throw new RuntimeException("Unknown type!");
    }

    @Override
    public boolean update(Persistable thing, Indexable index, String item) throws Exception {
        if (thing instanceof Transaction) {
            return transactionProvider.update(thing, index, item);
        }
        throw new RuntimeException("Unknown type!");
    }

    @Override
    public boolean exists(Class<?> model, Indexable key) throws Exception {
        if (model == Transaction.class) {
            return transactionProvider.exists(key);
        } else if (model == Milestone.class) {
            return milestoneProvider.exists(key);
        } else if (model == StateDiff.class) {
            return stateDiffProvider.exists(key);
        } else if (model == Address.class) {
            return addressProvider.exists(key);
        } else if (model == Approvee.class) {
            return approveeProvider.exists(key);
        } else if (model == Bundle.class) {
            return bundleProvider.exists(key);
        } else if (model == Tag.class) {
            return tagProvider.exists(key);
        }
        throw new RuntimeException("Unknown type!");
    }

    @Override
    public Pair<Indexable, Persistable> latest(Class<?> model, Class<?> indexModel) throws Exception {
        if (model == Transaction.class) {
            return transactionProvider.latest(indexModel);
        } else if (model == Milestone.class) {
            return milestoneProvider.latest(indexModel);
        } else if (model == StateDiff.class) {
            return stateDiffProvider.latest(indexModel);
        } else if (model == Address.class) {
            return addressProvider.latest(indexModel);
        } else if (model == Approvee.class) {
            return approveeProvider.latest(indexModel);
        } else if (model == Bundle.class) {
            return bundleProvider.latest(indexModel);
        } else if (model == Tag.class) {
            return tagProvider.latest(indexModel);
        }
        throw new RuntimeException("Unknown type!");
    }

    @Override
    public Set<Indexable> keysWithMissingReferences(Class<?> modelClass, Class<?> otherClass) throws Exception {
        // todo
        return null;
    }

    @Override
    public Persistable get(Class<?> model, Indexable index) throws Exception {
        Persistable object = null;
        if (model == Transaction.class) {
            return transactionProvider.get(index);
        } else if (model == Milestone.class) {
            return milestoneProvider.get(index);
        } else if (model == StateDiff.class) {
            return stateDiffProvider.get(index);
        } else if (model == Address.class) {
            return addressProvider.get(index);
        } else if (model == Approvee.class) {
            return approveeProvider.get(index);
        } else if (model == Bundle.class) {
            return bundleProvider.get(index);
        } else if (model == Tag.class) {
            return tagProvider.get(index);
        }
        throw new RuntimeException("Unknown type!");
    }

    @Override
    public boolean mayExist(Class<?> model, Indexable index) throws Exception {
        // 外部直接使用exist了
        return exists(model, index);
    }

    @Override
    public long count(Class<?> model) throws Exception {

        if (totalTransaction == 0) {
            totalTransaction = -1;
            new Thread(() -> {
                try {
                    totalTransaction = transactionProvider.count();
                } catch (Exception e) {
                    log.error("", e);
                }
            }).start();
        }
        return totalTransaction;
    }


    @Override
    public Set<Indexable> keysStartingWith(Class<?> model, byte[] value) {
        if (model == Transaction.class) {
            return transactionProvider.keysStartingWith(value);
        } else if (model == Milestone.class) {
            return milestoneProvider.keysStartingWith(value);
        } else if (model == StateDiff.class) {
            return stateDiffProvider.keysStartingWith(value);
        } else if (model == Address.class) {
            return addressProvider.keysStartingWith(value);
        } else if (model == Approvee.class) {
            return approveeProvider.keysStartingWith(value);
        } else if (model == Bundle.class) {
            return bundleProvider.keysStartingWith(value);
        } else if (model == Tag.class) {
            return tagProvider.keysStartingWith(value);
        }
        throw new RuntimeException("Unknown type!");

    }

    @Override
    public Persistable seek(Class<?> model, byte[] value) throws Exception {
        if (model == Transaction.class) {
            return transactionProvider.seek(value);
        } else if (model == Milestone.class) {
            return milestoneProvider.seek(value);
        } else if (model == StateDiff.class) {
            return stateDiffProvider.seek(value);
        } else if (model == Address.class) {
            return addressProvider.seek(value);
        } else if (model == Approvee.class) {
            return approveeProvider.seek(value);
        } else if (model == Bundle.class) {
            return bundleProvider.seek(value);
        } else if (model == Tag.class) {
            return tagProvider.seek(value);
        }
        throw new RuntimeException("Unknown type!");
    }

    @Override
    public Pair<Indexable, Persistable> next(Class<?> model, Indexable index) throws Exception {
        if (model == Transaction.class) {
            return transactionProvider.next(index);
        } else if (model == Milestone.class) {
            return milestoneProvider.next(index);
        } else if (model == StateDiff.class) {
            return stateDiffProvider.next(index);
        } else if (model == Address.class) {
            return addressProvider.next(index);
        } else if (model == Approvee.class) {
            return approveeProvider.next(index);
        } else if (model == Bundle.class) {
            return bundleProvider.next(index);
        } else if (model == Tag.class) {
            return tagProvider.next(index);
        }
        throw new RuntimeException("Unknown type!");
    }

    @Override
    public Pair<Indexable, Persistable> previous(Class<?> model, Indexable index) throws Exception {
        if (model == Transaction.class) {
            return transactionProvider.previous(index);
        } else if (model == Milestone.class) {
            return milestoneProvider.previous(index);
        } else if (model == StateDiff.class) {
            return stateDiffProvider.previous(index);
        } else if (model == Address.class) {
            return addressProvider.previous(index);
        } else if (model == Approvee.class) {
            return approveeProvider.previous(index);
        } else if (model == Bundle.class) {
            return bundleProvider.previous(index);
        } else if (model == Tag.class) {
            return tagProvider.previous(index);
        }
        throw new RuntimeException("Unknown type!");
    }

    @Override
    public Pair<Indexable, Persistable> first(Class<?> model, Class<?> index) throws Exception {
        if (model == Transaction.class) {
            return transactionProvider.first(index);
        } else if (model == Milestone.class) {
            return milestoneProvider.first(index);
        } else if (model == StateDiff.class) {
            return stateDiffProvider.first(index);
        } else if (model == Address.class) {
            return addressProvider.first(index);
        } else if (model == Approvee.class) {
            return approveeProvider.first(index);
        } else if (model == Bundle.class) {
            return bundleProvider.first(index);
        } else if (model == Tag.class) {
            return tagProvider.first(index);
        }
        throw new RuntimeException("Unknown type!!");
    }


    @Override
    public boolean saveBatch(List<Pair<Indexable, Persistable>> models) throws Exception {

        final SqlSession session = DbHelper.getSingletonSessionFactory().openSession(ExecutorType.BATCH, false);
        try {
            Indexable mainHash = null;
            int s1 = 0, s2 = 0, s3 = 0;
            long start = System.currentTimeMillis();

            for (Pair<Indexable, Persistable> model : models) {
                Persistable thing = model.hi;
                Indexable index = model.low;
                if (thing instanceof Transaction) {
                    List<Pair<Indexable, Persistable>> temp = new ArrayList<Pair<Indexable, Persistable>>();
                    temp.add(new Pair<Indexable, Persistable>(index, thing));
                    transactionProvider.saveInBatch(temp, true, session);
                    mainHash = index;

                    s1 = 1;
                } else if (thing instanceof Milestone) {
                    milestoneProvider.save(thing, index, session, true);

                    s2 = 1;
                } else if (thing instanceof StateDiff) {
                    AtomicLong tl = new AtomicLong(0);
                    stateDiffProvider.save(thing, index, session, true, tl);

                    s3 = 1;
                } else {
                    // donothing
                }
            }

            log.info(" tranListSize:" + transactionCacheLis.size() + " before cost:" + (System.currentTimeMillis() - start));
            session.commit();
            lastCost = (System.currentTimeMillis() - start);
            String msg = "test " + converterIndexableToStr(mainHash) + " \t"
                    + s1 + " " + s2 + " " + s3 +
                    " \n\t test Inno commit cost :" + lastCost;
            log.info(msg);
        } catch (
                Exception e)

        {
            session.rollback();
            log.error("", e);
            return false;
        } finally

        {
            session.close();
        }

        // 不发生异常就是true
        return true;
    }


    @Override
    public void clear(Class<?> column) throws Exception {

    }

    @Override
    public void clearMetadata(Class<?> column) throws Exception {

    }

}
