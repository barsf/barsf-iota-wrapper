package cn.zhonggu.barsf.iri.runner;

import cn.zhonggu.barsf.iri.modelWrapper.*;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.subProvider.*;
import com.iota.iri.model.*;
import com.iota.iri.model.ObsoleteTag;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.utils.Pair;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Created by ZhuDH on 2018/4/11.
 */
public class RodbDataFishingRunner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(cn.zhonggu.barsf.iri.runner.RodbDataFishingRunner.class);
    private static final int MAX_BATCH_SIZE = 6000;
    private RocksDB db;
    private DBOptions options;
    private BloomFilter bloomFilter;
    private List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    private ColumnFamilyHandle transactionHandle;
    private ColumnFamilyHandle transactionMetadataHandle;
    private ColumnFamilyHandle milestoneHandle;
    private ColumnFamilyHandle stateDiffHandle;
    private ColumnFamilyHandle addressHandle;
    private ColumnFamilyHandle approveeHandle;
    private ColumnFamilyHandle bundleHandle;
    private ColumnFamilyHandle obsoleteTagHandle;
    private ColumnFamilyHandle tagHandle;

    private ArrayList<Class> classes = new ArrayList<>();

    // subDbProviders
    private TransactionProvider transactionProvider;
    private MilestoneProvider milestoneProvider;
    private StateDiffProvider stateDiffProvider;
    private AddressProvider addressProvider;
    private ApproveeProvider approveeProvider;
    private BundleProvider bundleProvider;
    private TagProvider tagProvider;
    private KvProvider kvProvider;

    private final HashMap<Class, List<String>> skip;

    // thread pool
    private static final int TASK_COUNT = 6; //启动线程的最大数量
    private static final FutureTask<Boolean>[] SYNC_FUTURES = new FutureTask[TASK_COUNT]; // 执行任务内核
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(TASK_COUNT); //线程池
    public static volatile boolean synced;

    public RodbDataFishingRunner() throws Exception {
        skip = new HashMap<>();

        initRdb();
        initClassTreeMap();
        log.info("rockdb ready!");
        //mybatis

        DbHelper.initDbSource("mybatis-config.xml");
        initSubProider();
        log.info("mybatis ready!");

        // init LastONeMAp
        initLastOneMap();

        classes.add(Transaction.class);
        classes.add(Milestone.class);
        classes.add(StateDiff.class);
//        classes.add(Address.class);
//        classes.add(Approvee.class);
//        classes.add(Bundle.class);
//        classes.add(Tag.class);
//
//        selfDebug(Transaction.class);
//        selfDebug(Milestone.class);
//        selfDebug(StateDiff.class);
//        selfDebug(Address.class);
//        selfDebug(Approvee.class);
//        selfDebug(Bundle.class);
//        selfDebug(Tag.class);
    }


    public static void selfCall() throws Exception {
        ScheduledExecutorService singleTread = Executors.newSingleThreadScheduledExecutor();
        singleTread.scheduleWithFixedDelay(new RodbDataFishingRunner(), 2, 2, TimeUnit.SECONDS);
    }


    @Override
    public void run() {
        long startAt = System.currentTimeMillis();
        int size = 0;
        if (classes.size() == 0) {
            synced = true;

            // 完成关闭连接
            shutdown();

            return;
        }
        Class nowClass = classes.get(new Random().nextInt(classes.size()));

        try {

            Indexable lastOne = lastIndexMap.get().get(nowClass);
            LinkedHashMap<Indexable, ArrayList<byte[]>> rvDate = getFromRdb(nowClass, lastOne == null ? Hash.NULL_HASH : lastOne);
            size = rvDate.size();
            log.info("从Rdb中获取" + nowClass + "数据 <" + size + ">个, 花费<" + (System.currentTimeMillis() - startAt) + "ms>");

            // 均分任务
            LinkedHashMap<Indexable, ArrayList<byte[]>>[] subRv = new LinkedHashMap[TASK_COUNT];
            for (int i = 0, subRvLength = subRv.length; i < subRvLength; i++) {
                subRv[i] = new LinkedHashMap<>();
            }

            int subRvSize = rvDate.size() / TASK_COUNT + 1;
            int subRvIndex = 0;

            rvDate.remove(lastOne);
            for (Map.Entry<Indexable, ArrayList<byte[]>> entry : rvDate.entrySet()) {
                LinkedHashMap<Indexable, ArrayList<byte[]>> nowSubRv = subRv[subRvIndex];
                nowSubRv.put(entry.getKey(), entry.getValue());
                if (nowSubRv.size() >= subRvSize) {
                    subRvIndex++;
                }
                lastOne = entry.getKey();
            }

            final KvEnum[] key = {tellMeWhatIsTheKey(nowClass)};


            // 多线程处理记录
            for (int i = 0; i < TASK_COUNT; i++) {
                int index = i;
                SYNC_FUTURES[i] = new FutureTask<>(() -> {
                    SqlSession session = DbHelper.getSingletonSessionFactory().openSession(ExecutorType.BATCH, false);
                    try {
                        String hash = "";
                        LinkedHashMap<Indexable, ArrayList<byte[]>> theSubRv = subRv[index];
                        if (rvDate.size() <= 1) {
                            if (index == TASK_COUNT - 1) {
                                classes.remove(nowClass);
                                log.info("Type <" + nowClass + "> is empty , removed. now remains :" + classes);
                            }
                            return true;
                        }
                        // 用于批量保存trans的列表
                        ArrayList<Pair<Indexable, Persistable>> transCacheList = new ArrayList<>();

                        for (Map.Entry<Indexable, ArrayList<byte[]>> entry : theSubRv.entrySet()) {
                            hash = DbHelper.converterIndexableToStr(entry.getKey());
                            if (skip.get(nowClass) != null && skip.get(nowClass).contains(hash)) {
                                continue;
                            }
                            log.info(Thread.currentThread().getName() + "> 同步到Innodb, <" + DbHelper.converterIndexableToStr(entry.getKey()) + "> Length:" + entry.getValue().get(0).length);

                            switch (key[0]) {
                                case transaction:
                                    // 数据预存到缓存
                                    TransactionWrapper target = TransactionWrapper.class.newInstance();
                                    target.read(entry.getValue().get(0));
                                    target.readMetadata(entry.getValue().get(1));
                                    target.filling(entry.getKey());

                                    transCacheList.add(new Pair<>(entry.getKey(), target.toTransaction()));
                                    break;
                                case statediff:
                                    stateDiffProvider.save(entry.getValue().get(0), entry.getKey(), session, false);
                                    break;
//                                case tag:
//                                    tagProvider.save(entry.getValue().get(0), entry.getKey(), session, false);
//                                    break;
//                                case bundle:
//                                    bundleProvider.save(entry.getValue().get(0), entry.getKey(), session, false);
//                                    break;
//                                case address:
//                                    addressProvider.save(entry.getValue().get(0), entry.getKey(), session, false);
//                                    break;
//                                case approvee:
//                                    approveeProvider.save(entry.getValue().get(0), entry.getKey(), session, false);
//                                    break;
                                case milestone:
                                    milestoneProvider.save(entry.getValue().get(0), entry.getKey(), session, false);
                                    break;
                                default:
                                    throw new RuntimeException();
                            }
                        }

                        if (transCacheList.size() > 0) {
                            // 批量推入
                            log.info(Thread.currentThread().getName() + " -> 批量推入transaction:" + transCacheList.size());
                            transactionProvider.saveInBatch(transCacheList, true, session);
                        }
                        session.commit();
                    } catch (Exception e) {
                        session.rollback();
                        log.info("err", e);
                        return false;
                    } finally {
                        session.close();
                    }
                    return true;
                });
            }

            for (int ii = 0; ii < TASK_COUNT; ii++) {
                EXECUTOR_SERVICE.submit(SYNC_FUTURES[ii]);
            }


            boolean processed = true;
            //等待子线程运行完毕
            for (int iii = 0; iii < TASK_COUNT; iii++) {
                try {
                    processed &= SYNC_FUTURES[iii].get();
                } catch (Exception e) {
                    log.info("", e);
                    throw new RuntimeException();
                }
            }

            if (processed) {
                kvProvider.saveValue(key[0].name(), DbHelper.converterIndexableToStr(lastOne), null);
                lastIndexMap.get().put(nowClass, lastOne);
            }

        } catch (Exception e) {
            log.error("", e);
        }

        log.info("this turn finished, syncd <" + size + "> cost<" + (System.currentTimeMillis() - startAt) + "ms> <" + ((double) size / ((System.currentTimeMillis() - startAt) / 1000.0) + " r/s>"));
    }

    private cn.zhonggu.barsf.iri.runner.KvEnum tellMeWhatIsTheKey(Class nowClass) {
        cn.zhonggu.barsf.iri.runner.KvEnum key;
        if (nowClass == Transaction.class) {
            key = cn.zhonggu.barsf.iri.runner.KvEnum.transaction;
        } else if (nowClass == Milestone.class) {
            key = cn.zhonggu.barsf.iri.runner.KvEnum.milestone;
        } else if (nowClass == StateDiff.class) {
            key = cn.zhonggu.barsf.iri.runner.KvEnum.statediff;
        } else if (nowClass == Address.class) {
            key = cn.zhonggu.barsf.iri.runner.KvEnum.address;
        } else if (nowClass == Approvee.class) {
            key = cn.zhonggu.barsf.iri.runner.KvEnum.approvee;
        } else if (nowClass == Bundle.class) {
            key = cn.zhonggu.barsf.iri.runner.KvEnum.bundle;
        } else if (nowClass == Tag.class) {
            key = cn.zhonggu.barsf.iri.runner.KvEnum.tag;
        } else {
            throw new RuntimeException();
        }
        return key;
    }

    private LinkedHashMap<Indexable, ArrayList<byte[]>> getFromRdb(Class<?> model, Indexable index) throws Exception {
        LinkedHashMap<Indexable, ArrayList<byte[]>> retList = new LinkedHashMap<>();
        RocksIterator iterator = db.newIterator(classTreeMap.get(model));
        if (index == Hash.NULL_HASH) {
            if (model == Milestone.class) {
                index = new IntegerIndex(0);
            }
            iterator.seekToFirst();
        } else {
            iterator.seek(index.bytes());
        }

        int count = 0;
        while (iterator.isValid() && count < MAX_BATCH_SIZE) {
            count++;
            ArrayList<byte[]> values = new ArrayList<>();
            Indexable indexable = index.getClass().newInstance();
            indexable.read(iterator.key());
            values.add(0, iterator.value());
            ColumnFamilyHandle referenceHandle = metadataReference.get(model);
            if (referenceHandle != null) {
                byte[] value2 = db.get(referenceHandle, iterator.key());
                values.add(1, value2);
            }
            retList.put(indexable, values);

            iterator.next();
        }
        iterator.close();

        return retList;
    }

    public void selfDebug(Class model) {
        LinkedHashMap<Indexable, Persistable> retList = new LinkedHashMap<Indexable, Persistable>();
        RocksIterator iterator = db.newIterator(classTreeMap.get(model));
        iterator.seekToFirst();
        log.info(" 数据检查:" + model);
        int count = 0;
        log.info("正序");
        while (iterator.isValid() && count < MAX_BATCH_SIZE) {
            count++;
            log.info(" ready size :" + iterator.value().length);
            iterator.next();
            log.info("滚动到下一个: " + iterator.isValid());
        }
        count = 0;
        log.info("倒序");
        iterator.seekToLast();
        while (iterator.isValid() && count < MAX_BATCH_SIZE) {
            count++;
            log.info(" ready size :" + iterator.value().length);
            iterator.prev();
            log.info("滚动到上一个: " + iterator.isValid());
        }


        iterator.close();
    }


    public void initRdb() throws Exception {
        try {
            RocksDB.loadLibrary();
        } catch (Exception e) {
            if (SystemUtils.IS_OS_WINDOWS) {
                log.error("Error loading RocksDB library. " +
                        "Please ensure that " +
                        "Microsoft Visual C++ 2015 Redistributable Update 3 " +
                        "is installed and updated");
            }
            throw e;
        }


        RocksEnv.getDefault()
                .setBackgroundThreads(Runtime.getRuntime().availableProcessors() / 2, RocksEnv.FLUSH_POOL)
                .setBackgroundThreads(Runtime.getRuntime().availableProcessors() / 2, RocksEnv.COMPACTION_POOL)
        /*
                .setBackgroundThreads(Runtime.getRuntime().availableProcessors())
        */
        ;

        options = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setDbLogDir("mainnetdb-test.log")
                .setMaxLogFileSize(SizeUnit.MB)
                .setMaxManifestFileSize(SizeUnit.MB)
                .setMaxOpenFiles(10000)
                .setMaxBackgroundCompactions(1)
                /*
                .setBytesPerSync(4 * SizeUnit.MB)
                .setMaxTotalWalSize(16 * SizeUnit.MB)
                */
        ;
        options.setMaxSubcompactions(Runtime.getRuntime().availableProcessors());

        bloomFilter = new BloomFilter(10);
        PlainTableConfig plainTableConfig = new PlainTableConfig();
        BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig().setFilter(bloomFilter);
        blockBasedTableConfig
                .setFilter(bloomFilter)
                .setCacheNumShardBits(2)
                .setBlockSizeDeviation(10)
                .setBlockRestartInterval(16)
                .setBlockCacheSize(100000 * SizeUnit.KB)
                .setBlockCacheCompressedNumShardBits(10)
                .setBlockCacheCompressedSize(32 * SizeUnit.KB)
                /*
                .setHashIndexAllowCollision(true)
                .setCacheIndexAndFilterBlocks(true)
                */
        ;
        options.setAllowConcurrentMemtableWrite(true);

        MemTableConfig hashSkipListMemTableConfig = new HashSkipListMemTableConfig()
                .setHeight(9)
                .setBranchingFactor(9)
                .setBucketCount(2 * SizeUnit.MB);
        MemTableConfig hashLinkedListMemTableConfig = new HashLinkedListMemTableConfig().setBucketCount(100000);
        MemTableConfig vectorTableConfig = new VectorMemTableConfig().setReservedSize(10000);
        MemTableConfig skipListMemTableConfig = new SkipListMemTableConfig();


        MergeOperator mergeOperator = new StringAppendOperator();
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions()
                .setMergeOperator(mergeOperator)
                .setTableFormatConfig(blockBasedTableConfig)
                .setMaxWriteBufferNumber(2)
                .setWriteBufferSize(2 * SizeUnit.MB)
                /*
                .setCompactionStyle(CompactionStyle.UNIVERSAL)
                .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                */;
        //columnFamilyOptions.setMemTableConfig(hashSkipListMemTableConfig);

        //List<ColumnFamilyDescriptor> familyDescriptors = columnFamilyNames.stream().map(name -> new ColumnFamilyDescriptor(name.getBytes(), columnFamilyOptions)).collect(Collectors.toList());
        //familyDescriptors.add(0, new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = columnFamilyNames.stream().map(name -> new ColumnFamilyDescriptor(name.getBytes(), columnFamilyOptions)).collect(Collectors.toList());
        db = RocksDB.open(options, "mainnetdb", columnFamilyDescriptors, columnFamilyHandles);
        db.enableFileDeletions(true);

        fillModelColumnHandles();
    }
    private final List<String> columnFamilyNames = Arrays.asList(
            new String(RocksDB.DEFAULT_COLUMN_FAMILY),
            "transaction",
            "transaction-metadata",
            "milestone",
            "stateDiff",
            "address",
            "approvee",
            "bundle",
            "obsoleteTag",
            "tag"
    );

    private void fillModelColumnHandles() throws Exception {
        int i = 0;
        transactionHandle = columnFamilyHandles.get(++i);
        transactionMetadataHandle = columnFamilyHandles.get(++i);
        milestoneHandle = columnFamilyHandles.get(++i);
        stateDiffHandle = columnFamilyHandles.get(++i);
        addressHandle = columnFamilyHandles.get(++i);
        approveeHandle = columnFamilyHandles.get(++i);
        bundleHandle = columnFamilyHandles.get(++i);
        obsoleteTagHandle = columnFamilyHandles.get(++i);
        tagHandle = columnFamilyHandles.get(++i);

        for (; ++i < columnFamilyHandles.size(); ) {
            db.dropColumnFamily(columnFamilyHandles.get(i));
        }
    }



    private final AtomicReference<Map<Class<?>, Indexable>> lastIndexMap = new AtomicReference<>();
    private Map<Class<?>, ColumnFamilyHandle> classTreeMap;
    private Map<Class<?>, ColumnFamilyHandle> metadataReference;


    private void initClassTreeMap() {
        Map<Class<?>, ColumnFamilyHandle> classMap = new LinkedHashMap<>();
        classMap.put(Transaction.class, transactionHandle);
        classMap.put(Milestone.class, milestoneHandle);
        classMap.put(StateDiff.class, stateDiffHandle);
        classMap.put(Address.class, addressHandle);
        classMap.put(Approvee.class, approveeHandle);
        classMap.put(Bundle.class, bundleHandle);
        classMap.put(ObsoleteTag.class, obsoleteTagHandle);
        classMap.put(Tag.class, tagHandle);
        classTreeMap = classMap;

        Map<Class<?>, ColumnFamilyHandle> metadataHashMap = new HashMap<>();
        metadataHashMap.put(Transaction.class, transactionMetadataHandle);
        metadataReference = metadataHashMap;
    }

    private void initLastOneMap() {

        Map<Class<?>, Indexable> lastIndexMAp = new HashMap<>();
        Kv transactionIndex = kvProvider.getValue(cn.zhonggu.barsf.iri.runner.KvEnum.transaction.name());
        lastIndexMAp.put(Transaction.class, transactionIndex == null ? Hash.NULL_HASH : new Hash(transactionIndex.getValueStr()));
        Kv milestoneIndex = kvProvider.getValue(cn.zhonggu.barsf.iri.runner.KvEnum.milestone.name());
        lastIndexMAp.put(Milestone.class, milestoneIndex == null ? new IntegerIndex(0) : new IntegerIndex(Integer.parseInt(milestoneIndex.getValueStr())));
        Kv stateDiffIndex = kvProvider.getValue(cn.zhonggu.barsf.iri.runner.KvEnum.statediff.name());
        lastIndexMAp.put(StateDiff.class, stateDiffIndex == null ? Hash.NULL_HASH : new Hash(stateDiffIndex.getValueStr()));
        Kv addressIndex = kvProvider.getValue(cn.zhonggu.barsf.iri.runner.KvEnum.address.name());
        lastIndexMAp.put(Address.class, addressIndex == null ? Hash.NULL_HASH : new Hash(addressIndex.getValueStr()));
        Kv approveeIndex = kvProvider.getValue(cn.zhonggu.barsf.iri.runner.KvEnum.approvee.name());
        lastIndexMAp.put(Approvee.class, approveeIndex == null ? Hash.NULL_HASH : new Hash(approveeIndex.getValueStr()));
        Kv bundleIndex = kvProvider.getValue(cn.zhonggu.barsf.iri.runner.KvEnum.bundle.name());
        lastIndexMAp.put(Bundle.class, bundleIndex == null ? Hash.NULL_HASH : new Hash(bundleIndex.getValueStr()));
        Kv tagIndex = kvProvider.getValue(cn.zhonggu.barsf.iri.runner.KvEnum.tag.name());
        lastIndexMAp.put(Tag.class, tagIndex == null ? Hash.NULL_HASH : new Hash(tagIndex.getValueStr()));
        log.info("last one map init success");
        lastIndexMap.set(lastIndexMAp);
    }


    private void initSubProider() {
        this.transactionProvider = TransactionProvider.getInstance();
        this.milestoneProvider = MilestoneProvider.getInstance();
        this.stateDiffProvider = StateDiffProvider.getInstance();
        this.addressProvider = AddressProvider.getInstance();
        this.approveeProvider = ApproveeProvider.getInstance();
        this.bundleProvider = BundleProvider.getInstance();
        this.tagProvider = TagProvider.getInstance();
        this.kvProvider = KvProvider.getInstance();
    }

    public void shutdown() {
        for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            IOUtils.closeQuietly(columnFamilyHandle::close);
        }
        IOUtils.closeQuietly(db::close, options::close, bloomFilter::close);
    }
}

enum KvEnum {
    transaction, milestone, statediff, address, bundle, approvee, tag
}
