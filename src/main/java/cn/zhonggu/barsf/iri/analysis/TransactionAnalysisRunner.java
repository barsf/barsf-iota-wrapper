package cn.zhonggu.barsf.iri.analysis;

import cn.zhonggu.barsf.iri.modelWrapper.AddressWrapper;
import cn.zhonggu.barsf.iri.modelWrapper.TransactionWrapper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.subProvider.AddressProvider;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.subProvider.TransactionProvider;
import com.iota.iri.model.Address;
import com.iota.iri.model.Hash;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionAnalysisRunner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TransactionAnalysisRunner.class);

    private static final int BATCH_SIZE = 500;
    // TODO: 2018/5/4  判断内容
    private static final String BARSF_TRANS_TAG = "????????";
    private static final String BARSF_TRANS_ADDRESS = "!!!!!!!";


    private static final TransactionProvider tacProvider = TransactionProvider.getInstance();
    private static final AddressProvider addProvider = AddressProvider.getInstance();

    public static void selfCall() {
        ScheduledExecutorService singleTread = Executors.newSingleThreadScheduledExecutor();
        singleTread.scheduleWithFixedDelay(new TransactionAnalysisRunner(), 30, 30, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        log.info("TransactionAnalysis started ...");
        long start = System.currentTimeMillis();
        final AtomicInteger done = new AtomicInteger(0);
        SqlSession session = DbHelper.getSingletonSessionFactory().openSession(ExecutorType.SIMPLE, false);
        try {
            // 单线程  因此未加锁
            List<TransactionWrapper> manyTransactions = tacProvider.selectNeedProcessTrans(BATCH_SIZE, session);
            manyTransactions.forEach(tac -> {
                if (!tac.getIsProcessed()) {
                    try {
                        Address aAddress = (Address) addProvider.get(new Hash(tac.getAddress()));
                        AddressWrapper addressWrapper = new AddressWrapper(aAddress.set);
                        addressWrapper.setHash(tac.getAddress());
                        addressWrapper.setBalance(addressWrapper.getBalance() == null ? tac.getValue() : addressWrapper.getBalance() + tac.getValue());
                        boolean ret = addProvider.save(addressWrapper, new Hash(addressWrapper.getHash()), session);
                        if (!ret) {
                            throw new RuntimeException("should't happened!");
                        }

                        tac.setIsProcessed(true);

                        // 验证是不是barsf交易
                        if (isBacTransaction(tac)) {
                            // 状态为2表示初筛通过
                            tac.setBarsfTransaction(2);
                        } else {
                            // 状态为1表示不是barsf交易
                            tac.setBarsfTransaction(1);
                        }

                        tacProvider.updateByPrimaryKeySelective(tac, session);
                        done.incrementAndGet();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            session.commit();
            log.info("TransactionAnalysis finished, do <" + manyTransactions.size() + ">,done <" + done.get() + "> cost <" + (System.currentTimeMillis() - start) + ">");

        } catch (
                Exception e)

        {
            log.error("", e);
            session.rollback();
        } finally

        {
            session.close();
        }

    }


    private boolean isBacTransaction(TransactionWrapper tac) {
        // address检查
        if (!tac.getAddress().equals(BARSF_TRANS_ADDRESS)) {
            return false;
        }

        // tag包含特定字符
        if (!tac.getTag().startsWith(BARSF_TRANS_TAG)) {
            return false;
        }

        // 初筛通过
        return true;
    }

    public static void main(String[] args) {
        System.out.println("TYPPITYPPI99999999999999999".contains(BARSF_TRANS_TAG));
    }
}
