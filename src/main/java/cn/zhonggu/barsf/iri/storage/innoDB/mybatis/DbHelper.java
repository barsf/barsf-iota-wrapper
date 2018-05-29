package cn.zhonggu.barsf.iri.storage.innoDB.mybatis;

import cn.zhonggu.barsf.iri.storage.innoDB.InnoDBPersistenceProvider;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.modelMapper.*;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.modelMapper.base.IotaEntryBaseMapper;
import com.iota.iri.model.Hash;
import com.iota.iri.model.IntegerIndex;
import com.iota.iri.storage.Indexable;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import tk.mybatis.mapper.entity.Config;
import tk.mybatis.mapper.mapperhelper.MapperHelper;
import tk.mybatis.mapper.mapperhelper.SelectKeyGenerator;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by ZhuDH on 2018/3/28.
 */
public class DbHelper {
    private static SqlSessionFactory ssFactory = null;
    private static Configuration conf= null;
    public synchronized static void initDbSource(String confPath) throws IOException {
        if (ssFactory == null) {
            InputStream inputs = InnoDBPersistenceProvider.class.getClassLoader().getResourceAsStream(confPath);
            ssFactory = new SqlSessionFactoryBuilder().build(inputs);
            conf = ssFactory.getConfiguration();
            conf.setUseGeneratedKeys(false);
            conf.setCacheEnabled(true);
            conf.addKeyGenerator("hash"+ SelectKeyGenerator.SELECT_KEY_SUFFIX,null);
            registMapper();
            MapperHelper mapperHelper = new MapperHelper();
            Config config = new Config();
            mapperHelper.setConfig(config);
            // 注册通用Mapper
            mapperHelper.registerMapper(IotaEntryBaseMapper.class);
            mapperHelper.processConfiguration(ssFactory.getConfiguration());
        }
    }
    
    private static void registMapper(){
        // 注册modelMapper
        conf.addMapper(TransactionMapper.class);
        conf.addMapper(TransactionTrytesMapper.class);
        conf.addMapper(TagMapper.class);
        conf.addMapper(StateDiffMapper.class);
        conf.addMapper(MilestoneMapper.class);
        conf.addMapper(BundleMapper.class);
        conf.addMapper(ApproveeMapper.class);
        conf.addMapper(AddressMapper.class);
        conf.addMapper(KvMapper.class);
    }

    public static SqlSessionFactory getSingletonSessionFactory(){
        if (ssFactory == null){
            throw new RuntimeException("dataBase haven't init");
        }

        return ssFactory;
    }

    public static void closeFactory(){
    }

    /*
    * 转换对象为Mysql 可以使用的主键值
    */
    public static String converterIndexableToStr(Indexable index) {
        if (index instanceof IntegerIndex) {
            return String.valueOf(((IntegerIndex) index).getValue());
        } else if (index instanceof Hash) {
            return index.toString();
        } else {
            throw new RuntimeException("invalided index type");
        }
    }
}
