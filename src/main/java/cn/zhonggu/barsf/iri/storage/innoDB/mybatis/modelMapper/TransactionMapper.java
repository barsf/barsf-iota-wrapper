package cn.zhonggu.barsf.iri.storage.innoDB.mybatis.modelMapper;

import cn.zhonggu.barsf.iri.modelWrapper.TransactionWrapper;
import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.modelMapper.base.IotaEntryBaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;


/**
 * Created by ZhuDH on 2018/3/29.
 */
public interface TransactionMapper extends IotaEntryBaseMapper<TransactionWrapper> {

    @Select("select * from t_transaction where is_processed = 0 and snapshot > 0 limit #{batchSize}")
    List<TransactionWrapper> selectNeedProcess(@Param("batchSize") int batchSize);

    // 专为地址查询TransHash优化
    @Select("SELECT `hash` FROM t_transaction where address = #{address}")
    List<String> selectHashesByAddress(@Param("address") String address);
}
