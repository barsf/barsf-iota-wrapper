package cn.zhonggu.barsf.iri.storage.innoDB.mybatis.modelMapper.base;

import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import tk.mybatis.mapper.common.Mapper;

import java.util.Set;

/**
 * Created by ZhuDH on 2018/3/29.
 */
public interface IotaEntryBaseMapper<T> extends Mapper<T> {

    @InsertProvider(type = IotaEntryBaseMapperProvider.class, method = "dynamicSQL")
    int insertOrUpdate(@Param(value = IotaEntryBaseMapperProvider.ENETY_SELF) T record);

    @SelectProvider(type = IotaEntryBaseMapperProvider.class, method = "dynamicSQL")
    T latest();

    @SelectProvider(type = IotaEntryBaseMapperProvider.class, method = "dynamicSQL")
    T first();

    @SelectProvider(type = IotaEntryBaseMapperProvider.class, method = "dynamicSQL")
    T next(@Param("pk") String hash);

    @SelectProvider(type = IotaEntryBaseMapperProvider.class, method = "dynamicSQL")
    T previous(@Param("pk") String hash);

    @SelectProvider(type = IotaEntryBaseMapperProvider.class, method = "dynamicSQL")
    long count();

    @SelectProvider(type = IotaEntryBaseMapperProvider.class, method = "dynamicSQL")
    Set<String> keysStartingWith(@Param("pk") String hash);

}
