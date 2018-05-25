package cn.zhonggu.barsf.iri.utils;

import java.util.Arrays;

/**
 * Created by ZhuDH on 2018/5/25.
 */
public class TransactionHelper {
    public static String getSignature(byte[] trytesInByte) {
        return Converter.trytes(Arrays.copyOfRange(Converter.trits(trytesInByte), 0, 6561));
    }
}
