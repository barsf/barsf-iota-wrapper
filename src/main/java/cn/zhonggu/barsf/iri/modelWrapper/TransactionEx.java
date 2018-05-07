package cn.zhonggu.barsf.iri.modelWrapper;

import javax.persistence.*;
import java.util.zip.DataFormatException;

import static cn.zhonggu.barsf.iri.utils.Converter.compress;
import static cn.zhonggu.barsf.iri.utils.Converter.uncompress;

/**
 * Created by ZhuDH on 2018/4/2.
 */
@Table(name = "t_transaction_trytes")
public class TransactionEx {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY, generator = "Mysql")
    private String hash;

    @Column(name = "trytes")
    public byte[] bytes;

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public byte[] getBytes() {
        return compress(bytes);
    }

    public void setBytes(byte[] bytes) {
        try {
            this.bytes = uncompress(bytes);
        } catch (DataFormatException e) {
            throw new RuntimeException();
        }
    }
}
