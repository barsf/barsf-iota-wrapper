package cn.zhonggu.barsf.iri.modelWrapper;

import javax.persistence.*;

/**
 * Created by ZhuDH on 2018/4/11.
 */
@Table(name = "t_kv")
public class Kv {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY, generator = "Mysql")
    private String keyStr;
    @Column
    private String valueStr;

    public Kv(String key, String value) {
        this.keyStr = key;
        this.valueStr = value;
    }

    public String getKeyStr() {
        return keyStr;
    }

    public void setKeyStr(String keyStr) {
        this.keyStr = keyStr;
    }

    public String getValueStr() {
        return valueStr;
    }

    public void setValueStr(String valueStr) {
        this.valueStr = valueStr;
    }
}
