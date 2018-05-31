package cn.zhonggu.barsf.iri.modelWrapper;

import cn.zhonggu.barsf.iri.storage.innoDB.mybatis.DbHelper;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.model.Transaction;
import com.iota.iri.storage.Indexable;
import com.iota.iri.utils.Serializer;

import javax.persistence.*;

@Table(name = "t_transaction")
public class TransactionWrapper{
    public TransactionWrapper(){

    }
    public TransactionWrapper(Transaction t){
        this.bytes = t.bytes;
        this.address = t.address;
        this.bundle = t.bundle;
        this.trunk = t.trunk;
        this.branch = t.branch;
        this.obsoleteTag = t.obsoleteTag;
        this.value = t.value;
        this.currentIndex = t.currentIndex;
        this.lastIndex = t.lastIndex;
        this.timestamp = t.timestamp;
        this.tag = t.tag;
        this.attachmentTimestamp = t.attachmentTimestamp;
        this.attachmentTimestampUpperBound = t.attachmentTimestampUpperBound;
        this.attachmentTimestampLowerBound = t.attachmentTimestampLowerBound;
        this.validity = t.validity;
        this.type = t.type;
        this.arrivalTime = t.arrivalTime;
        this.parsed = t.parsed;
        this.solid = t.solid;
        this.height = t.height;
        this.sender = t.sender;
        this.snapshot = t.snapshot;
    }


    public static final int SIZE = 1604;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY, generator = "Mysql")
    private String hash;
//    private Boolean isProcessed = false;
    private Integer barsfTransaction = 0;
    // 注意,这个字段已经被分隔到另一张表中 [TransactionEx],当前它的作用是iri需要
    @Transient
    public byte[] bytes;
    @Transient
    private TransactionEx innerTrytes;
    public Hash address;
    public Hash bundle;
    public Hash trunk;
    public Hash branch;
    public Hash obsoleteTag;
    public Long value = 0L;
    public Long currentIndex = 0L;
    public Long lastIndex = 0L;
    public Long timestamp = 0L;

    public Hash tag;
    public Long attachmentTimestamp = 0L;
    @Column(name = "at_upper_bound")
    public Long attachmentTimestampUpperBound = 0L;
    @Column(name = "at_lower_bound")
    public Long attachmentTimestampLowerBound = 0L;

    public Integer validity = 0;
    public Integer type = 1;
    public Long arrivalTime = 0L;

    //public boolean confirmed = false;
    @Transient
    public Boolean parsed = true;
    public Boolean solid = false;
    public Long height = 0L;
    public String sender = "";
    public Integer snapshot = 0;

    public static TransactionWrapper emptyTrans(String hash) {
        TransactionWrapper emptyTrans = new TransactionWrapper();
        emptyTrans.hash = hash;
//        emptyTrans.isProcessed = null;
        emptyTrans.barsfTransaction = null;
        emptyTrans.address = null;
        emptyTrans.bundle = null;
        emptyTrans.trunk = null;
        emptyTrans.branch = null;
        emptyTrans.obsoleteTag = null;
        emptyTrans.value = null;
        emptyTrans.currentIndex = null;
        emptyTrans.lastIndex = null;
        emptyTrans.timestamp = null;
        emptyTrans.tag = null;
        emptyTrans.attachmentTimestamp = null;
        emptyTrans.attachmentTimestampUpperBound = null;
        emptyTrans.attachmentTimestampLowerBound = null;
        emptyTrans.validity = null;
        emptyTrans.type = null;
        emptyTrans.arrivalTime = null;
        emptyTrans.parsed = null;
        emptyTrans.solid = null;
        emptyTrans.height = null;
        emptyTrans.sender = null;
        emptyTrans.snapshot = null;
        return emptyTrans;
    }

    /* get方法 */
//    public Boolean getIsProcessed() {
//        return isProcessed;
//    }

    public Integer getBacNode() {
        return barsfTransaction;
    }

    public String getHash() {
        return hash;
    }


    public byte[] getBytes() {
        return bytes;
    }

    public String getAddress() {
        return address !=null? address.toString():null;
    }

    public String getBundle() {
        return  bundle!=null?bundle.toString():null;
    }

    public String getTrunk() {
        return trunk!=null?trunk.toString():null;
    }

    public String getBranch() {
        return branch!=null?branch.toString():null;
    }

    public String getObsoleteTag() {
        return obsoleteTag!=null?obsoleteTag.toString():null;
    }

    public Long getValue() {
        return value;
    }

    public Long getCurrentIndex() {
        return currentIndex;
    }

    public Long getLastIndex() {
        return lastIndex;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getTag() {
        return tag!=null?tag.toString().substring(0,27):null;
    }

    public Long getAttachmentTimestamp() {
        return attachmentTimestamp;
    }

    public Long getAttachmentTimestampUpperBound() {
        return attachmentTimestampUpperBound;
    }

    public Long getAttachmentTimestampLowerBound() {
        return attachmentTimestampLowerBound;
    }

    public Integer getValidity() {
        return validity;
    }

    public Integer getType() {
        return type;
    }

    public Long getArrivalTime() {
        return arrivalTime;
    }

    public Boolean isSolid() {
        return solid;
    }

    public Long getHeight() {
        return height;
    }

    public String getSender() {
        if (sender == null){
            return null;
        }
        if (sender.length() > 100) {
            return "sender to long...";
        }
        return sender;
    }

    public Integer getSnapshot() {
        return snapshot;
    }

    public TransactionEx getInnerTrytes() {
        return innerTrytes;
    }

    public Integer getBarsfTransaction() {
        return barsfTransaction;
    }

    /* set方法 */

    public void setAddress(String address) {
        this.address = new Hash(address);
    }

    public void setBundle(String bundle) {
        this.bundle = new Hash(bundle);
    }

    public void setTrunk(String trunk) {
        this.trunk = new Hash(trunk);
    }

    public void setBranch(String branch) {
        this.branch = new Hash(branch);
    }

    public void setObsoleteTag(String obsoleteTag) {
        this.obsoleteTag = new Hash(obsoleteTag);
    }

    public void setTag(String tag) {
        this.tag = new Hash(tag);
    }

//    public void setIsProcessed(Boolean processed) {
//        isProcessed = processed;
//    }

    public void setBarsfTransaction(Integer barsfTransaction) {
        this.barsfTransaction = barsfTransaction;
    }

    // 从数据库中读取数据时,需要通过这个方法setTrytes 内部对bytes做了联动
    public void setInnerTrytes(TransactionEx innerTrytes) {
        this.innerTrytes = innerTrytes;
        read(innerTrytes.bytes);
    }

    public Boolean getParsed() {
        return parsed;
    }

    public void setParsed(Boolean parsed) {
        this.parsed = parsed;
    }

    /**
     *   使用hash的内部数据,填充model内部属性
     *   filling之后才能用mybatis数据库保存对象
     *   @param index 由上层传入的参数
     */
    public void filling(Indexable index) {
        this.hash = DbHelper.converterIndexableToStr(index);
        if (innerTrytes == null) {
            innerTrytes = new TransactionEx();
            innerTrytes.setHash(this.hash);
        }
        innerTrytes.bytes = this.bytes;
    }


    public byte[] bytes() {
        return bytes;
    }

    public void read(byte[] bytes) {
        if (bytes != null) {
            this.bytes = new byte[SIZE];
            System.arraycopy(bytes, 0, this.bytes, 0, SIZE);
            this.type = TransactionViewModel.FILLED_SLOT;
        }
    }

    public void readMetadata(byte[] bytes) {
        int i = 0;
        if (bytes != null) {
            address = new Hash(bytes, i, Hash.SIZE_IN_BYTES);
            i += Hash.SIZE_IN_BYTES;
            bundle = new Hash(bytes, i, Hash.SIZE_IN_BYTES);
            i += Hash.SIZE_IN_BYTES;
            trunk = new Hash(bytes, i, Hash.SIZE_IN_BYTES);
            i += Hash.SIZE_IN_BYTES;
            branch = new Hash(bytes, i, Hash.SIZE_IN_BYTES);
            i += Hash.SIZE_IN_BYTES;
            obsoleteTag = new Hash(bytes, i, Hash.SIZE_IN_BYTES);
            i += Hash.SIZE_IN_BYTES;
            value = Serializer.getLong(bytes, i);
            i += Long.BYTES;
            currentIndex = Serializer.getLong(bytes, i);
            i += Long.BYTES;
            lastIndex = Serializer.getLong(bytes, i);
            i += Long.BYTES;
            timestamp = Serializer.getLong(bytes, i);
            i += Long.BYTES;

            tag = new Hash(bytes, i, Hash.SIZE_IN_BYTES);
            i += Hash.SIZE_IN_BYTES;
            attachmentTimestamp = Serializer.getLong(bytes, i);
            i += Long.BYTES;
            attachmentTimestampLowerBound = Serializer.getLong(bytes, i);
            i += Long.BYTES;
            attachmentTimestampUpperBound = Serializer.getLong(bytes, i);
            i += Long.BYTES;

            validity = Serializer.getInteger(bytes, i);
            i += Integer.BYTES;
            type = Serializer.getInteger(bytes, i);
            i += Integer.BYTES;
            arrivalTime = Serializer.getLong(bytes, i);
            i += Long.BYTES;
            height = Serializer.getLong(bytes, i);
            i += Long.BYTES;
            /*
            confirmed = bytes[i] == 1;
            i++;
            */
            solid = bytes[i] == 1;
            i++;
            snapshot = Serializer.getInteger(bytes, i);
            i += Integer.BYTES;
            byte[] senderBytes = new byte[bytes.length - i];
            if (senderBytes.length != 0) {
                System.arraycopy(bytes, i, senderBytes, 0, senderBytes.length);
            }
            sender = new String(senderBytes);
            parsed = true;
        }
    }

    public boolean merge() {
        return false;
    }


    public Transaction toTransaction(){
        // 22 个变量
        Transaction transaction = new Transaction();
        transaction.bytes = this.bytes;
        transaction.address = this.address;
        transaction.bundle = this.bundle;
        transaction.trunk = this.trunk;
        transaction.branch = this.branch;
        transaction.obsoleteTag = this.obsoleteTag;
        transaction.value = this.value;
        transaction.currentIndex = this.currentIndex;
        transaction.lastIndex = this.lastIndex;
        transaction.timestamp = this.timestamp;
        transaction.tag = this.tag;
        transaction.attachmentTimestamp = this.attachmentTimestamp;
        transaction.attachmentTimestampUpperBound = this.attachmentTimestampUpperBound;
        transaction.attachmentTimestampLowerBound = this.attachmentTimestampLowerBound;
        transaction.validity = this.validity;
        transaction.type = this.type;
        transaction.arrivalTime = this.arrivalTime;
        transaction.parsed = this.parsed;
        transaction.solid = this.solid;
        transaction.height = this.height;
        transaction.sender = this.sender;
        transaction.snapshot = this.snapshot;
        return transaction;
    }
}
