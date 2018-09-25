package cn.zhonggu.barsf.iri.service.dto;

import com.iota.iri.service.dto.AbstractResponse;

import java.util.List;

/**
 * Created by bfq on 2018/5/31.
 */
public class GetBarsfBalancesResponse extends AbstractResponse {
    private List<String> balances;
    private List<Integer> lastChangedMilestoneIndexes;

    //获取地址余额时，对应的barsf里程碑的hash值
    private List<String> reference;

    //里程碑对应的序号
    private int milestoneIndex;

    public GetBarsfBalancesResponse(List<String> balances, List<String> reference, int milestoneIndex, List<Integer> lastChangedMilestoneIndexes) {
        this.balances = balances;
        this.reference = reference;
        this.milestoneIndex = milestoneIndex;
        this.lastChangedMilestoneIndexes = lastChangedMilestoneIndexes;
    }

    public List<String> getBalances() {
        return balances;
    }

    public void setBalances(List<String> balances) {
        this.balances = balances;
    }

    public List<String> getReference() {
        return reference;
    }

    public void setReference(List<String> reference) {
        this.reference = reference;
    }

    public int getMilestoneIndex() {
        return milestoneIndex;
    }

    public void setMilestoneIndex(int milestoneIndex) {
        this.milestoneIndex = milestoneIndex;
    }

    public List<Integer> getLastChangedMilestoneIndexes() {
        return lastChangedMilestoneIndexes;
    }

    public void setLastChangedMilestoneIndexes(List<Integer> lastChangedMilestoneIndexes) {
        this.lastChangedMilestoneIndexes = lastChangedMilestoneIndexes;
    }

    public static GetBarsfBalancesResponse create(List<String> balances, List<String> reference, int milestoneIndex, List<Integer> lastChangedMilestoneIndexes) {
        return new GetBarsfBalancesResponse(balances, reference, milestoneIndex, lastChangedMilestoneIndexes);
    }
}
