package org.sj4axao.stater.kudu.config;

import com.sun.istack.internal.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kudu")
public class KuduProperties {
    public void setKuduAddress(@NotNull String kuduAddress) {
        if (kuduAddress == null)
            throw new NullPointerException("kuduAddress is marked @NonNull but is null");
        this.kuduAddress = kuduAddress;
    }

    public void setWorkerId(Long workerId) {
        this.workerId = workerId;
    }

    public void setDefaultDataBase(String defaultDataBase) {
        this.defaultDataBase = defaultDataBase;
    }

    public void setIgnoreDuplicateRows(boolean ignoreDuplicateRows) {
        this.ignoreDuplicateRows = ignoreDuplicateRows;
    }

    public void setFlushBufferSize(int flushBufferSize) {
        this.flushBufferSize = flushBufferSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof KuduProperties))
            return false;
        KuduProperties other = (KuduProperties) o;
        if (!other.canEqual(this))
            return false;
        Object this$kuduAddress = getKuduAddress(), other$kuduAddress = other.getKuduAddress();
        if ((this$kuduAddress == null) ? (other$kuduAddress != null) : !this$kuduAddress.equals(other$kuduAddress))
            return false;
        Object this$workerId = getWorkerId(), other$workerId = other.getWorkerId();
        if ((this$workerId == null) ? (other$workerId != null) : !this$workerId.equals(other$workerId))
            return false;
        Object this$defaultDataBase = getDefaultDataBase(), other$defaultDataBase = other.getDefaultDataBase();
        return ((this$defaultDataBase == null) ? (other$defaultDataBase != null) : !this$defaultDataBase.equals(other$defaultDataBase)) ?
                false :
                ((isIgnoreDuplicateRows() != other.isIgnoreDuplicateRows()) ?
                        false :
                        ((getFlushBufferSize() != other.getFlushBufferSize()) ? false : (!(getBatchSize() != other.getBatchSize()))));
    }

    protected boolean canEqual(Object other) {
        return other instanceof KuduProperties;
    }

    public int hashCode() {
        int PRIME = 59;
        int result = 1;
        Object $kuduAddress = getKuduAddress();
        result = result * 59 + (($kuduAddress == null) ? 43 : $kuduAddress.hashCode());
        Object $workerId = getWorkerId();
        result = result * 59 + (($workerId == null) ? 43 : $workerId.hashCode());
        Object $defaultDataBase = getDefaultDataBase();
        result = result * 59 + (($defaultDataBase == null) ? 43 : $defaultDataBase.hashCode());
        result = result * 59 + (isIgnoreDuplicateRows() ? 79 : 97);
        result = result * 59 + getFlushBufferSize();
        return result * 59 + getBatchSize();
    }

    public String toString() {
        return "KuduProperties(kuduAddress=" + getKuduAddress() + ", workerId=" + getWorkerId() + ", defaultDataBase=" + getDefaultDataBase()
                + ", ignoreDuplicateRows=" + isIgnoreDuplicateRows() + ", flushBufferSize=" + getFlushBufferSize() + ", batchSize=" + getBatchSize()
                + ")";
    }

    public static final Long DEFAULT_WORK_ID = Long.valueOf(35L);

    @NotNull
    private String kuduAddress;

    @NotNull
    public String getKuduAddress() {
        return this.kuduAddress;
    }

    private Long workerId = DEFAULT_WORK_ID;

    private String defaultDataBase;

    public Long getWorkerId() {
        return this.workerId;
    }

    public String getDefaultDataBase() {
        return this.defaultDataBase;
    }

    private boolean ignoreDuplicateRows = true;

    public boolean isIgnoreDuplicateRows() {
        return this.ignoreDuplicateRows;
    }

    private int flushBufferSize = 10000;

    public int getFlushBufferSize() {
        return this.flushBufferSize;
    }

    private int batchSize = 16;

    public int getBatchSize() {
        return this.batchSize;
    }
}
