package drz.tmdb.cache;

import java.io.Serializable;

import drz.tmdb.ARIES_log.TransactionId;

public class V implements Serializable {

    public String valueString;

    private TransactionId lastDirtyOperation;

    public V(){
    }

    public V(String str){
        this.valueString = str;
    }

    public V(byte[] bytes){
        this.valueString = new String(bytes);
    }

    public byte[] serialize(){
        return this.valueString.getBytes();
    }

    @Override
    public boolean equals(Object obj){
        // 如果是同一个对象，直接返回true
        if (this == obj) {
            return true;
        }

        // 如果obj为null或者不是同一个类的实例，返回false
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        // 自定义相等性比较规则: 比较string
        return this.valueString.equals(((K)obj).keyString);
    }

    @Override
    public int hashCode(){
        return this.valueString.hashCode();
    }

    public void markDirty(boolean dirty, TransactionId tid) {
        lastDirtyOperation = dirty ? tid : null;
    }

    public TransactionId isDirty(){
        return lastDirtyOperation;
    }


}