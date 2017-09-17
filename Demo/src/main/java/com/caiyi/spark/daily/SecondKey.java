package com.caiyi.spark.daily;

import com.google.common.base.Objects;
import scala.math.Ordered;

import java.io.Serializable;

/**
 * Created by Socean on 2016/12/20.
 */
public class SecondKey implements Ordered<SecondKey>, Serializable {

    private String first;

    private String second;

    public SecondKey(String first, String second) {
        this.first = first;
        this.second = second;
    }

    public int compare(SecondKey other) {
        if (this.first.compareTo(other.getFirst()) != 0) {
            return this.first.compareTo(other.getFirst());
        } else {
            return this.second.compareTo(other.getSecond());
        }
    }

    public boolean $less(SecondKey other) {
        if (this.first.compareTo(other.getFirst()) < 0) {
            return true;
        } else if (this.first.compareTo(other.getFirst()) == 0 && this.second.compareTo(other.getSecond()) < 0) {
            return true;
        }
        return false;
    }

    public boolean $greater(SecondKey other) {
        if (this.first.compareTo(other.getFirst()) > 0) {
            return true;
        } else if (this.first.compareTo(other.getFirst()) == 0 &&
                this.second.compareTo(other.getSecond()) > 0) {
            return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;

        SecondKey other = (SecondKey)obj;
        if(first.compareTo(other.first) != 0){
            return  false;
        }
        if(second.compareTo(other.second) != 0){
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(first,second);
    }

    public boolean $less$eq(SecondKey other) {
        if (this.$less(other)) {
            return true;
        } else if (this.first.compareTo(other.getFirst()) == 0 && this.second.compareTo(other.getSecond()) == 0) {
            return true;
        }
        return false;
    }

    public boolean $greater$eq(SecondKey other) {
        if (this.$greater(other)) {
            return true;
        } else if (this.first.compareTo(other.getFirst()) == 0 && this.second.compareTo(other.getSecond()) == 0) {
            return true;
        }
        return false;
    }

    public int compareTo(SecondKey other) {
        if (this.first.compareTo(other.getFirst()) != 0) {
            return this.first.compareTo(other.getFirst());
        } else {
            return this.second.compareTo(other.getSecond());
        }
    }


    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public String getSecond() {
        return second;
    }

    public void setSecond(String second) {
        this.second = second;
    }
}
