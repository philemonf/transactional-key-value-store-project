package ch.epfl.tkvs.transactionmanager.versioningunit;

import java.io.Serializable;


class PrefixedKey implements Serializable {

    private static final long serialVersionUID = 7099912040047138926L;
    private String prefix;
    private Serializable key;

    public PrefixedKey(String prefix, Serializable key) {
        this.prefix = prefix;
        this.key = key;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + ((prefix == null) ? 0 : prefix.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PrefixedKey other = (PrefixedKey) obj;
        if (key == null) {
            if (other.key != null)
                return false;
        } else if (!key.equals(other.key))
            return false;
        if (prefix == null) {
            if (other.prefix != null)
                return false;
        } else if (!prefix.equals(other.prefix))
            return false;
        return true;
    }
}
