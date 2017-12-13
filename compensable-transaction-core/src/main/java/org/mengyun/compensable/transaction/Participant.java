package org.mengyun.compensable.transaction;

import java.io.Serializable;

/**
 * Created by changming.xie on 8/23/17.
 */
public interface Participant extends Serializable {
    Participant getParent();

    void addChild(Participant participant);
}
