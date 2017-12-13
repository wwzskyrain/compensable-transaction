package org.mengyun.compensable.transaction;

import java.io.Serializable;
import java.lang.reflect.Method;

/**
 * Created by changming.xie on 8/22/17.
 */
public interface Invocation extends Serializable {

    public Class getTargetClass();

    public String getMethodName();

    public Class[] getParameterTypes();

    public Object[] getArgs();

    Object proceed() throws Throwable;
}
