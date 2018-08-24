package com.thoughtworks.martdhis2sync;

import java.lang.reflect.Field;

public class CommonTestHelper {

    public static void setValuesForMemberFields(Object classInstance, String fieldName, Object valueForMemberField)
            throws NoSuchFieldException, IllegalAccessException {
        setField(classInstance, valueForMemberField, classInstance.getClass().getDeclaredField(fieldName));
    }

    private static void setField(Object classInstance, Object valueForMemberField, Field field)
            throws IllegalAccessException {
        field.setAccessible(true);
        field.set(classInstance, valueForMemberField);
    }
}
