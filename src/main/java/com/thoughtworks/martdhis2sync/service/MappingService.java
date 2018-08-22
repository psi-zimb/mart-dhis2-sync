package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.dao.MappingDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class MappingService {

    @Autowired
    private MappingDAO mappingDAO;

    public Map<String, Object> getMapping(String mapping) {
        return mappingDAO.getMapping(mapping);
    }
}
