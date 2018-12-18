package com.thoughtworks.martdhis2sync.dao;

import com.google.gson.Gson;
import com.thoughtworks.martdhis2sync.model.Config;
import com.thoughtworks.martdhis2sync.model.LookupTable;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class MappingDAO {

    @Autowired
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @Value("classpath:sql/Searchable.sql")
    private Resource searchableResource;

    public Map<String, Object> getMapping(String mapping) {
        String sql = String.format("SELECT lookup_table, mapping_json, config FROM mapping WHERE mapping_name='%s'", mapping);

        return jdbcTemplate.queryForMap(sql);
    }

    public List<Map<String, Object>> getSearchableFields(String mappingName) throws IOException {
        StringBuilder columns = new StringBuilder();
        Gson gson = new Gson();
        Map<String, Object> mapping = getMapping(mappingName);

        gson.fromJson(mapping.get("config").toString(), Config.class).getSearchable().forEach(column -> {
            columns.append("\"");
            columns.append(column);
            columns.append("\"");
            columns.append(",");
        });

        if (StringUtils.isEmpty(columns.toString())) {
            return new ArrayList<>();
        }

        return jdbcTemplate.queryForList(
                String.format(
                        BatchUtil.convertResourceOutputToString(searchableResource),
                        columns.substring(0, columns.length() - 1),
                        gson.fromJson(mapping.get("lookup_table").toString(), LookupTable.class).getInstance(),
                        mappingName
                )
        );
    }
}
