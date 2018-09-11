package com.thoughtworks.martdhis2sync.response;

import lombok.Data;

@Data
public class Pager {
    private int page;
    private int pageCount;
    private int total;
    private int pageSize;
    private String nextPage;
    private String prevPage;
}
