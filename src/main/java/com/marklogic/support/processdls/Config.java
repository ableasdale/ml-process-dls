package com.marklogic.support.processdls;

public class Config {

    public static final int THREAD_POOL_SIZE = 64;

    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_RESET = "\u001B[0m";

    public static final String CTS_URI_COUNT_QUERY = "src/main/resources/uri-count-query.xqy";
    public static final String CTS_URIS_QUERY = "src/main/resources/uri-query.xqy";
    public static final String DOCUMENT_HISTORY_QUERY = "src/main/resources/document-history.xqy";
    public static final String URI_VERSIONS_QUERY = "src/main/resources/uri-versions.xqy";
    public static final String CHANGE_DLS_LATEST_QUERY = "src/main/resources/change-dls-latest.xqy";

}