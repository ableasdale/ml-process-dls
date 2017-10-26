package com.marklogic.support.processdls;

import com.marklogic.xcc.*;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Processor {

    private static Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static String HOST_XCC_URI = null;
    private static Map<String, String> documentMap;
    private static String lastProcessedURI = "/";
    private static String batchQuery = null;
    private static String documentHistoryQuery = null;
    private static String uriVersionsQuery = null;
    private static boolean complete = false;
    private static ExecutorService es = Executors.newFixedThreadPool(Config.THREAD_POOL_SIZE);
    private static ContentSource cs = null;


    private static ResultSequence getBatch(String uri, Session sourceSession) {
        String query = null;
        try {
            query = new String(Files.readAllBytes(Paths.get(Config.CTS_URI_COUNT_QUERY)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Request request = sourceSession.newAdhocQuery(query);
        request.setNewStringVariable("URI", uri);
        ResultSequence rs = null;
        try {
            rs = sourceSession.submitRequest(request);
        } catch (RequestException e) {
            e.printStackTrace();
        }
        boolean moreThanOne = (Integer.parseInt(rs.asString()) > 1);

        if (moreThanOne) {
            Request r2 = sourceSession.newAdhocQuery(batchQuery);
            r2.setNewStringVariable("URI", uri);
            LOG.debug("URI passed to batch query: " + uri);
            try {
                rs = sourceSession.submitRequest(r2);
            } catch (RequestException e) {
                e.printStackTrace();
            }
        } else {
            LOG.debug(String.format("Down to the last item in the list: %d URI returned", Integer.parseInt(rs.asString())));
            // Down to last item, so close the result sequence and set the complete flag to true
            complete = true;
            rs.close();
            rs = null;
        }
        return rs;
    }

    public static void main(String[] args) {
        documentMap = new ConcurrentHashMap<>();

        try {
            Configurations configs = new Configurations();
            Configuration config = configs.properties(new File("config.properties"));
            HOST_XCC_URI = config.getString("source.uri");
            LOG.debug(String.format("Configured Input XCC URI: %s", HOST_XCC_URI));
            LOG.info(String.format("running URIs query: %s", lastProcessedURI));
            documentHistoryQuery = new String(Files.readAllBytes(Paths.get(Config.DOCUMENT_HISTORY_QUERY)));
            batchQuery = new String(Files.readAllBytes(Paths.get(Config.CTS_URIS_QUERY)));
            uriVersionsQuery = new String(Files.readAllBytes(Paths.get(Config.URI_VERSIONS_QUERY)));
            cs = ContentSourceFactory.newContentSource(URI.create(HOST_XCC_URI));
            Session sourceSession = cs.newSession();
            while (!complete) {
                LOG.debug("Itemlist not complete - more URIs still to process.");
                processResultSequence(documentMap, getBatch(lastProcessedURI, sourceSession));
            }

            // Stop the thread pool
            es.shutdown();
            // Drain the queue
            while (!es.isTerminated()) {
                try {
                    es.awaitTermination(72, TimeUnit.HOURS);
                } catch (InterruptedException e) {
                    LOG.error("Exception caught: ", e);
                }
            }

            sourceSession.close();

        } catch (XccConfigException | RequestException | IOException | ConfigurationException e) {
            LOG.error("Exception caught: ", e);
        }
        LOG.info("Total documents examined: " + documentMap.size());

        // process report
        for (String s : documentMap.keySet()) {
            String data = documentMap.get(s);
            String[] data2 = data.split("~");
            LOG.debug(String.format("URI: %s Revisions: %s Total latest false: %s Total latest true: %s", s, data2[0], data2[1], data2[2]));
            if (Integer.parseInt(data2[2]) > 1) {
                LOG.info(String.format("URI found with more than one 'dls:latest' property: %s", s));
            }
        }
    }


    private static void processResultSequence(Map<String, String> documentMap, ResultSequence rs) throws RequestException {
        if (rs != null) {
            if (rs.size() <= 1) {
                LOG.debug("Only one item returned - is this the end of the run?");
                complete = true;
            }

            LOG.debug(String.format("Starting with a batch of %d documents", rs.size()));

            Iterator<ResultItem> resultItemIterator = rs.iterator();
            String currentUri = "/";
            while (resultItemIterator.hasNext()) {
                ResultItem i = resultItemIterator.next();
                currentUri = i.asString();
                es.execute(new DLSHistoryProcessor(i.asString()));
            }

            lastProcessedURI = currentUri;
            LOG.debug(String.format("Last URI in batch of %s URI(s): %s%s%s", rs.size(), Config.ANSI_BLUE, lastProcessedURI, Config.ANSI_RESET));
            if (rs.size() == 0) {
                complete = true;
            }
            rs.close();
        }
    }


    public static class DLSHistoryProcessor implements Runnable {

        String uri;

        DLSHistoryProcessor(String uri) {
            LOG.debug(String.format("Working on: %s", uri));
            this.uri = uri;
        }

        public void run() {
            try {
                Session dlsSession = cs.newSession();
                Request dlsRequest = dlsSession.newAdhocQuery(documentHistoryQuery);
                dlsRequest.setNewStringVariable("URI", uri);
                ResultSequence dlsRs = dlsSession.submitRequest(dlsRequest);


                String[] data = dlsRs.asString().split("~");
                if (Integer.parseInt(data[2]) > 1) {
                    LOG.info(String.format("Making a change to: %s", uri));
                    Session d2 = cs.newSession();
                    Request d2r = d2.newAdhocQuery(uriVersionsQuery);
                    d2r.setNewStringVariable("URI", uri);
                    ResultSequence d2rs = d2.submitRequest(d2r);
                    // iterate
                    Iterator<ResultItem> resultItemIterator = d2rs.iterator();
                    while (resultItemIterator.hasNext()) {
                        String item = resultItemIterator.next().asString();
                        if (!resultItemIterator.hasNext()){
                            LOG.info("last item: "+item + " no processing");
                        } else {
                            LOG.info("ITEM: " + item);
                            if(item.contains("true")){
                                LOG.info("MUST FIX: "+item);
                            }
                        }

                    }
                    d2rs.close();
                    d2.close();

                    // xdmp:document-properties("/10988734552927421120_xml_versions/3-10988734552927421120.xml")
                    // /content/assets/2017/06/29/16/06/6013a428-75f9-b729-fe82-db3abe8c4278a3311b88.xml
                    // /content/assets/2017/06/29/16/06/6013a428-75f9-b729-fe82-db3abe8c4278a3311b88_xml_versions/1-6013a428-75f9-b729-fe82-db3abe8c4278a3311b88.xml
                }

                documentMap.put(uri, dlsRs.asString());
                dlsRs.close();
                dlsSession.close();
            } catch (RequestException e) {
                LOG.error(String.format("Exception caught while processing URI: %s", uri), e);
            }
        }
    }


}