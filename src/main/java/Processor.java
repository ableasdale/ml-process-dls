import com.marklogic.xcc.*;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
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
    private static String lastProcessedURI = "/";
    private static String batchQuery = null;
    private static String documentHistoryQuery = null;
    private static boolean complete = false;
    private static ExecutorService es = Executors.newFixedThreadPool(Config.THREAD_POOL_SIZE);
    private static ContentSource cs = null;


    private static ResultSequence getBatch(String uri, Session sourceSession) {
        LOG.info("running batch query");
        String query = null;
        try {
            query = new String(Files.readAllBytes(Paths.get(Config.CTS_URI_COUNT_QUERY)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        //String query = String.format("fn:count(cts:uris( \"%s\", ('limit=1000')))", uri);
        //LOG.debug(String.format("Query: %s", query));
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
            LOG.info("in more than one");
            Request r2 = sourceSession.newAdhocQuery(batchQuery);
            //LOG.info("batch query: "+ batchQuery);

            /* Session session = cs.newSession("Documents");
14
        Request request = session.newAdhocQuery(xquery);
15
        request.setNewStringVariable("node-string", xmlcontent);
*/
            r2.setNewStringVariable("URI", uri);
            LOG.info("URI passed to batch query: "+ uri);
            try {
                rs = sourceSession.submitRequest(r2);
                //LOG.info(rs.asString());
            } catch (RequestException e) {
                e.printStackTrace();
            }
        } else {
            LOG.info(String.format("Down to the last item in the list: %d URI returned", Integer.parseInt(rs.asString())));
            // Down to last item, so close the result sequence and set the complete flag to true
            complete = true;
            rs.close();
            rs = null;
        }
        return rs;
    }

    public static void main(String[] args) {
        Map<String, String> documentMap = new ConcurrentHashMap<>();

        try {
            Configurations configs = new Configurations();
            Configuration config = configs.properties(new File("config.properties"));
            HOST_XCC_URI = config.getString("source.uri");
            LOG.debug(String.format("Configured Input XCC URI: %s", HOST_XCC_URI));
            LOG.info("running URIs query: "+ lastProcessedURI);
            documentHistoryQuery = new String(Files.readAllBytes(Paths.get(Config.DOCUMENT_HISTORY_QUERY)));
            batchQuery = new String(Files.readAllBytes(Paths.get(Config.CTS_URIS_QUERY)));
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

            /*
            // xdmp:estimate on both master and target
            Request sReq = sourceSession.newAdhocQuery(Config.XDMP_ESTIMATE_QUERY);
            ResultSequence sRs = sourceSession.submitRequest(sReq);
            int sourceCount = Integer.parseInt(sRs.asString());

            Request tReq = targetSession.newAdhocQuery(Config.XDMP_ESTIMATE_QUERY);
            ResultSequence tRs = targetSession.submitRequest(tReq);
            int targetCount = Integer.parseInt(tRs.asString());

            if (sourceCount == targetCount) {
                LOG.info(String.format("%sSource and target number of documents match:\t(Source: %d)\t\t(Target: %d)%s", Config.ANSI_GREEN, sourceCount, targetCount, Config.ANSI_RESET));
            } else {
                LOG.error(String.format("%sSource and target number of documents do not match:\t(Source: %d)\t\t(Target: %d)%s", Config.ANSI_RED, sourceCount, targetCount, Config.ANSI_RESET));
            } */

            sourceSession.close();

/*
            if (RUN_FULL_REPORT) {
                LOG.debug("About to run the report...");
                runFinalReport(documentMap);
            } */

        } catch (XccConfigException | RequestException | IOException | ConfigurationException e ) {
            LOG.error("Exception caught: ", e);
        }
        LOG.info("Total documents examined: " + documentMap.size());

        // process report
        for (String s : documentMap.keySet()){
            String data = documentMap.get(s);
            String[] data2 = data.split("~");
            //LOG.info("URI: "+s+" Revisions: "+data2[0] +" Total latest false: "+ data2[1] + " Total latest true: " + data2[2]);
            if (Integer.parseInt(data2[2]) > 1 ){
                LOG.info(s);
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
            //Session tS = csTarget.newSession();

            Iterator<ResultItem> resultItemIterator = rs.iterator();
            String currentUri = "/";
            while (resultItemIterator.hasNext()) {
                ResultItem i = resultItemIterator.next();
                currentUri = i.asString();
                //LOG.debug("URI:"+i.asString());



                // DLS DOCUMENT HISTORY HERE

                Session dlsSession = cs.newSession();
                Request dlsRequest = dlsSession.newAdhocQuery(documentHistoryQuery);
                dlsRequest.setNewStringVariable("URI", i.asString());
                ResultSequence dlsRs = dlsSession.submitRequest(dlsRequest);

                documentMap.put(i.asString(), dlsRs.asString());



                //arkLogicDocument md = new MarkLogicDocument();
                //md.setUri(i.asString().substring(0, i.asString().lastIndexOf("~~~")));
                //md.setSourceMD5(i.asString().substring(i.asString().lastIndexOf("~~~") + 3));

                /*
                // Check target
                Request targetRequest = tS.newAdhocQuery(String.format("fn:doc-available(\"%s\")", md.getUri()));
                ResultSequence rsT = tS.submitRequest(targetRequest);
                LOG.debug(String.format("Is the doc available? %s", rsT.asString()));

                if (rsT.asString().equals("false")) {
                    if (md.getUri().equals("/")) {
                        LOG.info(String.format("Don't need to replicate an empty directory node: %s", md.getUri()));
                    } else {
                        LOG.debug(String.format("Doc not available in destination: %s", md.getUri()));
                        es.execute(new DocumentCopier(md));
                    }
                } else {
                    LOG.debug(String.format("Doc (%s) exists - getting the MD5 hash", md.getUri()));
                    Request targetDocReq = tS.newAdhocQuery(Config.MD5_ONELINE.replace("$URI", String.format("\"%s\"", md.getUri())));

                    ResultSequence rsT2 = tS.submitRequest(targetDocReq);
                    String md5sum = rsT2.asString();
                    LOG.debug(String.format("MD5 on target: %s MD5 on source: %s", md5sum, md.getSourceMD5()));
                    md.setTargetMD5(md5sum);
                    rsT2.close();

                    // Sychronise if the hashes don't match
                    if (!md.getTargetMD5().equals(md.getSourceMD5()) && !md.getUri().equals("/")) {
                        LOG.debug(String.format("MD5 hashes do not match for %s - copying document over", md.getUri()));
                        es.execute(new DocumentCopier(md));
                    }
                }

                rsT.close();
                documentMap.put(md.getUri(), md);
                lastProcessedURI = md.getUri(); */

            }

            lastProcessedURI = currentUri;
            //tS.close();
            LOG.info(String.format("Last URI in batch of %s URI(s): %s%s%s", rs.size(), Config.ANSI_BLUE, lastProcessedURI, Config.ANSI_RESET));
            if (rs.size() == 0) {
                complete = true;
            }
            rs.close();
        }
    }


}