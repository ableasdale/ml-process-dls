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
    private static boolean complete = false;
    private static ExecutorService es = Executors.newFixedThreadPool(Config.THREAD_POOL_SIZE);
    private static ContentSource cs = null;


    private static ResultSequence getBatch(String uri, Session sourceSession) {
        String query = String.format("fn:count(cts:uris( \"%s\", ('limit=1000')))", uri);
        LOG.debug(String.format("Query: %s", query));
        Request request = sourceSession.newAdhocQuery(query);
        ResultSequence rs = null;
        try {
            rs = sourceSession.submitRequest(request);
        } catch (RequestException e) {
            e.printStackTrace();
        }
        boolean moreThanOne = (Integer.parseInt(rs.asString()) > 1);

        if (moreThanOne) {
            request = sourceSession.newAdhocQuery(batchQuery.replace("(),", String.format("\"%s\",", uri)));
            try {
                rs = sourceSession.submitRequest(request);
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

        Configurations configs = new Configurations();
        try {
            Configuration config = configs.properties(new File("config.properties"));
            HOST_XCC_URI = config.getString("source.uri");
            LOG.debug(String.format("Configured Input XCC URI: %s", HOST_XCC_URI));
        } catch (ConfigurationException cex) {
            LOG.error("Configuration issue: ", cex);
        }

        try {
            batchQuery = Config.CTS_URIS_QUERY;
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

        } catch (XccConfigException | RequestException e) {
            LOG.error("Exception caught: ", e);
        }
    }

    private static void runFinalReport(Map<String, String> documentMap) {
        LOG.info("Generating report ...");
        for (String s : documentMap.keySet()) {
           // MarkLogicDocument m = documentMap.get(s);
            StringBuilder sb = new StringBuilder();
            //sb.append("URI:\t").append(Config.ANSI_BLUE).append(m.getUri()).append(Config.ANSI_RESET).append("\tSource MD5:\t").append(m.getSourceMD5());
           /* if (m.getSourceMD5().equals(m.getTargetMD5())) {
                sb.append("\tTarget MD5:\t").append(Config.ANSI_GREEN).append(m.getTargetMD5()).append(Config.ANSI_RESET);
            } else if (StringUtils.isEmpty(m.getTargetMD5())) {
                sb.append(Config.ANSI_GREEN).append("\tURI synchronised").append(Config.ANSI_RESET);
            } else {
                sb.append("\tTarget MD5:\t").append(Config.ANSI_RED).append(m.getTargetMD5()).append(Config.ANSI_RESET);
            }
*/
           LOG.info(sb.toString());
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
                //LOG.debug("X:"+i.asString());
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

    public static class DocumentCopier implements Runnable {

        private void writeDocument() {
            LOG.info("Doing something in a thread...");
        }
        /*
        private MarkLogicDocument md;

        DocumentCopier(MarkLogicDocument md) {
            LOG.debug(String.format("Working on: %s", md.getUri()));
            this.md = md;
        }

        private String buildAdHocQuery(String uri) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format(Config.FN_DOC, uri)).append(", ");
            sb.append(String.format(Config.PROPERTIES_QUERY, uri)).append(", ");
            sb.append(String.format(Config.PERMISSIONS_QUERY, uri)).append(", ");
            sb.append(String.format(Config.COLLECTIONS_QUERY, uri));
            return sb.toString();
        }

        private void writeDocument() {
            LOG.debug(String.format("Writing Document %s", md.getUri()));
            Session s = csSource.newSession();
            Session t = csTarget.newSession();

            LOG.debug(String.format("We need to copy this doc (%s) over", md.getUri()));

            Request sourceDocReq = s.newAdhocQuery(buildAdHocQuery(md.getUri()));
            ResultSequence rsS = null;
            try {
                rsS = s.submitRequest(sourceDocReq);
                LOG.debug(String.format("Collection size: %d", rsS.size()));
                LOG.debug(String.format("Full resultset: %s", rsS.asString("  ~ | *** | ~ ")));

                // TODO - also copy metadata, qualities, etc?

                ContentCreateOptions co = ContentCreateOptions.newXmlInstance();
                // Only copy collections if there are any to copy:
                LOG.debug(String.format("Collections: %d", rsS.resultItemAt(3).asString().length()));
                if(rsS.resultItemAt(3).asString().length() > 0) {
                    co.setCollections(rsS.resultItemAt(3).asString().split("~"));
                }
                //co.setMetadata();

                Content content = ContentFactory.newContent(md.getUri(), rsS.resultItemAt(0).asString(), co);
                t.insertContent(content);

                LOG.debug(String.format("xdmp:document-set-properties(\"%s\", %s)", md.getUri(), rsS.resultItemAt(1).asString()));

                Request targetProps = t.newAdhocQuery(String.format("xdmp:document-set-properties(\"%s\", %s)", md.getUri(), rsS.resultItemAt(1).asString()));
                t.submitRequest(targetProps);

                Request targetPerms = t.newAdhocQuery(String.format("xdmp:document-set-permissions(\"%s\", %s)", md.getUri(), rsS.resultItemAt(2).asString()));
                t.submitRequest(targetPerms);

            } catch (RequestException e) {
                LOG.error("Exception caught: ", e);
            }

            s.close();
            t.close();
        } */

        public void run() {
            writeDocument();
        }
    }
}