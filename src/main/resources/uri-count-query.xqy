xquery version "1.0-ml";

import module namespace dls = "http://marklogic.com/xdmp/dls" at "/MarkLogic/dls.xqy";

declare variable $URI as xs:string external;

fn:count(cts:uris( $URI, ("properties", "limit=10"), cts:and-not-query(
        cts:element-value-query(
                xs:QName("dls:latest"),"true",
                (),0),
        cts:element-value-query(
                xs:QName("dls:version-id"),"*",
                ("wildcarded"),0))
))