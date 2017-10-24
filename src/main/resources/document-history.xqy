xquery version "1.0-ml";

import module namespace dls = "http://marklogic.com/xdmp/dls" at "/MarkLogic/dls.xqy";

declare variable $URI as xs:string external;

let $history := dls:document-history($URI)
return count($history/dls:version/dls:version-id)||"~"||count($history/dls:version/dls:latest[. eq fn:false()])||"~"||count($history/dls:version/dls:latest[. eq fn:true()])