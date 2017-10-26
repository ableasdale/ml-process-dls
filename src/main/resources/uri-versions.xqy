xquery version "1.0-ml";

import module namespace dls = "http://marklogic.com/xdmp/dls" at "/MarkLogic/dls.xqy";

declare variable $URI as xs:string external;

for $i in dls:document-history($URI)/dls:version
return xs:unsignedLong($i/dls:version-id)||"~"||xs:boolean($i/dls:latest)