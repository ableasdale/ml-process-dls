xquery version "1.0-ml";

import module namespace dls = "http://marklogic.com/xdmp/dls" at "/MarkLogic/dls.xqy";

declare variable $URI as xs:string external;

declare function local:document-change-properties(
        $uri as xs:string,
        $properties as element()*,
        $function-type as xs:unsignedInt
) as empty-sequence()
{

(: Adding properties :)
    if ($function-type eq 0)
    then xdmp:document-add-properties($uri,$properties)

    (: Set properties :)
    else if ($function-type eq 1)
    then xdmp:document-set-properties($uri,$properties)
    else if ($function-type eq 2)
        then
            for $prop in $properties
            return xdmp:document-set-property($uri,$prop)
        else ()

};

declare function local:version-property($old as element(dls:version)) as element(dls:version)
{
    <dls:version>
        { $old/dls:version-id }
        { $old/dls:document-uri }
        <dls:latest>false</dls:latest>
        { $old/dls:created }
        { $old/dls:replaced }
        { $old/dls:author }
        { $old/dls:external-security-id }
        { $old/dls:external-user-name }
        { $old/dls:annotation }
        { $old/dls:deleted }
    </dls:version>};

let $old-properties := xdmp:document-properties($URI)//dls:version
return
    local:document-change-properties($URI, local:version-property($old-properties), 2)
