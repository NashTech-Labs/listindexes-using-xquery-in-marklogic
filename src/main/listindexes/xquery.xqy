xquery version "1.0-ml";

module namespace xquery = "https://marklogic.com/MarkLogic/xquery-tools";

import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";

declare namespace db = "http://marklogic.com/xdmp/database";

declare variable $FINDBADURIS := fn:false();
declare variable $MAXURIS as xs:integer := 10;

declare function xquery:listIndexes($indexTypes as xs:string+, $dbNames as xs:string+) {

  for $dbName in $dbNames
  for $indexType in $indexTypes
  let $config := admin:get-configuration()
  let $indexes :=
    switch ($indexType)
      case "ELEMENT" return   admin:database-get-range-element-indexes($config, xdmp:database($dbName) )
      case "ATTRIBUTE" return admin:database-get-range-element-attribute-indexes($config, xdmp:database($dbName) )
      case "FIELD" return     admin:database-get-range-field-indexes($config, xdmp:database($dbName) )
      case "PATH" return      admin:database-get-range-path-indexes($config, xdmp:database($dbName) )
      default return fn:error(xs:QName("INVALID-INDEX-TYPE"), "INDEXTYPE must be one of the following: ELEMENT, ATTRIBUTE, FIELD, PATH")

  return (
    (: Output the column names :)
    let $localNameNum := -1
    let $columnNames :=
      for $element at $i in $indexes[1]/element()
      let $columnName := $element/local-name()
      let $_ := if ($columnName eq "localname") then xdmp:set($localNameNum, $i) else ()
      return $columnName
    return (
      if ($columnNames) then
        fn:string-join(("database","index",$columnNames), ",")
      else "No data."
      ,

      (: Output the row data found for this index type :)
      for $rawindex in $indexes

      (: Sometimes people group localnames in index definitions. This splits them out. :)
      let $splitLocalNames := fn:tokenize( $rawindex/db:localname/fn:string(), " " )

      let $index :=
        for $localname in $splitLocalNames
        return
          fn:string-join(
            (
              $dbName, $indexType,
              for $r at $i in $rawindex/element()
              return
                if ($i eq $localNameNum) then $localname else $r/fn:string()
            )
            , ","
          )
      return ($index)
    )
  )
};

declare function xquery:getNodeValue($docNode as node(), $searchNode as xs:QName) {
  if (fn:node-name($docNode) eq $searchNode) then $docNode/text()
  else ($docNode/child::* ! xquery:getNodeValue(., $searchNode))
};

declare function xquery:checkIndexIntegrity($dbNames as xs:string+, $findAffectedUris as xs:boolean, $maxUris as xs:integer) {


  for $databaseName in $dbNames

  let $indexes := xquery:listIndexes("ELEMENT", $databaseName )

  for $entry at $i in $indexes
  return if ($i eq 1) then ()
  else

    let $pieces := fn:tokenize($entry, ",")
    let $namespace := $pieces[4]
    let $elementName := $pieces[5]
    let $type := $pieces[3]

    return

      if ($elementRangeEstimate eq $elementValuesEstimate) then ()
      else (

        "Index on ["|| fn:string($qname) ||"] has "
        || fn:format-number($elementRangeEstimate - $elementValuesEstimate, "#,##0") ||" documents with invalid data in its ["|| $type ||"] index. "
        ,

        if (not($findAffectedUris)) then ()
        else
          let $valueURIs := cts:uris((), ("map"), cts:element-value-query($qname, $randomValue ))
          let $rangeURIs := cts:uris((), ("map"), cts:element-range-query($qname, "=", $randomValue ))
          let $affectedURIs := map:keys($rangeURIs - $valueURIs)


          return (
            fn:count( $affectedURIs ) || " affected URIs. "
            || (if ($maxUris eq -1) then ""  else (" First " || $maxUris || " URIS:"))
            ,

         (
            $affectedURIs[1 to (if ($maxUris eq -1) then fn:last() else $maxUris)]
          )
      )
};

