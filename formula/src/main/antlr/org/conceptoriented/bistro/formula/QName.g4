grammar QName;
import Common;

@header {
  package org.conceptoriented.bistro.formula;
}

// path of names
qname
  : name ('.' name)*
  ;

// schema.table.column
qcolumn
  : ((name '.')? name '.')? name
  ;

name : (ID | DELIMITED_ID) ;
