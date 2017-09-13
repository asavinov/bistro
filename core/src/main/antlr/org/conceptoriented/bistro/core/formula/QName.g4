grammar QName;
import Common;

@header {
  package org.conceptoriented.bistro.core.formula;
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
