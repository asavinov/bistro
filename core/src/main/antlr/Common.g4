lexer grammar Common;

// Assign token names. They can be then used as constant in the program
//NEG : '-' ;
NOT : '!' ;

MUL : '*' ;
DIV : '/' ;
ADD : '+' ;
SUB : '-' ;

LEQ : '<=' ;
GEQ : '>=' ;
GRE : '>' ;
LES : '<' ;

EQ : '==' ;
NEQ : '!=' ;

AND : '&&' ;
OR : '||' ;

ID : LETTER (LETTER|DIGIT)* ;
DELIMITED_ID : '[' (LETTER|DIGIT|' ')* ']' ;

INT : DIGIT+ ;
DECIMAL : DIGIT+ '.' DIGIT* | '.' DIGIT+;
STRING : '"' ('\\"'|.)*? '"' ;

fragment
ALPHA : [a-zA-Z] ;
fragment
LETTER : [a-zA-Z\u0080-\u00FF_] ;
fragment
DIGIT : [0-9] ;

COMMENT
  : '/*' .*? '*/' -> skip // channel(HIDDEN) // match anything between /* and */
  ;

WS 
  : [ \t\r\n]+ -> skip // channel(HIDDEN) // toss out whitespace
  ;

NEWLINE:'\r'? '\n' ; // return newlines to parser (is end-statement signal)
