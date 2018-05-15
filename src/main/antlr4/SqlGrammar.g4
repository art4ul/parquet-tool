grammar SqlGrammar;

sqlRule: SELECT projection (WHERE filterPredicate)?;

projection: (ALL | ID (',' ID) *);

filterPredicate : '(' filterPredicate ')'
       | NOT filterPredicate
       | filterPredicate AND filterPredicate
       | filterPredicate OR filterPredicate
       | valuePredicate;

valuePredicate: ID GT value
              | ID LT value
              | ID GT_EQ value
              | ID LT_EQ value
              | ID NOT_EQ value
              | ID EQ value ;

ALL: '*';

SELECT: [Ss][Ee][Ll][Ee][Cc][Tt];
WHERE: [Ww][Hh][Ee][Rr][Ee];
AND : [Aa][Nn][Dd];
NOT : [Nn][Oo][Tt];
OR : [Oo][Rr];
GT: '>';
LT : '<';
EQ : '=';
NOT_EQ : ('<>'|'!=');
IN: [Ii][Nn];
GT_EQ : '>=';
LT_EQ: '<=';

comparableValue: (longValue | doubleValue);
value: ( stringValue | longValue | doubleValue | boolValue);
stringValue: STRING_LITERAL;
longValue: LONG;
doubleValue: DOUBLE;
boolValue: BOOL;


ID: ( 'a'..'z' | 'A'..'Z' ) ( 'a'..'z' | 'A'..'Z' | DIGITS | '_' | '.' )* ;
STRING_LITERAL : '\'' ( ~'\'' | '\'\'' )* '\'' ;
DOUBLE : ( '0' | '1'..'9' DIGITS* )('.' DIGITS+ ) ;
LONG: [+-]?DIGITS;
BOOL : (TRUE | FALSE);
TRUE : [Tt][Rr][Uu][Ee];
FALSE : [Ff][Aa][Ll][Ss][Ee];

fragment DIGITS : [0-9]+;