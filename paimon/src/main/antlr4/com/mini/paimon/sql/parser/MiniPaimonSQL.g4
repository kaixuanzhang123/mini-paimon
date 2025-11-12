grammar MiniPaimonSQL;

// Parser Rules
statement
    : createTableStatement
    | insertStatement
    | selectStatement
    | dropTableStatement
    | deleteStatement
    ;

// CREATE TABLE
createTableStatement
    : CREATE TABLE qualifiedName
      '(' columnDefinition (',' columnDefinition)* ')'
      (PARTITIONED BY '(' identifier (',' identifier)* ')')?
      (TBLPROPERTIES '(' tableProperty (',' tableProperty)* ')')?
    ;

columnDefinition
    : identifier dataType (NOT NULL)?
    ;

tableProperty
    : STRING_LITERAL '=' STRING_LITERAL
    ;

// DROP TABLE
dropTableStatement
    : DROP TABLE (IF EXISTS)? qualifiedName
    ;

// INSERT
insertStatement
    : INSERT INTO qualifiedName
      ('(' identifier (',' identifier)* ')')?
      VALUES '(' expression (',' expression)* ')'
    ;

// SELECT
selectStatement
    : SELECT selectItem (',' selectItem)*
      FROM qualifiedName
      (WHERE booleanExpression)?
    ;

selectItem
    : '*'
    | expression (AS? identifier)?
    ;

// DELETE
deleteStatement
    : DELETE FROM qualifiedName
      (WHERE booleanExpression)?
    ;

// Expressions
booleanExpression
    : expression comparisonOperator expression                    # ComparisonExpression
    | booleanExpression AND booleanExpression                     # LogicalBinaryExpression
    | booleanExpression OR booleanExpression                      # LogicalBinaryExpression
    | NOT booleanExpression                                       # NotExpression
    | '(' booleanExpression ')'                                   # ParenthesizedBooleanExpression
    ;

expression
    : primaryExpression                                           # PrimaryExpr
    | expression op=('*' | '/' | '%') expression                  # ArithmeticBinary
    | expression op=('+' | '-') expression                        # ArithmeticBinary
    ;

primaryExpression
    : NULL                                                        # NullLiteral
    | number                                                      # NumericLiteral
    | booleanValue                                                # BooleanLiteral
    | STRING_LITERAL                                              # StringLiteral
    | identifier                                                  # ColumnReference
    | '(' expression ')'                                          # ParenthesizedExpression
    ;

comparisonOperator
    : '=' | '<>' | '!=' | '<' | '<=' | '>' | '>='
    ;

dataType
    : INT | INTEGER
    | BIGINT | LONG
    | DOUBLE | FLOAT
    | STRING
    | VARCHAR ('(' INTEGER_VALUE ')')?
    | CHAR ('(' INTEGER_VALUE ')')?
    | TEXT
    | BOOLEAN | BOOL
    ;

qualifiedName
    : identifier ('.' identifier)?
    ;

identifier
    : IDENTIFIER
    | nonReserved
    ;

nonReserved
    : SHOW | TABLES | COLUMNS
    ;

booleanValue
    : TRUE | FALSE
    ;

number
    : DECIMAL_VALUE
    | INTEGER_VALUE
    ;

// Lexer Rules
// Keywords
CREATE      : C R E A T E;
TABLE       : T A B L E;
DROP        : D R O P;
INSERT      : I N S E R T;
INTO        : I N T O;
VALUES      : V A L U E S;
SELECT      : S E L E C T;
FROM        : F R O M;
WHERE       : W H E R E;
DELETE      : D E L E T E;
AND         : A N D;
OR          : O R;
NOT         : N O T;
NULL        : N U L L;
TRUE        : T R U E;
FALSE       : F A L S E;
IF          : I F;
EXISTS      : E X I S T S;
AS          : A S;
PARTITIONED : P A R T I T I O N E D;
BY          : B Y;
TBLPROPERTIES : T B L P R O P E R T I E S;

// Data Types
INT         : I N T;
INTEGER     : I N T E G E R;
BIGINT      : B I G I N T;
LONG        : L O N G;
DOUBLE      : D O U B L E;
FLOAT       : F L O A T;
STRING      : S T R I N G;
VARCHAR     : V A R C H A R;
CHAR        : C H A R;
TEXT        : T E X T;
BOOLEAN     : B O O L E A N;
BOOL        : B O O L;

// Non-reserved keywords
SHOW        : S H O W;
TABLES      : T A B L E S;
COLUMNS     : C O L U M N S;

// Literals
STRING_LITERAL
    : '\'' (~['\\\r\n] | '\\' .)* '\''
    | '"' (~["\\\r\n] | '\\' .)* '"'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

// Fragments
fragment DIGIT : [0-9];
fragment LETTER : [a-zA-Z];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];

// Whitespace and comments
WS
    : [ \t\r\n]+ -> skip
    ;

LINE_COMMENT
    : '--' ~[\r\n]* -> skip
    ;

BLOCK_COMMENT
    : '/*' .*? '*/' -> skip
    ;
