/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 grammar HoodieSqlCommon;

 @lexer::members {
  /**
   * Verify whether current token is a valid decimal token (which contains dot).
   * Returns true if the character that follows the token is not a digit or letter or underscore.
   *
   * For example:
   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
   * which is not a digit or letter or underscore.
   */
  public boolean isValidDecimal() {
    int nextChar = _input.LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }
}

 singleStatement
    : statement EOF
    ;

 statement
    : compactionStatement                                                       #compactionCommand
    | indexStatement                                                            #indexCommand
    | CALL multipartIdentifier '(' (callArgument (',' callArgument)*)? ')'      #call
    | .*?                                                                       #passThrough
    ;

 compactionStatement
    : operation = (RUN | SCHEDULE) COMPACTION  ON tableIdentifier (AT instantTimestamp = INTEGER_VALUE)?    #compactionOnTable
    | operation = (RUN | SCHEDULE) COMPACTION  ON path = STRING   (AT instantTimestamp = INTEGER_VALUE)?    #compactionOnPath
    | SHOW COMPACTION  ON tableIdentifier (LIMIT limit = INTEGER_VALUE)?                             #showCompactionOnTable
    | SHOW COMPACTION  ON path = STRING (LIMIT limit = INTEGER_VALUE)?                               #showCompactionOnPath
    ;

 indexStatement
    : CREATE INDEX (IF NOT EXISTS)? indexName ON (TABLE)?
        tableIdentifier indexCols (USING indexType)?
        indexProperties?                                               #createIndex
    | DROP INDEX (IF EXISTS)? indexName ON (TABLE)? tableIdentifier    #dropIndex
    | REFRESH INDEX indexName ON (TABLE)? tableIdentifier              #refreshIndex
    | SHOW INDEX (indexName)? ON (TABLE)? tableIdentifier              #showIndex
    ;

 tableIdentifier
    : (db=IDENTIFIER '.')? table=IDENTIFIER
    ;

 callArgument
    : expression                    #positionalArgument
    | identifier '=>' expression    #namedArgument
    ;

 indexName
     : IDENTIFIER
     ;

 indexCols
     : '(' indexCol (',' indexCol)* ')'
     ;

 indexCol
     : identifier (ASC | DESC)?
     ;

 indexType
     : BLOOM
     | BTREE
     | BITMAP
     | LUCENE
     ;

 indexProperties
     : PROPERTIES '(' indexProperty (',' indexProperty)* ')'
     ;

 indexProperty
     : identifier (EQ constant)?
     ;

 expression
    : constant
    | stringMap
    ;

 constant
    : number                          #numericLiteral
    | booleanValue                    #booleanLiteral
    | STRING+                         #stringLiteral
    | identifier STRING               #typeConstructor
    ;

 stringMap
    : MAP '(' constant (',' constant)* ')'
    ;

 booleanValue
    : TRUE | FALSE
    ;

 number
    : MINUS? EXPONENT_VALUE           #exponentLiteral
    | MINUS? DECIMAL_VALUE            #decimalLiteral
    | MINUS? INTEGER_VALUE            #integerLiteral
    | MINUS? BIGINT_LITERAL           #bigIntLiteral
    | MINUS? SMALLINT_LITERAL         #smallIntLiteral
    | MINUS? TINYINT_LITERAL          #tinyIntLiteral
    | MINUS? DOUBLE_LITERAL           #doubleLiteral
    | MINUS? FLOAT_LITERAL            #floatLiteral
    | MINUS? BIGDECIMAL_LITERAL       #bigDecimalLiteral
    ;

 multipartIdentifier
    : parts+=identifier ('.' parts+=identifier)*
    ;

 identifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    | nonReserved             #unquotedIdentifier
    ;

 quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

 nonReserved
     : CALL  | COMPACTION | RUN | SCHEDULE | ON | SHOW | LIMIT | TABLE
     | TRUE  | FALSE | REFRESH | CREATE | INDEX | IF | NOT | EXISTS
     | USING | DROP | SHOW | ASC | DESC | PROPERTIES | INTERVAL | TO
     | BLOOM | BTREE| BITMAP | LUCENE
     ;

 ALL: 'ALL';
 AT: 'AT';
 ASC: 'ASC';
 BTREE: 'BTREE';
 BLOOM: 'BLOOM';
 BITMAP: 'BITMAP';
 CALL: 'CALL';
 COMPACTION: 'COMPACTION';
 CREATE: 'CREATE';
 DESC: 'DESC';
 DROP: 'DROP';
 EXISTS: 'EXISTS';
 REFRESH: 'REFRESH';
 RUN: 'RUN';
 SCHEDULE: 'SCHEDULE';
 ON: 'ON';
 PROPERTIES: 'PROPERTIES';
 USING: 'USING';
 SHOW: 'SHOW';
 IF: 'IF';
 INDEX: 'INDEX';
 LIMIT: 'LIMIT';
 LUCENE: 'LUCENE';
 MAP: 'MAP';
 NOT: 'NOT';
 NULL: 'NULL';
 TABLE: 'TABLE';
 TRUE: 'TRUE';
 FALSE: 'FALSE';
 INTERVAL: 'INTERVAL';
 TO: 'TO';

 PLUS: '+';
 MINUS: '-';

 EQ : '=' | '==';

 STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

 BIGINT_LITERAL
    : DIGIT+ 'L'
    ;

 SMALLINT_LITERAL
    : DIGIT+ 'S'
    ;

 TINYINT_LITERAL
    : DIGIT+ 'Y'
    ;

 INTEGER_VALUE
    : DIGIT+
    ;

 EXPONENT_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT {isValidDecimal()}?
    ;

 DECIMAL_VALUE
    : DECIMAL_DIGITS {isValidDecimal()}?
    ;

 FLOAT_LITERAL
    : DIGIT+ EXPONENT? 'F'
    | DECIMAL_DIGITS EXPONENT? 'F' {isValidDecimal()}?
    ;

 DOUBLE_LITERAL
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D' {isValidDecimal()}?
    ;

 BIGDECIMAL_LITERAL
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD' {isValidDecimal()}?
    ;

 IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

 BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

 fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

 fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

 fragment DIGIT
    : [0-9]
    ;

 fragment LETTER
    : [A-Z]
    ;

 SIMPLE_COMMENT
     : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
     ;

 BRACKETED_COMMENT
     : '/*' .*? '*/' -> channel(HIDDEN)
     ;

 WS  : [ \r\n\t]+ -> channel(HIDDEN)
     ;

 // Catch-all for anything we can't recognize.
 // We use this to be able to ignore and recover all the text
 // when splitting statements with DelimiterLexer
 UNRECOGNIZED
     : .
     ;
