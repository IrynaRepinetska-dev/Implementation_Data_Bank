/* A Bison parser, made by GNU Bison 3.4.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2019 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* Undocumented macros, especially those whose name start with YY_,
   are private implementation details.  Do not rely on them.  */

#ifndef YY_YY_USERS_BZCSCHAE_WORKSPACE_DBS2_HUBDB_INCLUDE_HUBDB_DBPARSER_H_INCLUDED
# define YY_YY_USERS_BZCSCHAE_WORKSPACE_DBS2_HUBDB_INCLUDE_HUBDB_DBPARSER_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    CREATE = 258,
    DROP = 259,
    TABLE = 260,
    INSERT = 261,
    INTO = 262,
    VALUES = 263,
    SELECT = 264,
    DELETE = 265,
    FROM = 266,
    WHERE = 267,
    CONNECT = 268,
    DISCONNECT = 269,
    EQUALS = 270,
    STAR = 271,
    PARA_OPEN = 272,
    PARA_CLOSE = 273,
    COMMATA = 274,
    SEMICOLON = 275,
    TO = 276,
    LIST = 277,
    TABLES = 278,
    IMPORT = 279,
    EXPORT = 280,
    QUIT = 281,
    APOSTROPHE = 282,
    INDEX = 283,
    TYPE = 284,
    GET = 285,
    SCHEMA = 286,
    FOR = 287,
    DATABASE = 288,
    INTT = 289,
    DOUBLET = 290,
    VARCHAR = 291,
    AS = 292,
    IN = 293,
    DOT = 294,
    UNIQUE = 295,
    ON = 296,
    AND = 297,
    JOIN = 298,
    PRINT = 299,
    STATISTICS = 300,
    CLASSES = 301,
    HELP = 302,
    NUM = 303,
    REALNUM = 304,
    DBNAME = 305,
    STR_VALUE = 306
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 25 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"

  char * str;
  QualifiedName qName;
  int num;
  double realnum;
  bool flag;
  AttrDefStruct attrDef;
  RelDefStruct relDef;
  DBAttrType * valT;
  DBTuple * tupleT;
  DBListQualifiedName * listString;
  DBListPredicate * listPredicate;
  DBJoin * join;

#line 124 "/Users/bzcschae/workspace/dbs2/HubDB/include/hubDB/DBParser.h"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif



int yyparse (yyscan_t scanner, DBQueryMgr& queryMgr);

#endif /* !YY_YY_USERS_BZCSCHAE_WORKSPACE_DBS2_HUBDB_INCLUDE_HUBDB_DBPARSER_H_INCLUDED  */
