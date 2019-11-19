/* A Bison parser, made by GNU Bison 3.4.2.  */

/* Bison implementation for Yacc-like parsers in C

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

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Undocumented macros, especially those whose name start with YY_,
   are private implementation details.  Do not rely on them.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "3.4.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 2

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1




/* First part of user prologue.  */
#line 6 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"

#include <hubDB/DBParserDefs.h>
#include <hubDB/DBParser.h>
#include <hubDB/DBLexer.h>
#include <hubDB/DBTypes.h>
#include <hubDB/DBException.h>
#include <hubDB/DBServer.h>

#include <hubDB/DBServerSocket.h>
using HubDB::Socket::DBServerSocket;
using namespace HubDB::Types;
using namespace HubDB::Exception;

static LoggerPtr loggerParser(Logger::getLogger("HubDB.Parser"));

YY_DECL ;


#line 89 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"

# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 0
#endif

/* Use api.header.include to #include this header
   instead of duplicating it here.  */
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

#line 199 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif



int yyparse (yyscan_t scanner, DBQueryMgr& queryMgr);

#endif /* !YY_YY_USERS_BZCSCHAE_WORKSPACE_DBS2_HUBDB_INCLUDE_HUBDB_DBPARSER_H_INCLUDED  */



#ifdef short
# undef short
#endif

#ifdef YYTYPE_UINT8
typedef YYTYPE_UINT8 yytype_uint8;
#else
typedef unsigned char yytype_uint8;
#endif

#ifdef YYTYPE_INT8
typedef YYTYPE_INT8 yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef YYTYPE_UINT16
typedef YYTYPE_UINT16 yytype_uint16;
#else
typedef unsigned short yytype_uint16;
#endif

#ifdef YYTYPE_INT16
typedef YYTYPE_INT16 yytype_int16;
#else
typedef short yytype_int16;
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif ! defined YYSIZE_T
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM ((YYSIZE_T) -1)

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif

#ifndef YY_ATTRIBUTE
# if (defined __GNUC__                                               \
      && (2 < __GNUC__ || (__GNUC__ == 2 && 96 <= __GNUC_MINOR__)))  \
     || defined __SUNPRO_C && 0x5110 <= __SUNPRO_C
#  define YY_ATTRIBUTE(Spec) __attribute__(Spec)
# else
#  define YY_ATTRIBUTE(Spec) /* empty */
# endif
#endif

#ifndef YY_ATTRIBUTE_PURE
# define YY_ATTRIBUTE_PURE   YY_ATTRIBUTE ((__pure__))
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# define YY_ATTRIBUTE_UNUSED YY_ATTRIBUTE ((__unused__))
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(E) ((void) (E))
#else
# define YYUSE(E) /* empty */
#endif

#if defined __GNUC__ && ! defined __ICC && 407 <= __GNUC__ * 100 + __GNUC_MINOR__
/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN \
    _Pragma ("GCC diagnostic push") \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")\
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# define YY_IGNORE_MAYBE_UNINITIALIZED_END \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYSIZE_T yynewbytes;                                            \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / sizeof (*yyptr);                          \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, (Count) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYSIZE_T yyi;                         \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  62
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   137

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  52
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  37
/* YYNRULES -- Number of rules.  */
#define YYNRULES  70
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  153

#define YYUNDEFTOK  2
#define YYMAXUTOK   306

/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                                \
  ((unsigned) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51
};

#if YYDEBUG
  /* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,    61,    61,    62,    65,    66,    67,    70,    78,    79,
      80,    81,    82,    83,    84,    85,    86,    87,    88,    89,
      90,    91,    92,    93,    94,    95,    98,   114,   123,   133,
     141,   149,   157,   164,   171,   179,   188,   204,   218,   219,
     222,   223,   224,   227,   235,   243,   244,   247,   254,   263,
     268,   275,   279,   283,   290,   301,   305,   313,   321,   331,
     345,   349,   355,   360,   368,   371,   381,   390,   399,   410,
     423
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || 0
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "CREATE", "DROP", "TABLE", "INSERT",
  "INTO", "VALUES", "SELECT", "DELETE", "FROM", "WHERE", "CONNECT",
  "DISCONNECT", "EQUALS", "STAR", "PARA_OPEN", "PARA_CLOSE", "COMMATA",
  "SEMICOLON", "TO", "LIST", "TABLES", "IMPORT", "EXPORT", "QUIT",
  "APOSTROPHE", "INDEX", "TYPE", "GET", "SCHEMA", "FOR", "DATABASE",
  "INTT", "DOUBLET", "VARCHAR", "AS", "IN", "DOT", "UNIQUE", "ON", "AND",
  "JOIN", "PRINT", "STATISTICS", "CLASSES", "HELP", "NUM", "REALNUM",
  "DBNAME", "STR_VALUE", "$accept", "start", "commands", "full_command",
  "command", "dbName", "qName", "strValue", "create_db", "drop_db",
  "connect", "disconnect", "list_tables", "get_schema", "create_table",
  "attrDefs", "unique", "type", "drop_table", "create_index", "itype",
  "drop_index", "insert", "value", "vtype", "delete", "where_clause",
  "predicates", "select", "projection", "attrList", "join", "import",
  "export", "statistics", "classes", "help", YY_NULLPTR
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[NUM] -- (External) token number corresponding to the
   (internal) symbol number NUM (which must be that of a token).  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     295,   296,   297,   298,   299,   300,   301,   302,   303,   304,
     305,   306
};
# endif

#define YYPACT_NINF -119

#define yypact_value_is_default(Yystate) \
  (!!((Yystate) == (-119)))

#define YYTABLE_NINF -1

#define yytable_value_is_error(Yytable_value) \
  0

  /* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
     STATE-NUM.  */
static const yytype_int8 yypact[] =
{
      45,   -15,    10,    11,     7,   -14,     9,     1,     3,    -2,
      13,   -25,  -119,    -5,   -35,    17,    40,    90,  -119,  -119,
    -119,  -119,  -119,  -119,  -119,  -119,  -119,  -119,  -119,  -119,
    -119,  -119,  -119,  -119,  -119,  -119,  -119,  -119,  -119,   -25,
     -25,   -25,   -25,   -25,   -25,   -25,  -119,  -119,     6,  -119,
      36,    31,   -25,   -25,  -119,    33,    30,    39,    32,    43,
      46,  -119,  -119,  -119,  -119,    28,    47,    48,    52,    57,
      58,    71,   -25,   -25,   -25,    68,    61,  -119,    34,    75,
      30,    78,  -119,  -119,    67,    30,    81,  -119,  -119,  -119,
    -119,    70,  -119,    55,  -119,   -25,    82,  -119,    79,   -25,
      85,   -25,   -25,  -119,  -119,    25,   -25,    68,    92,    66,
    -119,  -119,    89,  -119,    91,   -27,    -6,  -119,  -119,  -119,
       0,  -119,    72,    97,    25,   -25,  -119,  -119,  -119,  -119,
    -119,    83,    98,   -25,    99,    25,   -25,  -119,  -119,   106,
    -119,  -119,  -119,   -27,  -119,  -119,   109,    25,    83,   -25,
    -119,  -119,  -119
};

  /* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
     Performed when YYTABLE does not specify something else to do.  Zero
     means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     3,     0,     0,     0,     0,     0,     5,     7,
      10,    11,     8,     9,    12,    13,    14,    15,    16,    17,
      18,    19,    20,    21,    22,    23,    24,    25,     6,     0,
       0,     0,     0,     0,     0,     0,    60,    26,     0,    63,
       0,    61,     0,     0,    32,     0,     0,     0,     0,     0,
       0,    70,     1,     2,     4,     0,    45,     0,     0,     0,
       0,     0,     0,     0,     0,    55,     0,    33,     0,     0,
       0,     0,    68,    69,     0,     0,     0,    29,    43,    47,
      30,     0,    27,    64,    62,     0,     0,    31,     0,     0,
       0,     0,     0,    46,    44,     0,     0,    55,     0,    56,
      54,    28,     0,    67,     0,     0,     0,    51,    52,    53,
       0,    50,     0,     0,     0,     0,    66,    34,    40,    41,
      42,    38,     0,     0,     0,     0,     0,    59,    58,     0,
      39,    37,    35,     0,    48,    49,     0,     0,    38,     0,
      57,    36,    65
};

  /* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
    -119,  -119,  -119,   108,  -119,   -11,   -39,   -53,  -119,  -119,
    -119,  -119,  -119,  -119,  -119,  -119,   -22,   -16,  -119,  -119,
    -119,  -119,  -119,  -119,  -118,  -119,    21,  -119,  -119,  -119,
    -119,  -119,  -119,  -119,  -119,  -119,  -119
};

  /* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    16,    17,    18,    19,    48,    49,   119,    20,    21,
      22,    23,    24,    25,    26,   116,   141,   131,    27,    28,
      86,    29,    30,   120,   121,    31,    96,   109,    32,    50,
      51,   107,    33,    34,    35,    36,    37
};

  /* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
     positive, shift that token.  If negative, reduce the rule whose
     number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_uint8 yytable[] =
{
      57,    66,    46,    79,    69,    38,   138,   128,   129,   130,
      59,    60,   132,   133,    45,    39,    42,   145,   134,   135,
      52,    55,    53,    54,    56,    47,    58,   100,    65,   150,
      67,    68,   103,    70,    71,    94,    47,    61,    40,    43,
      62,    75,    76,    41,    44,    72,     1,    73,     2,     3,
      74,     4,    78,    77,     5,     6,   108,    78,     7,     8,
      80,    92,    93,    82,    81,    84,    83,     9,    87,    10,
      11,    12,    88,   117,   118,    13,    85,    89,    90,    91,
      95,    97,    99,   101,   102,    98,   139,   105,   112,    14,
     114,   115,    15,     2,     3,   122,     4,   146,   106,     5,
       6,   104,   110,     7,     8,   113,   111,   124,   125,   126,
     152,   127,     9,   136,    10,    11,    63,   137,   142,   144,
      13,   147,   143,   140,   149,    64,   151,   148,   123,     0,
       0,     0,     0,     0,    14,     0,     0,    15
};

static const yytype_int16 yycheck[] =
{
      11,    40,    16,    56,    43,    20,   124,    34,    35,    36,
      45,    46,    18,    19,     7,     5,     5,   135,    18,    19,
      11,    23,    21,    20,    11,    50,    31,    80,    39,   147,
      41,    42,    85,    44,    45,    74,    50,    20,    28,    28,
       0,    52,    53,    33,    33,    39,     1,    11,     3,     4,
      19,     6,    27,    20,     9,    10,    95,    27,    13,    14,
      21,    72,    73,    20,    32,    37,    20,    22,    20,    24,
      25,    26,    20,    48,    49,    30,    29,    20,    20,     8,
      12,    20,     7,     5,    17,    51,   125,    17,    99,    44,
     101,   102,    47,     3,     4,   106,     6,   136,    43,     9,
      10,    20,    20,    13,    14,    20,    27,    15,    42,    20,
     149,    20,    22,    41,    24,    25,    26,    20,    20,    20,
      30,    15,   133,    40,    15,    17,   148,   143,   107,    -1,
      -1,    -1,    -1,    -1,    44,    -1,    -1,    47
};

  /* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
     symbol of state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,     1,     3,     4,     6,     9,    10,    13,    14,    22,
      24,    25,    26,    30,    44,    47,    53,    54,    55,    56,
      60,    61,    62,    63,    64,    65,    66,    70,    71,    73,
      74,    77,    80,    84,    85,    86,    87,    88,    20,     5,
      28,    33,     5,    28,    33,     7,    16,    50,    57,    58,
      81,    82,    11,    21,    20,    23,    11,    57,    31,    45,
      46,    20,     0,    26,    55,    57,    58,    57,    57,    58,
      57,    57,    39,    11,    19,    57,    57,    20,    27,    59,
      21,    32,    20,    20,    37,    29,    72,    20,    20,    20,
      20,     8,    57,    57,    58,    12,    78,    20,    51,     7,
      59,     5,    17,    59,    20,    17,    43,    83,    58,    79,
      20,    27,    57,    20,    57,    57,    67,    48,    49,    59,
      75,    76,    57,    78,    15,    42,    20,    20,    34,    35,
      36,    69,    18,    19,    18,    19,    41,    20,    76,    58,
      40,    68,    20,    57,    20,    76,    58,    15,    69,    15,
      76,    68,    58
};

  /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint8 yyr1[] =
{
       0,    52,    53,    53,    54,    54,    54,    55,    56,    56,
      56,    56,    56,    56,    56,    56,    56,    56,    56,    56,
      56,    56,    56,    56,    56,    56,    57,    58,    59,    60,
      61,    62,    63,    64,    65,    66,    67,    67,    68,    68,
      69,    69,    69,    70,    71,    72,    72,    73,    74,    75,
      75,    76,    76,    76,    77,    78,    78,    79,    79,    80,
      81,    81,    82,    82,    83,    83,    84,    85,    86,    87,
      88
};

  /* YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     2,     1,     2,     1,     2,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     3,     3,     4,
       4,     4,     2,     3,     6,     8,     5,     3,     0,     1,
       1,     1,     1,     4,     5,     0,     2,     4,     8,     3,
       1,     1,     1,     1,     5,     0,     2,     5,     3,     7,
       1,     1,     3,     1,     0,     6,     6,     5,     3,     3,
       2
};


#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)
#define YYEMPTY         (-2)
#define YYEOF           0

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (scanner, queryMgr, YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Error token number */
#define YYTERROR        1
#define YYERRCODE       256



/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)

/* This macro is provided for backward compatibility. */
#ifndef YY_LOCATION_PRINT
# define YY_LOCATION_PRINT(File, Loc) ((void) 0)
#endif


# define YY_SYMBOL_PRINT(Title, Type, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Type, Value, scanner, queryMgr); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo, int yytype, YYSTYPE const * const yyvaluep, yyscan_t scanner, DBQueryMgr& queryMgr)
{
  FILE *yyoutput = yyo;
  YYUSE (yyoutput);
  YYUSE (scanner);
  YYUSE (queryMgr);
  if (!yyvaluep)
    return;
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyo, yytoknum[yytype], *yyvaluep);
# endif
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YYUSE (yytype);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo, int yytype, YYSTYPE const * const yyvaluep, yyscan_t scanner, DBQueryMgr& queryMgr)
{
  YYFPRINTF (yyo, "%s %s (",
             yytype < YYNTOKENS ? "token" : "nterm", yytname[yytype]);

  yy_symbol_value_print (yyo, yytype, yyvaluep, scanner, queryMgr);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yytype_int16 *yybottom, yytype_int16 *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yytype_int16 *yyssp, YYSTYPE *yyvsp, int yyrule, yyscan_t scanner, DBQueryMgr& queryMgr)
{
  unsigned long yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       yystos[yyssp[yyi + 1 - yynrhs]],
                       &yyvsp[(yyi + 1) - (yynrhs)]
                                              , scanner, queryMgr);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule, scanner, queryMgr); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
static YYSIZE_T
yystrlen (const char *yystr)
{
  YYSIZE_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYSIZE_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYSIZE_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            else
              goto append;

          append:
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (! yyres)
    return yystrlen (yystr);

  return (YYSIZE_T) (yystpcpy (yyres, yystr) - yyres);
}
# endif

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return 1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return 2 if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYSIZE_T *yymsg_alloc, char **yymsg,
                yytype_int16 *yyssp, int yytoken)
{
  YYSIZE_T yysize0 = yytnamerr (YY_NULLPTR, yytname[yytoken]);
  YYSIZE_T yysize = yysize0;
  enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat. */
  char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
  /* Number of reported tokens (one for the "unexpected", one per
     "expected"). */
  int yycount = 0;

  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yytoken != YYEMPTY)
    {
      int yyn = yypact[*yyssp];
      yyarg[yycount++] = yytname[yytoken];
      if (!yypact_value_is_default (yyn))
        {
          /* Start YYX at -YYN if negative to avoid negative indexes in
             YYCHECK.  In other words, skip the first -YYN actions for
             this state because they are default actions.  */
          int yyxbegin = yyn < 0 ? -yyn : 0;
          /* Stay within bounds of both yycheck and yytname.  */
          int yychecklim = YYLAST - yyn + 1;
          int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
          int yyx;

          for (yyx = yyxbegin; yyx < yyxend; ++yyx)
            if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR
                && !yytable_value_is_error (yytable[yyx + yyn]))
              {
                if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
                  {
                    yycount = 1;
                    yysize = yysize0;
                    break;
                  }
                yyarg[yycount++] = yytname[yyx];
                {
                  YYSIZE_T yysize1 = yysize + yytnamerr (YY_NULLPTR, yytname[yyx]);
                  if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
                    yysize = yysize1;
                  else
                    return 2;
                }
              }
        }
    }

  switch (yycount)
    {
# define YYCASE_(N, S)                      \
      case N:                               \
        yyformat = S;                       \
      break
    default: /* Avoid compiler warnings. */
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
# undef YYCASE_
    }

  {
    YYSIZE_T yysize1 = yysize + yystrlen (yyformat);
    if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
      yysize = yysize1;
    else
      return 2;
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return 1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yyarg[yyi++]);
          yyformat += 2;
        }
      else
        {
          yyp++;
          yyformat++;
        }
  }
  return 0;
}
#endif /* YYERROR_VERBOSE */

/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep, yyscan_t scanner, DBQueryMgr& queryMgr)
{
  YYUSE (yyvaluep);
  YYUSE (scanner);
  YYUSE (queryMgr);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YYUSE (yytype);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}




/*----------.
| yyparse.  |
`----------*/

int
yyparse (yyscan_t scanner, DBQueryMgr& queryMgr)
{
/* The lookahead symbol.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

    /* Number of syntax errors so far.  */
    int yynerrs;

    int yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       'yyss': related to states.
       'yyvs': related to semantic values.

       Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yytype_int16 yyssa[YYINITDEPTH];
    yytype_int16 *yyss;
    yytype_int16 *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    YYSIZE_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken = 0;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yyssp = yyss = yyssa;
  yyvsp = yyvs = yyvsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */
  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yynewstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  *yyssp = (yytype_int16) yystate;

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    goto yyexhaustedlab;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = (YYSIZE_T) (yyssp - yyss + 1);

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        YYSTYPE *yyvs1 = yyvs;
        yytype_int16 *yyss1 = yyss;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * sizeof (*yyssp),
                    &yyvs1, yysize * sizeof (*yyvsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yytype_int16 *yyss1 = yyss;
        union yyalloc *yyptr =
          (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
        if (! yyptr)
          goto yyexhaustedlab;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
# undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
                  (unsigned long) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = yylex (&yylval, scanner);
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token.  */
  yychar = YYEMPTY;

  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 2:
#line 61 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    { YYACCEPT; }
#line 1409 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 3:
#line 62 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    { YYACCEPT; }
#line 1415 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 6:
#line 67 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {yyerrok; queryMgr.getSocket()->getWriteStream()<<endl; queryMgr.getSocket()->writeToSocket();}
#line 1421 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 7:
#line 71 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
		queryMgr.getSocket()->getWriteStream()<<endl;
		queryMgr.getSocket()->writeToSocket();
		HubDB::Server::DBServer::checkAbortStatus();
	}
#line 1431 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 26:
#line 99 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
			if(strlen((yyvsp[0].str))>MAX_STR_LEN){
				queryMgr.getSocket()->getWriteStream() << "string is too large" <<endl;
				YYERROR;
			}
            (yyval.str)=(yyvsp[0].str);
            uint i=0;
            while((yyval.str)[i]!='\0'){
                if(isalpha((yyval.str)[i]))
                    (yyval.str)[i] = toupper((yyval.str)[i]);
                ++i;
            }
		}
#line 1449 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 27:
#line 115 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
			strcpy((yyval.qName).relationName,(yyvsp[-2].str));
			free((yyvsp[-2].str));
			strcpy((yyval.qName).attributeName,(yyvsp[0].str));
			free((yyvsp[0].str));
		}
#line 1460 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 28:
#line 124 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
			(yyval.str)=(yyvsp[-1].str);
			if(strlen((yyval.str))>MAX_STR_LEN){
				queryMgr.getSocket()->getWriteStream() << "string is too large" <<endl;
				YYERROR;
			}
		}
#line 1472 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 29:
#line 134 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
		LOG4CXX_DEBUG(loggerParser,(string)"create database " + (yyvsp[-1].str) );
		queryMgr.createDB((yyvsp[-1].str));
        free((yyvsp[-1].str));
	}
#line 1482 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 30:
#line 142 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
		LOG4CXX_DEBUG(loggerParser,(string)"drop database " + (yyvsp[-1].str) );
		queryMgr.dropDB((yyvsp[-1].str));
        free((yyvsp[-1].str));
	}
#line 1492 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 31:
#line 150 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
		LOG4CXX_DEBUG(loggerParser,(string)"connect to " + (yyvsp[-1].str) );
		queryMgr.connectTo((yyvsp[-1].str));
        free((yyvsp[-1].str));
	}
#line 1502 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 32:
#line 158 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
		LOG4CXX_DEBUG(loggerParser,"disconnect");
		queryMgr.disconnect();
	}
#line 1511 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 33:
#line 165 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        LOG4CXX_DEBUG(loggerParser,"list tables " );
        queryMgr.listTables();
    }
#line 1520 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 34:
#line 172 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        LOG4CXX_DEBUG(loggerParser,(string)"get schema for tables " + (yyvsp[-1].str) );
        queryMgr.getSchemaForTable((yyvsp[-1].str));
        free((yyvsp[-1].str));
    }
#line 1530 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 35:
#line 180 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
	    strcpy((yyvsp[-2].relDef).relationName,(yyvsp[-5].str));
        LOG4CXX_DEBUG(loggerParser,(string)"create table\n"+TO_STR(&(yyvsp[-2].relDef))); 
        queryMgr.createTable((yyvsp[-2].relDef));
        free((yyvsp[-5].str));
    }
#line 1541 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 36:
#line 189 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        (yyval.relDef) = (yyvsp[-4].relDef);
    	if((yyval.relDef).attrCnt == MAX_ATTR_PER_REL){
			queryMgr.getSocket()->getWriteStream() << "too many attributes" <<endl;
			YYERROR;
    	}
        (yyvsp[-1].attrDef).isUnique = (yyvsp[0].flag);
        (yyvsp[-1].attrDef).isIndexed = false;
        (yyvsp[-1].attrDef).attrPos = (yyval.relDef).attrCnt;
		(yyvsp[-1].attrDef).indexType[0] = '\0';
        strcpy((yyvsp[-1].attrDef).attrName,(yyvsp[-2].str));
        free((yyvsp[-2].str));
        (yyval.relDef).attrList[(yyval.relDef).attrCnt++] = (yyvsp[-1].attrDef);
        (yyval.relDef).tupleSize += (yyvsp[-1].attrDef).attrLen;
    }
#line 1561 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 37:
#line 205 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        (yyval.relDef).attrCnt = 0;
        (yyval.relDef).tupleSize = (yyvsp[-1].attrDef).attrLen;
        (yyvsp[-1].attrDef).isUnique = (yyvsp[0].flag);
        (yyvsp[-1].attrDef).isIndexed = false;
        (yyvsp[-1].attrDef).attrPos = (yyval.relDef).attrCnt;
		(yyvsp[-1].attrDef).indexType[0] = '\0';
		strcpy((yyvsp[-1].attrDef).attrName,(yyvsp[-2].str));
        free((yyvsp[-2].str));
        (yyval.relDef).attrList[(yyval.relDef).attrCnt++] = (yyvsp[-1].attrDef);
    }
#line 1577 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 38:
#line 218 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    { (yyval.flag)=false; }
#line 1583 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 39:
#line 219 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    { (yyval.flag)=true;}
#line 1589 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 40:
#line 222 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    { (yyval.attrDef).attrType = INT; (yyval.attrDef).attrLen = DBIntType::getSize(); }
#line 1595 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 41:
#line 223 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    { (yyval.attrDef).attrType = DOUBLE; (yyval.attrDef).attrLen = DBDoubleType::getSize();}
#line 1601 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 42:
#line 224 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {  (yyval.attrDef).attrType = VCHAR; (yyval.attrDef).attrLen = DBVCharType::getSize(); }
#line 1607 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 43:
#line 228 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        LOG4CXX_DEBUG(loggerParser,(string)"drop table " + (yyvsp[-1].str));
        queryMgr.dropTable((yyvsp[-1].str));
        free((yyvsp[-1].str));
    }
#line 1617 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 44:
#line 236 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        LOG4CXX_DEBUG(loggerParser,(string)"create index " + (yyvsp[-2].qName).toString() + " TYPE " + (yyvsp[-1].str) );
        queryMgr.createIndex((yyvsp[-2].qName),(yyvsp[-1].str));
        free((yyvsp[-1].str));
    }
#line 1627 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 45:
#line 243 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    { (yyval.str)=strdup(DEFAULT_IDXTYPE.c_str());}
#line 1633 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 46:
#line 244 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {(yyval.str)=(yyvsp[0].str);}
#line 1639 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 47:
#line 248 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        LOG4CXX_DEBUG(loggerParser,(string)"drop index " + (yyvsp[-1].qName).toString());
        queryMgr.dropIndex((yyvsp[-1].qName));
    }
#line 1648 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 48:
#line 255 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
       LOG4CXX_DEBUG(loggerParser,(string)"insert into " + (yyvsp[-5].str) + " value(" + (yyvsp[-2].tupleT)->toString() + ")");
       queryMgr.insertInto((yyvsp[-5].str),(yyvsp[-2].tupleT));
       free((yyvsp[-5].str));
       delete (yyvsp[-2].tupleT);
    }
#line 1659 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 49:
#line 264 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        (yyval.tupleT) = (yyvsp[-2].tupleT);
        (yyval.tupleT)->appendAttrVal((yyvsp[0].valT));
    }
#line 1668 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 50:
#line 269 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        (yyval.tupleT) = new DBTuple();
        (yyval.tupleT)->appendAttrVal((yyvsp[0].valT));
    }
#line 1677 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 51:
#line 276 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        (yyval.valT) = new DBIntType((yyvsp[0].num));
    }
#line 1685 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 52:
#line 280 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        (yyval.valT) = new DBDoubleType((yyvsp[0].realnum));
    }
#line 1693 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 53:
#line 284 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        (yyval.valT) = new DBVCharType((yyvsp[0].str));
        free((yyvsp[0].str));
    }
#line 1702 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 54:
#line 291 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        LOG4CXX_DEBUG(loggerParser,(string)"delete from " + (yyvsp[-2].str) + ((yyvsp[-1].listPredicate)!=NULL ? " where " + TO_STR(*(yyvsp[-1].listPredicate)) : ""));
        queryMgr.deleteFromTable((yyvsp[-2].str),(yyvsp[-1].listPredicate));
        free((yyvsp[-2].str));
        if((yyvsp[-1].listPredicate)!=NULL)
            delete (yyvsp[-1].listPredicate);
    }
#line 1714 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 55:
#line 301 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
    	LOG4CXX_DEBUG(loggerParser,"where is null");
        (yyval.listPredicate) = NULL;
    }
#line 1723 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 56:
#line 306 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
    	LOG4CXX_DEBUG(loggerParser,"where: " + TO_STR((yyvsp[0].listPredicate)));    
    	(yyval.listPredicate) = (yyvsp[0].listPredicate);
    }
#line 1732 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 57:
#line 314 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        (yyval.listPredicate) = (yyvsp[-4].listPredicate);
        DBPredicate p((yyvsp[0].valT),(yyvsp[-2].qName));
        LOG4CXX_DEBUG(loggerParser,"Create Predicate: "+ p.toString())
        (yyval.listPredicate)->push_back(p);
        LOG4CXX_DEBUG(loggerParser,"PredicateList is"+ TO_STR(*(yyval.listPredicate)))
    }
#line 1744 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 58:
#line 322 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        (yyval.listPredicate) = new DBListPredicate();
        DBPredicate p((yyvsp[0].valT),(yyvsp[-2].qName));
        LOG4CXX_DEBUG(loggerParser,"Create Predicate: "+ p.toString())
        (yyval.listPredicate)->push_back(p);
        LOG4CXX_DEBUG(loggerParser,"PredicateList is "+ TO_STR((yyval.listPredicate))+ " "+ TO_STR(*(yyval.listPredicate)))
    }
#line 1756 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 59:
#line 332 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        LOG4CXX_DEBUG(loggerParser,"select " + ((yyvsp[-5].listString)!=NULL ? TO_STR((yyvsp[-5].listString)) : "*") + " from " + (yyvsp[-3].str) + " " + ((yyvsp[-2].join)!=NULL ? (yyvsp[-2].join)->toString() : "") + " " + ((yyvsp[-1].listPredicate)!=NULL ? "where " + TO_STR(*(yyvsp[-1].listPredicate)) : ""));
        queryMgr.select((yyvsp[-5].listString),(yyvsp[-3].str),(yyvsp[-2].join),(yyvsp[-1].listPredicate));
        free((yyvsp[-3].str));
        if((yyvsp[-5].listString)!=NULL)
            delete (yyvsp[-5].listString);
        if((yyvsp[-2].join)!=NULL)
            delete (yyvsp[-2].join);
        if((yyvsp[-1].listPredicate)!=NULL)
            delete (yyvsp[-1].listPredicate); 
    }
#line 1772 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 60:
#line 346 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        (yyval.listString) = NULL;
    }
#line 1780 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 61:
#line 350 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        (yyval.listString) = (yyvsp[0].listString);
    }
#line 1788 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 62:
#line 356 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        (yyval.listString) = (yyvsp[-2].listString);
        (yyval.listString)->push_back((yyvsp[0].qName));
    }
#line 1797 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 63:
#line 361 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        (yyval.listString) = new DBListQualifiedName;
        (yyval.listString)->push_back((yyvsp[0].qName));
    }
#line 1806 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 64:
#line 368 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        (yyval.join) = NULL;
    }
#line 1814 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 65:
#line 372 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        (yyval.join) = new DBJoin;
		strcpy((yyval.join)->relationName,(yyvsp[-4].str));
        free((yyvsp[-4].str));
        (yyval.join)->attrNames[0] = (yyvsp[-2].qName);
        (yyval.join)->attrNames[1] = (yyvsp[0].qName);        
    }
#line 1826 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 66:
#line 382 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        LOG4CXX_DEBUG(loggerParser,(string)"import from " + (yyvsp[-3].str) + " into " + (yyvsp[-1].str));
        queryMgr.importTab((yyvsp[-3].str),(yyvsp[-1].str));
        free((yyvsp[-3].str));
        free((yyvsp[-1].str));
    }
#line 1837 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 67:
#line 391 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        LOG4CXX_DEBUG(loggerParser,(string)"export " + (yyvsp[-3].str) + " to " + (yyvsp[-1].str));
        queryMgr.exportTab((yyvsp[-3].str),(yyvsp[-1].str));
        free((yyvsp[-3].str));
        free((yyvsp[-1].str));
    }
#line 1848 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 68:
#line 400 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
		LOG4CXX_DEBUG(loggerParser,(string)"print statistics");
		if(DBMonitorMgr::getMonitorPtr() != NULL){
			DBMonitorMgr::getMonitorPtr()->printStatistic(queryMgr.getSocket()->getWriteStream());
		}else{
			queryMgr.getSocket()->getWriteStream() << "Monitoring is not enabled"<<endl;
		}
	}
#line 1861 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 69:
#line 411 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        LOG4CXX_DEBUG(loggerParser,(string)"print classes");
        queryMgr.getSocket()->getWriteStream() << "----------KnownClasses----------" <<endl;
        list<string> l = getKnownClassNames();
        uint cnt = l.size();
        while(l.empty()==false){
            queryMgr.getSocket()->getWriteStream() << l.front()  <<endl;
            l.pop_front();
        }
        queryMgr.getSocket()->getWriteStream() << "   " << cnt << " row(s) selected."<<endl;
    }
#line 1877 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;

  case 70:
#line 424 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"
    {
        LOG4CXX_DEBUG(loggerParser,(string)"help");
        queryMgr.getSocket()->getWriteStream() << "HELP:" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tCREATE DATABASE <dbname>" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tDROP DATABASE <dbname>" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tCONNECT TO <dbname>" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tDISCONNECT" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tLIST TABLES" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tGET SCHEMA FOR TABLE <tabname>" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tCREATE TABLE <tabname> AS ( <attrname> {INTEGER|DOUBLE|VARCHAR} [UNIQUE] {,<attrname> {INTEGER|DOUBLE|VARCHAR} [UNIQUE]}*)" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tDROP TABLE <tabname>" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tCREATE INDEX <tabname>.<attrname> [TYPE '<idxclassname>']" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tDROP INDEX <tabname>.<attrname>" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tINSERT INTO <tabname> VALUES (x,y,z,..)" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tDELETE FROM <tabname> [WHERE <tabname>.<attrname> = value {AND <tabname>.<attrname> = value}*]" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tSELECT {*|<tabname>.<attrname>{,<tabname>.<attrname>}*} FROM <tabname> [JOIN <tabname> ON <tabname>.<attrname> = <tabname>.<attrname>] [WHERE <tabname>.<attrname> = value {AND <tabname>.<attrname> = value}*]" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tIMPORT FROM '<filename>' INTO <tabname>" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tEXPORT <tabname> TO '<filename>'" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tPRINT STATISTICS" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tPRINT CLASSES" <<endl;
        queryMgr.getSocket()->getWriteStream() << "\tHELP" <<endl;
    }
#line 1904 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"
    break;


#line 1908 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.cpp"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYEMPTY : YYTRANSLATE (yychar);

  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (scanner, queryMgr, YY_("syntax error"));
#else
# define YYSYNTAX_ERROR yysyntax_error (&yymsg_alloc, &yymsg, \
                                        yyssp, yytoken)
      {
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = YYSYNTAX_ERROR;
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == 1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = (char *) YYSTACK_ALLOC (yymsg_alloc);
            if (!yymsg)
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = 2;
              }
            else
              {
                yysyntax_error_status = YYSYNTAX_ERROR;
                yymsgp = yymsg;
              }
          }
        yyerror (scanner, queryMgr, yymsgp);
        if (yysyntax_error_status == 2)
          goto yyexhaustedlab;
      }
# undef YYSYNTAX_ERROR
#endif
    }



  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, scanner, queryMgr);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYTERROR;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;


      yydestruct ("Error: popping",
                  yystos[yystate], yyvsp, scanner, queryMgr);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;


#if !defined yyoverflow || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (scanner, queryMgr, YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif


/*-----------------------------------------------------.
| yyreturn -- parsing is finished, return the result.  |
`-----------------------------------------------------*/
yyreturn:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, scanner, queryMgr);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  yystos[*yyssp], yyvsp, scanner, queryMgr);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  return yyresult;
}
#line 449 "/Users/bzcschae/workspace/dbs2/HubDB/DBLib/DBParser.y"


void extyyerror(const char *str,DBQueryMgr & queryMgr)
{
    LOG4CXX_INFO(loggerParser,str);
	queryMgr.getSocket()->getWriteStream() << str;
}

int yywrap(yyscan_t yyscanner)
{
	DBQueryMgr * queryMgr = (DBQueryMgr *)yyget_extra(yyscanner);
	DBServerSocket * socket = queryMgr->getSocket();
	if(socket->readFromSocket()>0){
		yy_scan_string(socket->getReadString().c_str(),yyscanner);
		return 0;
   	}
	return 1;
}

void parse(DBQueryMgr & queryMgr){
  	yyscan_t scanner;
	yylex_init ( &scanner );
	yyset_extra(&queryMgr,scanner);
	yy_scan_string("",scanner);
	yyparse(scanner,queryMgr);
	yylex_destroy( scanner );
}

DBParserException::DBParserException(const std::string& msg1)
     : DBException(msg1) {
}

DBParserException::DBParserException(const DBParserException& src)
      : DBException(src) {
}

DBParserException& DBParserException::operator=(const DBParserException& src) {
      DBException::operator=(src);
      return *this;
}

DBParserSyntaxException::DBParserSyntaxException(const std::string& msg1)
     : DBParserException(msg1) {
}

DBParserSyntaxException::DBParserSyntaxException(const DBParserSyntaxException& src)
      : DBParserException(src) {
}

DBParserSyntaxException& DBParserSyntaxException::operator=(const DBParserSyntaxException& src) {
      DBException::operator=(src);
      return *this;
}

DBParserQuitException::DBParserQuitException(const std::string& msg1)
     : DBParserException(msg1) {
}

DBParserQuitException::DBParserQuitException(const DBParserQuitException& src)
      : DBParserException(src) {
}

DBParserQuitException& DBParserQuitException::operator=(const DBParserQuitException& src) {
      DBException::operator=(src);
      return *this;
}

