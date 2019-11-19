%option reentrant
%{
    
#include <hubDB/DBParserDefs.h>
#include <hubDB/DBParser.h>

%}

%option case-insensitive
%s STATE_COMMENT
%s STATE_VALUE

CHAR [a-zA-Z]
DIGIT [0-9]

list             (list)
join             (join)
get              (get)
and              (and)
schema           (schema)
unique           (unique)
for              (for)
index            (index)
type             (type)
create           (create)
drop             (drop)
table            (table)
tables           (tables)
insert           (insert)
into             (into)
values           (values)
select           (select)
delete           (delete)
from             (from)
where            (where)
in               (in)
on               (on)
import           (import)
export           (export)
quit             (quit)
connect          (connect)
to               (to)
disconnect       (disconnect)
database         (database)
db	             (db)
as               (as)
intT			 (integer)
doubleT			 (double)
vchar			 (varchar)
print			 (print)
stat1			 (statistics)
stat2			 (stat)
classes			 (classes)
help             (help)
comment          --
blank            [ \t]+
dbname           [a-zA-Z][a-zA-Z0-9]*
equals           [=]
star             [*]
para_open        [(]
para_close       [)]
commata          [,]
dot              [.]
semicolon        [;]
apostrophe       [\']
digit            [0-9]
number           {digit}+
realnumber       {digit}+{dot}{digit}+
nl               [\n]
cr               [\r]
all              [^\']*
all_comment      [^\n]*
%%

<STATE_VALUE>{apostrophe}          {BEGIN 0; return APOSTROPHE;}
<STATE_VALUE>{all}                 {myyylval->str=strdup(yytext); return STR_VALUE;}
<STATE_COMMENT>{all_comment}       {}
<STATE_COMMENT>{nl}                {BEGIN 0;}
{list}                             {return LIST;}
{on}                               {return ON;}
{join}                             {return JOIN;}
{index}                            {return INDEX;}
{get}                              {return GET;}
{and}                              {return AND;}
{schema}                           {return SCHEMA;}
{unique}                           {return UNIQUE;}
{for}                              {return FOR;}
{type}                             {return TYPE;}
{create}                           {return CREATE;}
{drop}                             {return DROP;}
{database}                         {return DATABASE;}
{db}                               {return DATABASE;}
{stat1}							   {return STATISTICS;}
{stat2}							   {return STATISTICS;}
{classes}						   {return CLASSES;}
{print}							   {return PRINT;}
{help}                             {return HELP;}
{tables}                           {return TABLES;}
{table}                            {return TABLE;}
{insert}                           {return INSERT;}
{into}                             {return INTO;}
{values}                           {return VALUES;}
{select}                           {return SELECT;}
{delete}                          {return DELETE;}
{from}                             {return FROM;}
{where}                            {return WHERE;}
{to}                               {return TO;}
{import}                           {return IMPORT;}
{export}                          {return EXPORT;}
{quit}                             {return QUIT;}
{connect}                          {return CONNECT;}
{disconnect}                       {return DISCONNECT;}
{as}				               {return AS;}
{intT}				               {return INTT;}
{doubleT}			               {return DOUBLET;}
{vchar}			           		   {return VARCHAR;}
{in}                               {return IN;}
{apostrophe}                       {BEGIN STATE_VALUE; return APOSTROPHE;}
{realnumber}                       {myyylval->realnum=atof(yytext);return REALNUM;}
{number}                           {myyylval->num=atoi(yytext);return NUM;}
{dbname}                           {myyylval->str=strdup(yytext);return DBNAME;}
{equals}                           {return EQUALS;}
{star}                             {return STAR;}
{para_open}                        {return PARA_OPEN;}
{para_close}                       {return PARA_CLOSE;}
{commata}                          {return COMMATA;}
{semicolon}                        {return SEMICOLON;}
{dot}                              {return DOT;}
{comment}                          {BEGIN STATE_COMMENT;}
{nl}                               {BEGIN 0;}
{cr}                               {}
{blank}                            {}
.                                  {extyyerror(yytext,*yyget_extra(yyscanner));}
%%
