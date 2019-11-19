#!/bin/bash


# +===========+
# | Anleitung |
# +===========+
#
#
# - Server mit dem gewuenschten Querymanager starten
#   - ./bin/hubDBServer -q DBMyQueryMgr


# ===================================================================


# +===========+
# | Variablen |
# +===========+


DATABASE='TESTDB'
TABLE='angestellte'
TABLE2='personen'
INDEXCLASS='DBSeqIndex'
INDEXCLASS2='DBMyIndex'
COLUMNS_AB='I:S:S:D:I'
COLUMNS='id INTEGER UNIQUE, name VARCHAR UNIQUE, vorname VARCHAR UNIQUE, gehalt DOUBLE UNIQUE, noid INTEGER UNIQUE'


# ===================================================================


cd  ..
clear


# ===================================================================


# +=======+
# | Tests |
# +=======+


# +------------------+
# | 0. Hilfsfunktion |
# +------------------+

# Hilfsfunktion: Erstellt die Beispieltabellen und Indizes
erstelleTabelle() {

echo "CREATE DATABASE $DATABASE;
CONNECT TO $DATABASE;
CREATE TABLE $TABLE AS ($COLUMNS);
CREATE INDEX $TABLE.id TYPE '$1';
CREATE INDEX $TABLE.name TYPE '$1';
CREATE INDEX $TABLE.noid TYPE '$1';
DISCONNECT;
QUIT;" | hubDBClient

echo "CONNECT TO $DATABASE;
CREATE TABLE $TABLE2 AS ($COLUMNS);
CREATE INDEX $TABLE2.noid TYPE '$1';
DISCONNECT;
QUIT;" | hubDBClient

}



# +----------+
# | Lasttest |
# +----------+
# Einfuegen und Loeschen grosser Datenmengen

lasttest() {

echo
echo "+-------------------------------------------------------+"
echo "| Lasttest (Einfuegen und Loeschen grosser Datenmengen) |"
echo "+-------------------------------------------------------+"
echo "Tupelzahl = $1"
let A=$2-1
let B=$2+1
echo "Loesche Attribut \"noid\" von 0 bis $A in $TABLE"
echo "Loesche Attribut \"noid\" von $B bis 99 in $TABLE2"
echo "Join ueber Attribut \"noid\""
echo "Auffuellen"
echo "Join ueber Attribut \"id\""
echo

echo "Erzeuge Tupel und fuelle Datenbank"
echo "----------------------------------"
perl tools/createTestTuple.pl -r $1 -F $COLUMNS_AB -o datafile.txt

erstelleTabelle $3

echo "CONNECT TO $DATABASE;
IMPORT FROM 'datafile.txt' INTO $TABLE;
IMPORT FROM 'datafile.txt' INTO $TABLE2;
DISCONNECT;
QUIT;" | hubDBClient $CARGS > /dev/null

echo
echo "Beginne mit dem Loeschen"
echo "------------------------"
echo
LIMIT=$2
for ((a=0; a < LIMIT; a++))
do
	echo "Loesche \"noid = $a\""
	echo "CONNECT TO $DATABASE; DELETE FROM $TABLE WHERE $TABLE.noid = $a; DISCONNECT; QUIT;" | hubDBClient $CARGS > /dev/null
done
for ((a=LIMIT+1; a < 100 ; a++))
do
	echo "Loesche \"noid = $a\""
	echo "CONNECT TO $DATABASE; DELETE FROM $TABLE2 WHERE $TABLE2.noid = $a; DISCONNECT; QUIT;" | hubDBClient $CARGS > /dev/null
done

# 2. Select: Genau $1 viele Tupel
echo
echo "Selektiere und loesche danach Datenbank"
echo "---------------------------------------"
echo "CONNECT TO $DATABASE;
SELECT $TABLE.id, $TABLE.noid, $TABLE2.id, $TABLE2.noid FROM $TABLE JOIN $TABLE2 ON $TABLE.noid = $TABLE2.noid;
IMPORT FROM 'datafile.txt' INTO $TABLE;
IMPORT FROM 'datafile.txt' INTO $TABLE2;
SELECT $TABLE.id, $TABLE.noid, $TABLE2.id, $TABLE2.noid FROM $TABLE JOIN $TABLE2 ON $TABLE.id = $TABLE2.id;
DISCONNECT;
DROP DATABASE $DATABASE;
QUIT;" | hubDBClient $CARGS #> /dev/null

rm datafile.txt

}


# ===================================================================


# +=====================+
# | Das "Hauptprogramm" |
# +=====================+


rm -rf $DATABASE

lasttest 1000 50 $INDEXCLASS
lasttest 1000 50 $INDEXCLASS2
# 1. Parameter: Anzahl Tupel
# 2. Parameter: Zu loeschende Tupel (0 bis zu diesem Wert-1)