#!/bin/bash


# +===========+
# | Anleitung |
# +===========+
#
# - Server mit dem gewuenschten Buffermanager starten
#   - ./bin/hubDBServer -b DBMyBufferMgr


# ===================================================================


# +===========+
# | Variablen |
# +===========+


DATABASE='TESTDB'
TABLE='angestellte'
COLUMNS_AB='I:s:s:d:i'
COLUMNS='id INTEGER UNIQUE, name VARCHAR, vorname VARCHAR, gehalt DOUBLE, noid INTEGER'


# ===================================================================


cd  ..
clear


# ===================================================================


# +=======+
# | Tests |
# +=======+


# +==========+
# | Lasttest |
# +==========+
# Einfuegen und Loeschen grosser Datenmengen

lasttest() {

echo
echo "+-------------------------------------------------------+"
echo "| Lasttest (Einfuegen und Loeschen grosser Datenmengen) |"
echo "+-------------------------------------------------------+"
echo "Tupelzahl = $1"
let A=$2-1
echo "Loesche Attribut \"noid\" von 0 bis $A"
echo

echo "Erzeuge Tupel und fuelle Datenbank"
echo "----------------------------------"
perl tools/createTestTuple.pl -r $1 -F $COLUMNS_AB -o datafile.txt

echo "CREATE DATABASE $DATABASE;
CONNECT TO $DATABASE;
CREATE TABLE $TABLE AS ($COLUMNS);
SELECT * FROM $TABLE;
IMPORT FROM 'datafile.txt' INTO $TABLE;
SELECT * FROM $TABLE;
DISCONNECT;
QUIT;" | hubDBClient

echo
echo "Beginne mit dem Loeschen"
echo "------------------------"
echo
LIMIT=$2
for ((a=0; a < LIMIT ; a++))
do
	echo "Loesche \"noid = $a\""
	echo "CONNECT TO $DATABASE; DELETE FROM $TABLE WHERE $TABLE.noid = $a; DISCONNECT; QUIT;" | hubDBClient
done

echo
echo "Loesche Datenbank"
echo "-----------------"
echo "CONNECT TO $DATABASE;
SELECT * FROM $TABLE;
DELETE FROM $TABLE;
SELECT * FROM $TABLE;
DISCONNECT;
DROP DATABASE $DATABASE;
QUIT;" | hubDBClient

rm datafile.txt

}


# ===================================================================


# +=====================+
# | Das "Hauptprogramm" |
# +=====================+


rm -rf $DATABASE

# Tests
lasttest 1000 50
# 1. Parameter: Anzahl Tupel
# 2. Parameter: Zu Loeschende Tupel (0 bis zu diesem Wert-1, maximaler Wert = 99)