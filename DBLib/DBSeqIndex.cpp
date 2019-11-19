#include <hubDB/DBSeqIndex.h>
#include <hubDB/DBException.h>

using namespace HubDB::Index;
using namespace HubDB::Exception;

LoggerPtr DBSeqIndex::logger(Logger::getLogger("HubDB.Index.DBSeqIndex"));

// registerClass()-Methode am Ende dieser Datei: macht die Klasse der Factory bekannt
int rSeqIdx = DBSeqIndex::registerClass();

// set static const rootBlockNo to 0 in DBSeqIndex Class - (BlockNo=uint)
const BlockNo DBSeqIndex::rootBlockNo(0);

// max this amount of entries with the some value in the index - WHEN NOT UNIQUE
const uint DBSeqIndex::MAX_TID_PER_ENTRY(20);

// Funktion bekannt machen
extern "C" void *createDBSeqIndex(int nArgs, va_list ap);

/**
 * Ausgabe des Indexes zum Debuggen
 */
string DBSeqIndex::toString(string linePrefix) const {
  stringstream ss;
  ss << linePrefix << "[DBSeqIndex]" << endl;
  ss << DBIndex::toString(linePrefix + "\t") << endl;
  ss << linePrefix << "tidsPerEntry: " << tidsPerEntry << endl;
  ss << linePrefix << "entriesPerPage: " << entriesPerPage() << endl;
  ss << linePrefix << "-----------" << endl;
  return ss.str();
}

/**
 * Konstruktor
 * @param bufferMgr Referenz auf Buffermanager
 * @param file Referenz auf Dateiobjekt
 * @param attrType Typ des Indexattributs
 * @param mode Accesstyp: READ, WRITE - siehe DBTypes.h
 * @param unique ist Attribute unique
 */
DBSeqIndex::DBSeqIndex(DBBufferMgr &bufferMgr, DBFile &file,
                       enum AttrTypeEnum attrType, ModType mode, bool unique) :
// call base constructor
        DBIndex(bufferMgr, file, attrType, mode, unique),

        // falls unqiue, dann nur ein Tupel pro value, ansonsten den Wert vom oben gesetzen MAX_TID_PER_ENTRY
        tidsPerEntry(unique ? 1 : MAX_TID_PER_ENTRY)
{

  if (logger != NULL) {
    LOG4CXX_INFO(logger, "DBSeqIndex()");
  }
  assert(entriesPerPage() > 1);

  // if this function is called for the first time -> index file has 0 blocks
  // -> call initializeIndex to create file
  if (bufMgr.getBlockCount(file) == 0) {
    initializeIndex();
  }

  // füge den ersten Block der Indexdatei dem Stack der fixierten Blöcke hinzu
  DBBACB rootBlock = bufMgr.fixBlock(
          file, rootBlockNo,mode == READ ? LOCK_SHARED : LOCK_INTWRITE);

  bacbStack.push(rootBlock);

  if (logger != NULL) {
    LOG4CXX_DEBUG(logger, "this:\n" + toString("\t"));
  }
}

/**
 * Destruktor
 */
DBSeqIndex::~DBSeqIndex() {
  LOG4CXX_INFO(logger, "~DBSeqIndex()");
  unfixBACBs(false);
}

/**
 * Freigeben aller vom Index fixierten Blöcke im BufferManager
 * @param setDirty
 */
void DBSeqIndex::unfixBACBs(bool setDirty) {
  LOG4CXX_INFO(logger, "unfixBACBs()");
  LOG4CXX_DEBUG(logger, "setDirty: " + TO_STR(setDirty));
  LOG4CXX_DEBUG(logger, "bacbStack.size()= " + TO_STR(bacbStack.size()));
  while (!bacbStack.empty()) {
    try {
      if (bacbStack.top().getModified()) {
        if (setDirty) {
          bacbStack.top().setDirty();
        }
      }
      bufMgr.unfixBlock(bacbStack.top());
    } catch (DBException e) {
    }
    bacbStack.pop();
  }
}

/**
  * Gibt die Anzahl der Einträge pro Seite zurueck
  *
  * Gesamtblockgroesse: DBFileBlock::getBlockSize()
  * Platz zum Speichern der Position des ersten freien Blocks: sizeof(uint)
  * Laenge des Schluesselattributs: DBAttrType::getSize4Type(attrType)
  * Grösse der TID (siehe DBTypes.h): sizeof(TID)
  *
  * Einträge (TIDs) pro Schlüsseleintrag (maximal MAX_TID_PER_ENTRY or 1 when unique): tidsPerEntry
  *
  * Bsp.: VARCHAR: entriesPerPage() = (1024-4) / (30+8*20) = 5, Rest 70
  * Bsp.: INTEGER UNIQUE: entriesPerPage() = (1024-4) / (4+8*1) = 85, Rest 0
  *
  * @return
  */
uint DBSeqIndex::entriesPerPage() const {
  return (DBFileBlock::getBlockSize() - sizeof(uint)) /
         (DBAttrType::getSize4Type(attrType) + sizeof(TID) * tidsPerEntry);
}

/**
 * Erstellt Indexdatei.
 */
void DBSeqIndex::initializeIndex() {
  LOG4CXX_INFO(logger, "initializeIndex()");
  if (bufMgr.getBlockCount(file) != 0) {
    throw DBIndexException("can not initialize existing table");
  }

  try {
    // einen ersten Block der Datei erhalten und gleich fixieren
    DBBACB firstBlock = bufMgr.fixNewBlock(file);

    // Block auf modifiziert ändern
    firstBlock.setModified();

    bufMgr.unfixBlock(firstBlock);
  } catch (DBException e) {
    throw e;
  }

}

/**
 * Sucht im Index nach einem bestimmten Wert
 *
 * Die Funktion ändert die übergebene Liste
 * von TID Objekten (siehe DBTypes.h: typedef list<TID> DBListTID;)
 *
 * @param val  zu suchender Schluesselwert
 * @param tids Leere Liste. Wird als Rückgabe verwendet. Referenz auf Liste von TID Objekten
 */
void DBSeqIndex::find(const DBAttrType &val, DBListTID &tids) {
  LOG4CXX_INFO(logger, "find()");
  LOG4CXX_DEBUG(logger, "val:\n" + val.toString("\t"));

  // ist genau eine Seite fixiert?
  checkBacbStackInvariant();

  // Löschen der übergebenen Liste ("Returnliste")
  tids.clear();

  // die erste Seite suchen auf der ein Tupel mit einem bestimmten Wert vorkommen könnte
  BlockNo page;
  if (findFirstPage(val, page)) {
    // wenn Page gefunden, dann auf dieser Page weitersuchen und TID List fuellen
    findFromPage(val, page, tids);
  }

  // ist genau eine Seite fixiert?
  checkBacbStackInvariant();
}

/**
 * Einfügen eines Schluesselwertes (moeglicherweise bereits vorhangen)
 * zusammen mit einer Referenz auf eine TID.
 *
 * @param val Schlüsselwert
 * @param tid
 */
void DBSeqIndex::insert(const DBAttrType &val, const TID &tid) {
  LOG4CXX_INFO(logger, "insert()");
  LOG4CXX_DEBUG(logger, "val:\n" + val.toString("\t"));
  LOG4CXX_DEBUG(logger, "tid: " + tid.toString());

  // ist genau eine Seite fixiert?
  checkBacbStackInvariant();

  // Den Root-Block exclusive locken
  if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE) {
    bufMgr.upgradeToExclusive(bacbStack.top());
  }

  // Suchen auf welcher Seite die Tupel mit Wert val im Index liegen könnten
  BlockNo page;
  findFirstPage(val, page);

  // nun Einfügen in diese Seite
  insertInPage(val, tid, page);

  // ist genau eine Seite fixiert?
  checkBacbStackInvariant();
}

/**
 * Entfernt alle Tupel aus der Liste der tids.
 *
 * Um schneller auf der richtigen Seite anfangen zu können,
 * wird zum Suchen zusätzlich noch der zu löschende Wert "value" übergeben
 *
 * @param valToRemove Der zu löschende Wert
 * @param tidsToRemove Die Tupel Ids (mindestens eine - mehrere möglich, falls Duplikate erlaubt sind)
 */
void DBSeqIndex::remove(const DBAttrType &valToRemove, const list<TID> &tidsToRemove) {
  LOG4CXX_INFO(logger, "remove()");
  LOG4CXX_DEBUG(logger, "valToRemove:\n" + valToRemove.toString("\t"));

  // ist genau eine Seite fixiert
  checkBacbStackInvariant();

  // Den Root-Block "exclusive" locken
  if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE) {
    bufMgr.upgradeToExclusive(bacbStack.top());
  }

  // wenn das Indexattribut unique ist, dann darf in der Liste TID nie mehr als ein Wert stehen
  if (unique && tidsToRemove.size() > 1) {
    throw DBIndexUniqueKeyException("trying to remove multiple keys in a unique index");
  }

  // Suchen auf welcher Seite Tupel mit valToRemove im Index liegen
  BlockNo page;
  if (findFirstPage(valToRemove, page)) {
    // sobald Seite gefunden: löschen der TIDs
    removeFromPage(valToRemove, tidsToRemove, page);
  } else {
    // ansonsten Fehler werfen
    throw DBIndexException("key not found (valToRemove:\n" + valToRemove.toString("\t") + ")");
  }

  // ist genau eine Seite fixiert
  checkBacbStackInvariant();
}

/**
 * Prüft, dass am Anfang/Ende der Operation genau eine Seite für den Root-Block fixiert ist
 */
void DBSeqIndex::checkBacbStackInvariant() {
  if (bacbStack.size() != 1) {
    throw DBIndexException("BACB Stack is invalid");
  }
}

/**
 * Sucht erste Seite auf der ein bestimmter Value in einem Tupel stehen könnte
 *
 * Da die Seiten aufsteigend sortiert gespeichert werden, wird eine Binäre Suche verwendet.
 *
 * Sonderfall: falls der erste Wert in einer Seite den gesuchten Wert enhält, könnte ein Duplikat
 * auf einer vorherigen Seite liegen. Daher muss dann die vorherige Seite noch untersucht werden.
 *
 */
bool DBSeqIndex::findFirstPage(const DBAttrType &val, BlockNo &blockNo) {
  LOG4CXX_INFO(logger, "findFirstPage()");
  LOG4CXX_DEBUG(logger, "val:\n" + val.toString("\t"));

  // ist genau eine Seite fixiert?
  checkBacbStackInvariant();

  bool found = false;
  bool potentialDuplikate = false; // mehr als 1 Block enthält den Wert (bei Duplikaten)
  BlockNo blockPotentialDuplikate;      // der zweite Block mit dem Wert (bei Duplikaten)

  // Bei rootBlockNo starten (ersten Seite)
  blockNo = rootBlockNo;

  // wenn die (Root-)Seite Tupel enthält
  if (!isEmpty(bacbStack.top().getDataPtr())) {

    // speicher Nummer des ersten und letzten Blocks der Datei
    BlockNo left = rootBlockNo;
    BlockNo right = bufMgr.getBlockCount(file);

    LOG4CXX_DEBUG(logger, "left:" + TO_STR(left));
    LOG4CXX_DEBUG(logger, "right:" + TO_STR(right));

    // binäre Suche durch alle Blöcke der Datei
    while (left < right && !found) {
      // mittleren Wert bestimmen
      blockNo = left + (right - left) / 2;
      LOG4CXX_DEBUG(logger, "left:" + TO_STR(left));
      LOG4CXX_DEBUG(logger, "right:" + TO_STR(right));
      LOG4CXX_DEBUG(logger, "blockNo:" + TO_STR(blockNo));

      // zusätzliche Blöcke lesen
      DBBACB current = fixNonRootBlock(blockNo, LOCK_SHARED);

      // Setze firstVal und lastVal Attribute dieses Objektes
      // (Wert des ersten / letzten Value des Blocks)
      std::shared_ptr<SequentialIndex> index = SequentialIndex::read(current.getDataPtr(), attrType, tidsPerEntry);

      // Erstes und letztes Element eines Blocks
      std::shared_ptr<DBAttrType> firstVal = index->entries[0].attr;
      std::shared_ptr<DBAttrType> lastVal = index->entries[index->countEntries-1].attr;

      LOG4CXX_DEBUG(logger, "first:" + firstVal->toString());
      LOG4CXX_DEBUG(logger, "last:" + lastVal->toString());

      // Wert befindet sich auf einem vorherigen Block
      if (val < *firstVal) {
        right = blockNo;
      }
        // Wert befindet sich auf einem nachfolgenden Block
      else if (val > *lastVal) {
        left = blockNo + 1;
      }
        // wenn nicht vor dem aktuellen und nicht danach dann KANN
        // (wenn ueberhaupt vorhanden) der Wert nur im aktuellen Block sein
      else if (unique) {
        found = true;
      }
        // wenn erster Wert gleich dem gesuchten Wert ist, dann könnte ein
        // Duplikat auf dem vorherigen Block liegen
      else if (val == *firstVal) {
        // wenn auf der ersten Seite, dann dann gibt es keinen vorherigen Block
        if (blockNo == rootBlockNo) {
          found = true;
        }
          // wenn nicht, dann könnte im vorherigen Block auch der gesuchte Wert stehen
          // Problematisch: Wenn sich der Wert über drei oder noch mehr Blöcke verteilt (entriesPerPage: 85)
        else {
          left = blockNo - 1;
          right = blockNo;

          potentialDuplikate = true;
          blockPotentialDuplikate = blockNo;
        }
      } else {
        // ansonsten kann der Wert auch hoechsten auf der aktuellen Seite sein
        found = true;
      }

      // wenn eine Seite nach der ersten gesperrt wurde - diese Sperre wieder aufheben
      unfixNonRootBlock(current);
    }
  }

  // wenn im aktuellen Block der Wert nicht gefunden wurde, aber im nachfolgenden
  if (!found && potentialDuplikate) {
    found = true;
    blockNo = blockPotentialDuplikate;
  }

  // ist genau eine Seite fixiert?
  checkBacbStackInvariant();

  LOG4CXX_DEBUG(logger, "found: " + TO_STR(found));
  LOG4CXX_DEBUG(logger, "blockNo: " + TO_STR(blockNo));
  return found;
}

/**
 * Durchsucht eine bestimmte Seite nach einem Wert
 *
 *
 * An erster Stelle einer Seite steht ein uint der die erste FREIE Stelle in der Seite angibt

 * Jeder Eintrag enthält den Schlüssel und mehrere Einträge (TIDs) gespeichert.
 * (maximal MAX_TID_PER_ENTRY or 1 falls der Index unique ist)
 *
 * Es wird die übergebene Liste tidsFound gefüllt
 *
 * @param val
 * @param blockNo
 * @param tidsFound Rückgabewert
 */
void DBSeqIndex::findFromPage(const DBAttrType &val, BlockNo blockNo, list<TID> &tidsFound) {
  LOG4CXX_INFO(logger, "findFromPage()");
  LOG4CXX_DEBUG(logger, "val:\n" + val.toString("\t"));

  // ist genau eine Seite fixiert?
  checkBacbStackInvariant();

  // von Seite mit dem ersten zum Value passenden Tupel suche ueber die
  // folgenden Seite bis Values groesser sind als der jetzige
  bool done = false;
  while (!done
         && blockNo < bufMgr.getBlockCount(file)) {

    // wenn eine andere Seite als die erste angefasst wird -> sperren der Seite
    DBBACB block = fixNonRootBlock(blockNo, LOCK_SHARED);

    // Pointer zum Anfang der Seite
    const char *ptr = block.getDataPtr();
    std::shared_ptr<SequentialIndex> index = SequentialIndex::read(ptr, attrType, tidsPerEntry);

    // Mindestens ein Eintrag muss vorhanden sein
    if (index->countEntries == 0) {
      throw DBIndexException("Invalid Index Page");
    }

    // iteriere über alle gefüllten Tupelfelder der Seite
    // (countEntries ist die Stelle des ersten freien Tupels)
    for (uint i = 0; !done && i < index->countEntries; ++i) {
      // Lese Attributwert an derzeitigen Stelle der Seite
      // setzt Pointer weiter
      std::shared_ptr<DBAttrType> attr = index->entries[i].attr;

      // falls der Wert gefunden wurde
      if (*attr == val) {
        std::vector<TID> tidList = index->entries[i].tidList;

        // dann iteriere über alle TIDs und füge sie in die tidsFound-Liste ein
        for (auto TID : tidList) {
          LOG4CXX_DEBUG(logger, "appending tid: " + TID.toString());
          tidsFound.push_back(TID);
        }
      }
        // wenn bereits an gesuchtem Wert vorbei - while-Schleife beenden
      else if (*attr > val) {
        done = true;
      }
    }

    // Seiten weider freigeben
    unfixNonRootBlock(block);

    // ein Block weiter
    ++blockNo;
  }

  // ist genau eine Seite fixiert?
  checkBacbStackInvariant();
}

/**
 * Gibt den Block frei und entfernt ihn vom Stack
 */
void DBSeqIndex::unfixNonRootBlock(const DBBACB & dbbacb) {
  if (dbbacb.getBlockNo() != rootBlockNo) {
    bufMgr.unfixBlock(bacbStack.top());
    bacbStack.pop();
  }
}

/**
 * Lädt den Block und speichert in auf dem Stack
 */
DBBACB& DBSeqIndex::fixNonRootBlock(const BlockNo & blockNo, const DBBCBLockMode & mode) {
  if (blockNo != rootBlockNo) {
    bacbStack.push(bufMgr.fixBlock(file, blockNo, mode));
  }
  return bacbStack.top();
}

/**
 * Löscht alle Tupel mit den übergebenen TIDs,
 * Es wird angefangen auf der Seite mit Blocknummer blockNo
 *  - Dort wird der passende Wert gesucht und alle übergebenen Ids gelöscht
 *  - Ist noch ein nachfolgender Block vorhanden, muss evtl auch in diesem gelöscht werden
 *
 * Die TID-Liste wird immmer terminiert mit: invalidTID
 *
 * @param valToRemove Der Wert, der entfernt werden soll
 * @param tidsToRemove Die zu entfernenden TIDs
 * @param startBlockNo Der erste Block (es kann sein, dass im darauffolgenden Block gelöscht werden muss)
 */
void DBSeqIndex::removeFromPage(const DBAttrType &valToRemove, list<TID> tidsToRemove, BlockNo startBlockNo) {
  LOG4CXX_INFO(logger, "removeFromPage()");
  LOG4CXX_DEBUG(logger, "valToRemove:\n" + valToRemove.toString("\t"));
  LOG4CXX_DEBUG(logger, "blockNo: " + TO_STR(startBlockNo));

  // ist genau eine Seite fixiert?
  checkBacbStackInvariant();

  // Den Root-Block exclusive locken
  if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE) {
    bufMgr.upgradeToExclusive(bacbStack.top());
  }

  // Set (hash) erstellen. Entfernt doppelte Objekte in der Liste
  unordered_set<TID, hash_TIDs> uniqueTids(
          std::begin(tidsToRemove), std::end(tidsToRemove));

  // Anzahl der TID-Objekte in der Liste ermitteln
  uint cntTIDs = uniqueTids.size();

  // Durchlauf der Schleife von blockNummer
  // (der Block bei dem wir mit dem Entfernen anfangen)
  // bis zur Gesamtseitenzahl der Datei hochgezählt ist
  bool done = false;
  while (!done && cntTIDs != 0
         && startBlockNo < bufMgr.getBlockCount(file)) {

    // wenn der aktuelle Block nicht dem ersten entspricht, dann "exclusive" sperren
    DBBACB startBlock = fixNonRootBlock(startBlockNo, LOCK_EXCLUSIVE);

    std::shared_ptr<SequentialIndex> index = SequentialIndex::read(startBlock.getDataPtr(), attrType, tidsPerEntry);

    // Kein Eintrag vorhanden
    if (index->countEntries == 0) {
      throw DBIndexException("Invalid Index Page");
    }

    // iteriere über alle gefüllten Tupelfelder der Seite
    // solange überhaupt noch TIDs zum Löschen vorhanden sind
    uint i = 0;
    while (i < index->countEntries && !done && cntTIDs != 0) {

      // Eintrag lesen
      std::shared_ptr<DBAttrType> attr = index->entries[i].attr;

      // ptrNext zeigt nun auf die TID-Liste
      // std::vector<TID>& tidList = index->entries[i].tidList;

      // wenn TID mit valToRemove gefunden
      if (*attr == valToRemove) {

        // dann gehe für alle tidsToRemove alle Tids durch
        uint c = 0;
        while (c < index->entries[i].tidList.size() && cntTIDs != 0) {
          // Die aktuelle TID soll entfernt werden
          if (uniqueTids.find(index->entries[i].tidList[c]) != uniqueTids.end()) {
            // Page auf modified setzen, damit sie auf Platte geschrieben werden kann
            startBlock.setModified();

            // Löscht den c-ten Eintrag.
            // alle Tids 1 nach vorne kopieren, und die Laufvariable c nicht erhöhen
            index->entries[i].tidList.erase(index->entries[i].tidList.begin() + c);

            // Eine TID wurde gelöscht
            cntTIDs--;
          }
          else {
            c++;
          }
        }

        // wenn beim Entfernen die einzige vorhandene TID zu einem Value entfernt wurde
        if (index->entries[i].tidList.empty()) {
          // Dann diesen Eintrag entfernen
          index->countEntries--;
          index->entries.erase(index->entries.begin() + i);
        }
          // ansonsten nächstes Feld prüfen
        else {
          ++i;
        }
      }
        // Falls value bereits zu hoch ist, abbrechen
      else if (*attr > valToRemove) {
        done = true;
      }
        // ansonsten nächstes Feld anschauen (*attr < valToRemove)
      else {
        ++i;
      }

    }

    if (startBlock.getModified()) {
      // Rohdaten in Block schreibe
      index->write(startBlock.getDataPtr(), attrType, tidsPerEntry);
    }

    // falls keine verbliebenen Tids vorhanden sind, kann die ganze Seite geloescht werden
    bool remove = (index->countEntries == 0);

    // Seite entsperren
    unfixNonRootBlock(bacbStack.top());

    // prüfen, ob Seite gelöscht werden soll
    if (remove && bufMgr.getBlockCount(file) > 1) {
      removeEmptyPage(startBlockNo);
    }
      // Die darauffolgende Seite untersuchen
    else {
      ++startBlockNo;
    }
  } // while loop solange wie: startBlockNo < bufMgr.getBlockCount(file)

  // ist genau eine Seite fixiert?
  checkBacbStackInvariant();

  // wurden alle Tupel mit den uebergebenen TIDs gelöscht?
  if (cntTIDs > 0) {
    throw DBIndexException("could not remove all tidsToRemove");
  }
}

/**
 * Einfügen eines neuen (möglicherweise bereits vorhandenen) Werts und
 * der dazugehörige TID in eine Seite.
 *
 * Start bei Seite mit blockNo
 *
 * @param valToInsert Der einzufügende Wert
 * @param tidToInsert Die einzugügende TID
 * @param blockNo Der Block, in den eingefügt werden soll
 */
void DBSeqIndex::insertInPage(const DBAttrType &valToInsert, const TID &tidToInsert, BlockNo blockNo) {
  LOG4CXX_INFO(logger, "insertInPage()");
  LOG4CXX_DEBUG(logger, "valToInsert:\n" + valToInsert.toString("\t"));
  LOG4CXX_DEBUG(logger, "tidToInsert: " + tidToInsert.toString());
  LOG4CXX_DEBUG(logger, "blockNo: " + TO_STR(blockNo));

  // ist genau eine Seite fixiert?
  checkBacbStackInvariant();

  // die oberste Seite exclusive locken
  if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE) {
    bufMgr.upgradeToExclusive(bacbStack.top());
  }

  bool done = false;

  // bool zum Speichern ob überhaupt Platz für den Wert vorhanden ist
  // -> möglicherweite muss eine neue Seite eingefügt werden
  bool inserted = false;

  // Schleife von blockNummer (der Block wo wir anfangen mit Einfügen)
  // bis zur Gesamtseitenzahl der Datei hochzählen
  while (!done
         && blockNo < bufMgr.getBlockCount(file)) {

    // Exclusive sperren
    LOG4CXX_DEBUG(logger, "blockNo: " + TO_STR(blockNo));
    DBBACB block = fixNonRootBlock(blockNo, LOCK_EXCLUSIVE);

    std::shared_ptr<SequentialIndex> index = SequentialIndex::read(block.getDataPtr(), attrType, tidsPerEntry);

    // da der erste Wert die Position der ersten freie Stelle speichert,
    // die Stelle in posToInsert merken
    uint posToInsert = index->countEntries;

    LOG4CXX_DEBUG(logger, "countEntries: " + TO_STR(posToInsert));

    // nun vom Anfang bis zur ersten freien Stelle, alle Felder mit TID Objekten durchlaufen
    for (uint i = 0; i < posToInsert && !done; ++i) {
      // Eintrag lesen
      std::shared_ptr<DBAttrType> attr = index->entries[i].attr;

      LOG4CXX_DEBUG(logger, "attr:\n" + attr->toString("\t"));

      // wenn value gefunden: in TID Liste einfügen
      if (valToInsert == *attr) {
        // bereits den Key gefunden der jetzt eingefügt werden soll UND Unique führt zu einer Exception
        if (isUnique()) {
          throw DBIndexUniqueKeyException("Attr already in index");
        }

        // Iteriere tidsPerEntry
        for (uint c = 0; c < tidsPerEntry; ++c) {
          // Exception, falls der Wert schon vorhanden ist
          if (index->entries[i].tidList[c] == tidToInsert) {
            throw DBIndexUniqueKeyException("Attr, tidToInsert already in index");
          }
        }

        // gehe bis an das Ende der TID Liste
        if (index->entries[i].tidList.size() < entriesPerPage()
            && !inserted) {
          // wenn noch nichts eingefügt wurde dann genau hier die neue TID einfuegen
          block.setModified();
          index->entries[i].tidList.push_back(tidToInsert);

          // Es wurde in die Liste eingefügt
          inserted = true;
        }
      }
        // falls einzufügender Wert kleiner als der aktuelle Wert,
        // posToInsert auf die Stelle setzen, wo value später eingefügt werden soll
      else if (valToInsert < *attr) {
        posToInsert = i;
        done = true;
      }
    } // for

    if (block.getModified()) {
      // Rohdaten in Block schreibe
      index->write(block.getDataPtr(), attrType, tidsPerEntry);
    }

    LOG4CXX_DEBUG(logger, "posToInsert: " + TO_STR(posToInsert));
    LOG4CXX_DEBUG(logger, "inserted: " + TO_STR(inserted));
    LOG4CXX_DEBUG(logger, "done: " + TO_STR(done));

    // TID/Value muss neu eingefügt werden
    if (!inserted) {
      bool unfix = false;

      // Es konnte nicht eingefügt werden, da keine freie Stelle auf der Seite frei ist
      if (index->countEntries == entriesPerPage()) {
        LOG4CXX_DEBUG(logger, "split page countEntries: " + TO_STR(index->countEntries));

        // split the page as it is full
        splitPage(blockNo, posToInsert, unfix);
      }

      // nun ist platz vorhanden, insert kein Problem mehr
      inserted = true;

      DBBACB blockToInsert = bacbStack.top();

      // TODO nicht so schön, immernoch low-level

      // Stelle bestimmen wo Einfügen möglich ist
      uint *numberOfElements = (uint *) blockToInsert.getDataPtr();
      //std::shared_ptr<SequentialIndex> indexNew = SequentialIndex::read(blockToInsert.getDataPtr(), attrType, tidsPerEntry);

      LOG4CXX_DEBUG(logger, "posToInsert: " + TO_STR(posToInsert));
      LOG4CXX_DEBUG(logger, "numberOfElements: " + TO_STR(numberOfElements) + " " + TO_STR(*numberOfElements));

      // Byte-Stelle erhalten wo ich gerne einfügen möchte
      char *from = blockToInsert.getDataPtr() // Position des Blocks
                   + sizeof(uint) // Position der ersten freien Stelle
                   + getEntrySize() * posToInsert; // Anzahl Einträge

      LOG4CXX_DEBUG(logger, "from: " + TO_STR(from));

      // falls ich nicht am Ende einfügen möchte, müssen die Einträge verschoben werden
      if (*numberOfElements != posToInsert) {
        // um eine Position verschieben
        char *to = from + getEntrySize();
        memmove(to, from, getEntrySize() * (*numberOfElements - posToInsert));
      }

      LOG4CXX_DEBUG(logger, "valToInsert: " + valToInsert.toString("\t"));

      // Nun den neuen Wert einfügen
      from = valToInsert.write(from);

      // und die zu speichernde TID
      TID *tidList = (TID *) from;
      tidList[0] = tidToInsert;

      // invalidTID häengen
      if (!isUnique()) {
        tidList[1] = invalidTID;
      }

      // Anzahl Element um 1 erhähen
      ++*numberOfElements;
      LOG4CXX_DEBUG(logger, "numberOfElements: " + TO_STR(*numberOfElements));

      // neues modified date
      blockToInsert.setModified();

      // im Buffer unfixen!
      if (unfix) {
        bufMgr.unfixBlock(blockToInsert);
        bacbStack.pop();
      }

      if (isUnique()) {
        done = true;
      }
    } // if inserted

    // wieder nur die erste Seite gelockt am Ende der ganzen Methode
    unfixNonRootBlock(bacbStack.top());

    // und ein Block weiter schauen
    ++blockNo;
  } // while

  // ist genau eine Seite fixiert?
  checkBacbStackInvariant();

}

/**
 * Liefert die Größe eines Eintrags mit Attribut-Value und TID-Listen
 * @return
 */
size_t DBSeqIndex::getEntrySize() const {
  return (sizeof(TID) * tidsPerEntry + attrTypeSize);
}


/**
 * Teilt eine Seite auf zwei Seiten auf
 * @param countEntries
 * @param blockNo
 * @param posToInsert
 * @param unfix
 */
void DBSeqIndex::splitPage(BlockNo &blockNo,
                           uint &posToInsert,
                           bool &unfix) {
  DBBACB blockToSplit = bacbStack.top();

  // Setze prtOld auf eine Stelle nach der Hälfte der Seite
  std::shared_ptr<SequentialIndex> index = SequentialIndex::read(blockToSplit.getDataPtr(), attrType, tidsPerEntry);

  // Häfte bestimmen
  index->countEntries = (entriesPerPage() - entriesPerPage() / 2);

  LOG4CXX_DEBUG(logger, "leftcnt: " + TO_STR(index->countEntries));

  // Pointer weitersetzen um die Hälfte
  blockToSplit.setModified();

  // neue LEERE Seite am Ende einfuegen + locken + modify date setzen
  insertEmptyPage(blockToSplit.getBlockNo() + 1);

  DBBACB newBlock = bufMgr.fixBlock(file, blockToSplit.getBlockNo() + 1, LOCK_EXCLUSIVE);
  bacbStack.push(newBlock);
  newBlock.setModified();

  // Pointer auf Anfang des neuen Blocks
  std::shared_ptr<SequentialIndex> indexNew = SequentialIndex::read(newBlock.getDataPtr(), attrType, tidsPerEntry);

  // Zukünftigen Anzahl von Werten auf der neuen Seite ausrechnen
  // (Gesamtanzahl minus Anzahl nach der geteilt wird auf der alten Seite)
  indexNew->countEntries = entriesPerPage() - index->countEntries;

  LOG4CXX_DEBUG(logger, "rightcnt: " + TO_STR(indexNew->countEntries));

  // Die Daten von der alten Seite auf die neue Seite kopieren (also Werte und TIDs)
  indexNew->entries.assign(index->entries.begin()+index->countEntries, index->entries.end());
  index->entries.erase(index->entries.begin()+index->countEntries, index->entries.end());

  if (indexNew->entries.size() != indexNew->countEntries
      || index->entries.size() != index->countEntries) {
    throw DBIndexException("Invalid Entries size");
  }

  // do not check this block a second time
  ++blockNo;

  // Rohdaten in Block schreibe
  index->write(blockToSplit.getDataPtr(), attrType, tidsPerEntry);
  indexNew->write(newBlock.getDataPtr(), attrType, tidsPerEntry);

  // wenn die Position bei der geteilt wurde, größer ist als die Position bei der ich einfügen will,
  // dann kann der rechte lock freigegeben werden... alles weitere passiert im linken block
  if (posToInsert <= index->countEntries) {
    LOG4CXX_DEBUG(logger, "unfix right page");
    bufMgr.unfixBlock(newBlock);
    bacbStack.pop();
  }
    //  ansonsten liegt die Position auf der neuen Seite
    //  (minimiert um die Anzahl der Werte die auf der alten Seiten geblieben sind )
  else {
    posToInsert -= index->countEntries;
    unfix = true;
    LOG4CXX_DEBUG(logger, "new posToInsert: " + TO_STR(posToInsert));
  }
}

/**
 * Diese Methode fügt in die sequenzielle Indexdatei (* file)
 * einen Block an Position pos ein. Alle darauffolgenden Seiten werden verschoben
 * @param pos
 */
void DBSeqIndex::insertEmptyPage(BlockNo pos) {
  LOG4CXX_INFO(logger, "insertEmptyPage()");
  LOG4CXX_DEBUG(logger, "blockNo: " + TO_STR(pos));

  // Anzahl der fixierten Seiten
  uint s = bacbStack.size();

  // die oberste Seite des Stacks nun EXCLUSIVE locken
  if (bacbStack.top().getLockMode() != LOCK_EXCLUSIVE) {
    bufMgr.upgradeToExclusive(bacbStack.top());
  }

  // neue Page am Ende vom File fixieren
  DBBACB newPos = bufMgr.fixNewBlock(file);
  bacbStack.push(newPos);

  // Nummer der neuen Seite ermitteln
  BlockNo blockNo = newPos.getBlockNo();

  // So lange vom Ende der Seiten-list (von Nummer der neuen Seite)
  // runterzählen bis bei der aktuellen Position angelangt
  while (blockNo > pos) {
    --blockNo;

    // Left wird nach Right kopiert, um Platz zu schaffen
    DBBACB left = bufMgr.fixBlock(file, blockNo, LOCK_EXCLUSIVE);

    // Right vom Stack laden
    DBBACB right = bacbStack.top();

    // Dann den Inhalt der Seite Left in die Seite Right kopieren
    memcpy(right.getDataPtr(), left.getDataPtr(), DBFileBlock::getBlockSize());
    right.setModified();

    // unfix und entfernen vom Stack
    unfixNonRootBlock(right);

    // Neues oberstes Element ist left
    bacbStack.push(left); // entspricht right = left
  }

  // nachdem alle Seite von pos bis zum Ende (wo jetzt die neue Seite ist) eine Position weiter
  // geschoben wurden, kann die Seite an Stelle pos mit 0en überschrieben werden
  DBBACB newPage = bacbStack.top();
  memset(newPage.getDataPtr(), 0, DBFileBlock::getBlockSize());
  newPage.setModified();

  // wenn die pos Seite nicht die rootSeite ist, dann unlocken
  unfixNonRootBlock(newPage);

  // Prüfen, ob Anzahl gelockter Seiten korrekt ist
  if (bacbStack.size() != s) {
    throw DBIndexException("BACB Stack is invalid");
  }
}

/**
 * Löscht an Position pos eine leere Seite.
 * Verschiebt dabei alle darauf folgenden Seiten um eine Position nach links
 *
 * @param blockNoToRemove
 */
void DBSeqIndex::removeEmptyPage(BlockNo blockNoToRemove) {
  LOG4CXX_INFO(logger, "removeEmptyPage()");
  LOG4CXX_DEBUG(logger, "blockNoToRemove: " + TO_STR(blockNoToRemove));

  // Höhe des Stacks der gesperrten Seiten vor dem Löschen
  uint s = bacbStack.size();

  // Anzahl der Seiten
  uint blockCnt = bufMgr.getBlockCount(file);

  // wenn mehr als eine Seite in der Datei vorhanden
  if (blockCnt > 1) {
    // Die Seite EXCLUSIVE locken und auf den Stack packen
    fixNonRootBlock(blockNoToRemove, LOCK_EXCLUSIVE);

    // Alle nachfolgenden Seiten um eine Position verschieben
    for (BlockNo i = blockNoToRemove + 1; i < blockCnt; ++i) {

      // Alle Seiten eine Stelle nach links verschieben
      // Während der Operation locken
      DBBACB left = bacbStack.top();
      DBBACB right = bufMgr.fixBlock(file, i, LOCK_EXCLUSIVE);

      memcpy(left.getDataPtr(), right.getDataPtr(), DBFileBlock::getBlockSize());
      left.setModified();

      // unfix und entfernen vom Stack
      unfixNonRootBlock(left);

      // füge "right" zum Stack hinzu
      bacbStack.push(right); // entspricht left = right
    }

    // nach der Operation die zu löschende Seite wieder freigeben
    unfixNonRootBlock(bacbStack.top());

    // Gesamtseitenzahl verringern und so eine Seite am Ende "abschneiden"
    bufMgr.setBlockCnt(file, blockCnt - 1);

    // the last page is never emptied?
  }

  // die Anzahl der gesperrten Seiten muss gleich geblieben sein
  if (bacbStack.size() != s) {
    throw DBIndexException("BACB Stack is invalid");
  }
}

/**
 * Prüft, ob eine Seite Tupel enthalt.
 * Es wird der uint an der nullten Stelle eines Blocks derefernziert
 * @param ptr
 * @return
 */
bool DBSeqIndex::isEmpty(const char *ptr) {
  uint cnt = *(uint *) ptr;
  return (cnt == 0);
}

/**
 * Fügt createDBSeqIndex zur globalen factory method-map hinzu
 */
int DBSeqIndex::registerClass() {
  setClassForName("DBSeqIndex", createDBSeqIndex);
  return 0;
}

/**
 * Wird aufgerufen von HubDB::Types::getClassForName von DBTypes, um DBIndex zu erstellen
 * @param DBBufferMgr *: Buffermanager
 * @param DBFile *: Dateiobjekt
 * @param attrType: Attributtp
 * @param ModeType: READ, WRITE
 * @param bool: unique Indexattribut
 */
extern "C" void *createDBSeqIndex(int nArgs, va_list ap) {
  // Genau 5 Parameter
  if (nArgs != 5) {
    throw DBException("Invalid number of arguments");
  }
  DBBufferMgr *bufMgr = va_arg(ap, DBBufferMgr *);
  DBFile *file = va_arg(ap, DBFile *);
  enum AttrTypeEnum attrType = (enum AttrTypeEnum) va_arg(ap, int);
  ModType m = (ModType) va_arg(ap, int); // READ, WRITE
  bool unique = (bool) va_arg(ap, int);
  return new DBSeqIndex(*bufMgr, *file, attrType, m, unique);
}
