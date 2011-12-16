#    cao_faktura_automatio - Some ruby script to automate tasks for CAO Faktura
#    Copyright (C) 2011 Daniel Exner <dex@dragonslave.de>
#
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

module Auftrag
  def auftragsliste(client_connection, verknuepfungsfeld)

    auftraege = client_connection.query(
        "select auftrag.* from JOURNAL as auftrag
    join
    JOURNALPOS as liste on auftrag.REC_ID = liste.JOURNAL_ID
    join
    ARTIKEL as artikel on liste.ARTNUM = artikel.ARTNUM
    where auftrag.REC_ID NOT IN
    (SELECT REC_ID from JOURNAL_DONE)
    and auftrag.QUELLE != 1
    and artikel.#{verknuepfungsfeld} IS NOT NULL
    group by auftrag.REC_ID
    ")

    return auftraege

  end

  def auftrag_postenliste(client_connection, auftrag)

    #Herraussuchen der entsprechenden Artikel aus JOURNALPOS
    query = "select * from JOURNALPOS where JOURNAL_ID = #{auftrag[:REC_ID]}
    and (ARTIKELTYP = 'N' or ARTIKELTYP = 'S')
    "

    puts query if DBConnection.flags.d?


    liste = client_connection.query(query)


    return liste

  end

  def insert_auftrag_done(client_connection, auftrags_id)
    insert_query =
        "insert into JOURNAL_DONE (REC_ID)
    VALUES(#{auftrags_id})
    "

    return client_connection.query(insert_query) unless DBConnection.flags.dr?
  end

  def init_auftrag(client_connection, auftrag, vrenum)

    journal_felder = "
  TERM_ID
 MA_ID
 QUELLE
 QUELLE_SUB
 ADDR_ID
 ASP_ID
 LIEF_ADDR_ID
 PROJEKT_ID
 AGBNUM
 ATRNUM
 VRENUM
 VLSNUM
 VERSNR
 FOLGENR
 KM_STAND
 KFZ_ID
 VERTRETER_ID
 VERTRETER_ABR_ID
 GLOBRABATT
 AGBDATUM
 ADATUM
 RDATUM
 LDATUM
 KLASSE_ID
 TERMIN
 PR_EBENE
 LIEFART
 ZAHLART
 GEWICHT
 KOST_NETTO
 WERT_NETTO
 LOHN
 WARE
 TKOST
 ROHGEWINN
 MWST_0
 MWST_1
 MWST_2
 MWST_3
 NSUMME_0
 NSUMME_1
 NSUMME_2
 NSUMME_3
 NSUMME
 MSUMME_0
 MSUMME_1
 MSUMME_2
 MSUMME_3
 MSUMME
 BSUMME_0
 BSUMME_1
 BSUMME_2
 BSUMME_3
 BSUMME
 ATSUMME
 ATMSUMME
 PROVIS_WERT
 WAEHRUNG
 KURS
 GEGENKONTO
 SOLL_NTAGE
 SOLL_SKONTO
 SOLL_STAGE
 SOLL_RATEN
 SOLL_RATBETR
 SOLL_RATINTERVALL
 STADIUM
 POS_TA_ID
 ERSTELLT
 ERST_NAME
 KUN_NUM
 KUN_ANREDE
 KUN_NAME1
 KUN_NAME2
 KUN_NAME3
 KUN_ABTEILUNG
 KUN_STRASSE
 KUN_LAND
 KUN_PLZ
 KUN_ORT
 USR1
 USR2
 KOPFTEXT
 FUSSTEXT
 PROJEKT
 ORGNUM
 BEST_NAME
 BEST_CODE
 BEST_DATUM
 INFO
 TRACKINGCODE
 FREIGABE1_FLAG
 PRINT_FLAG
 BRUTTO_FLAG
 MWST_FREI_FLAG
 PROVIS_BERECHNET
 SHOP_ID
 SHOP_ORDERID
 SHOP_STATUS
 SHOP_CHANGE_FLAG
 AUSLAND_TYP
  "

    auftrag.delete_if { |key, value| !journal_felder.include? key.to_s }


    #Datum
    auftrag[:ADATUM]     = "CURDATE()"
    auftrag[:LDATUM]     = "CURDATE()"
    auftrag[:RDATUM]     = "NOW()"
    auftrag[:BEST_DATUM] = "CURDATE()"
    auftrag[:QUELLE]     ||= 8
    auftrag[:VRENUM]     = vrenum

    #auftrag[:TERMIN] = "CURDATE()"

    #Loesche leere Daten
    auftrag.delete_if { |key, value| (value.nil? || value.to_s.empty? || (value == -1) || (value == 0.0)) }

    insert_query ="insert into JOURNAL
    (#{auftrag.keys.join(',')})
    VALUES(#{value_join(auftrag.values)})
    "

    puts "insert_query neuer auftrag: #{insert_query}" if DBConnection.flags.d?

    return client_connection.query(insert_query) unless DBConnection.flags.dr?

  end

  def insert_posten_auftrag(client_connection, posten, neuer_auftrag, position)

    journal_pos_fields = "
    REC_ID
   QUELLE
   QUELLE_SUB
   QUELLE_SRC
   PROJEKT_ID
   JOURNAL_ID
   WARENGRUPPE
   ARTIKELTYP
   ARTIKEL_ID
   TOP_POS_ID
   ADDR_ID
   LTERMIN
   ATRNUM
   VRENUM
   POSITION
   VIEW_POS
   MATCHCODE
   ARTNUM
   BARCODE
   MENGE
   LAENGE
   BREITE
   HOEHE
   GROESSE
   DIMENSION
   GEWICHT
   ME_EINHEIT
   PR_EINHEIT
   VPE
   EK_PREIS
   CALC_FAKTOR
   EPREIS
   GPREIS
   E_RGEWINN
   G_RGEWINN
   RABATT
   RABATT2
   RABATT3
   E_RABATT_BETRAG
   G_RABATT_BETRAG
   STEUER_CODE
   ALTTEIL_PROZ
   ALTTEIL_STCODE
   PROVIS_PROZ
   PROVIS_WERT
   GEBUCHT
   GEGENKTO
   BEZEICHNUNG
   SN_FLAG
   ALTTEIL_FLAG
   BEZ_FEST_FLAG
   BRUTTO_FLAG
   NO_RABATT_FLAG
   APOS_FLAG
   "

    posten[:BEZEICHNUNG] = posten[:LANGNAME]
    posten[:POSITION]    = position

    posten.delete_if { |key, value| !journal_pos_fields.include? key.to_s }

    #Loesche leere Daten
    posten.delete_if { |key, value| (value.nil? || value.to_s.empty? || (value == -1) || (value == 0.0)) }

    #Neuer Posten soll JOURNAL_ID von neuer Bestellung haben
    posten[:JOURNAL_ID] = neuer_auftrag
    posten[:ARTIKEL_ID] = posten[:REC_ID]
    posten.delete :REC_ID

    insert_query ="insert into JOURNALPOS
      (#{posten.keys.join(',')})
      VALUES(#{value_join(posten.values)})
      "
    puts "insert_query neuer auftragsposten : #{insert_query}" if DBConnection.flags.d?

    return client_connection.query(insert_query) unless DBConnection.flags.dr?

  end


end
