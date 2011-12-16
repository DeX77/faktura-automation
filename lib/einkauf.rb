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

module Einkauf

  def einkaufsliste(client_connection, verknuepfungsfeld)

    query ="select auftrag.* from EKBESTELL as auftrag,
  EKBESTELL_POS as liste,
  ARTIKEL as artikel
    where auftrag.REC_ID = liste.EKBESTELL_ID
    and liste.ARTNUM = artikel.ARTNUM
    and artikel.#{verknuepfungsfeld} IS NOT NULL
    and artikel.MENGE_AKT <= liste.MENGE
    group by auftrag.REC_ID
   "

    puts "Einkaufsliste: #{query}" if DBConnection.flags.d?

    return client_connection.query(query)

  end

  def einkauf_postenliste(client_connection, einkauf)

    #Herraussuchen der entsprechenden Artikel aus JOURNALPOS
    query = "select * from EKBESTELL_POS where EKBESTELL_ID = #{einkauf[:REC_ID]}
    and ARTIKELTYP!='F'
    "

    puts query if DBConnection.flags.d?


    liste = client_connection.query(query)


    return liste

  end


  def insert_posten_einkauf(client_connection, posten, neuer_einkauf)

    #REC_ID ist primär Key!
    #posten[:ARTIKEL_ID] = posten[:REC_ID]
    posten.delete :REC_ID

    #Loesche leere Daten
    posten.delete_if { |key, value| (value.nil? || value.to_s.empty? || (value == -1) || (value == 0.0)) }

    posten[:BEZEICHNUNG] = posten[:LANGNAME] if !posten[:LANGNAME].nil?

    bestell_pos_fields = "
 PREISANFRAGE BELEGNUM ADDR_ID LIEF_ADDR_ID PROJEKT_ID REC_ID POSITION VIEW_POS WARENGRUPPE ARTIKELTYP MENGE
 ARTIKEL_ID MATCHCODE ARTNUM BARCODE LAENGE BREITE HOEHE GROESSE DIMENSION GEWICHT ME_EINHEIT PR_EINHEIT VPE
 EPREIS GPREIS RABATT1 RABATT2 RABATT3 E_RABATT_BETRAG G_RABATT_BETRAG STEUER_CODE ALTTEIL_PROZ ALTTEIL_STCODE GEGENKTO
 BEZEICHNUNG ALTTEIL_FLAG BRUTTO_FLAG STADIUM
 "

    posten.delete_if { |key, value| !bestell_pos_fields.include? key.to_s }

    #Neuer Posten soll EKBESTELL_ID von neuer Bestellung haben
    posten[:EKBESTELL_ID] = neuer_einkau

    puts "posten[:MENGE]: #{posten[:MENGE]}" if DBConnection.flags.d?

    insert_query ="insert into EKBESTELL_POS
    (#{posten.keys.join(',')})
    VALUES(#{value_join(posten.values)})
    "
    puts "insert_query neuer einkaufsposten: #{insert_query}" if DBConnection.flags.d?

    return client_connection.query(insert_query) unless DBConnection.flags.dr?

  end


  def init_einkauf(client_connection, auftrag, vrenum)

    #Loesche leere Daten
    auftrag.delete_if { |key, value| (value.nil? || value.to_s.empty? || (value == -1) || (value == 0.0)) }

    #Loesche unbekannte Felder

    ekbestell_fields = "
 TERM_ID
 MA_ID
 PREISANFRAGE
 ADDR_ID
 ASP_ID
 LIEF_ADDR_ID
 PROJEKT_ID
 BELEGNUM
 BELEGDATUM
 TERMIN
 LIEFART
 ZAHLART
 GLOBRABATT
 GEWICHT
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
 WAEHRUNG
 KURS
 GEGENKONTO
 SOLL_STAGE
 SOLL_SKONTO
 SOLL_NTAGE
 STADIUM
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
 FREIGABE1_FLAG
 PRINT_FLAG
 BRUTTO_FLAG
 MWST_FREI_FLAG
 "

    auftrag.delete_if { |key, value| !ekbestell_fields.include? key.to_s }

    #auftrag[:ADATUM] = "CURDATE()"
    auftrag[:TERMIN]     = "CURDATE()"
    #auftrag[:RDATUM] = "NOW()"
    auftrag[:BEST_DATUM] = "CURDATE()"
    auftrag[:ORGNUM]     = vrenum

    insert_query ="insert into EKBESTELL
    (#{auftrag.keys.join(',')})
    VALUES(#{value_join(auftrag.values)})
    "

    #pp auftrag.keys if DBConnection.flags.d?

    puts "insert_query neuer einkauf: #{insert_query}" if DBConnection.flags.d?

    return client_connection.query(insert_query) unless DBConnection.flags.dr?

  end


  def init_einkauf_stueckliste(client, liste, neuer_einkauf)
    liste.each do |stueck|
      insert_posten_einkauf(client, stueck, neuer_einkauf)
    end
  end


end
