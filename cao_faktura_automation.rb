#!/usr/bin/env ruby
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

    
require 'rubygems'
require 'mysql2'
require 'pp'
require 'optiflag'


module DBConnection
  extend OptiFlagSet

  flag "db" do
    description "Database name for database connection. No default"
    long_form "database-name"
  end

  flag "kn" do
    description "KundenNr for new created items"
    long_form "kundennummer"
    value_matches ["KundenNr must be a number", /^\d+$/]
  end
  
  flag "s" do
    description "File for processed Orders"
    long_form "savefile"    
  end

  optional_flag "H" do
    description "Hostname for database connection. Defaults to localhost"
    default "localhost"
    long_form "hostname"
  end

  optional_flag "u" do
    description "Username for database connection. Defaults to root"
    default "root"
    long_form "username"
  end

  optional_flag "p" do
    description "Password for database connection. Defaults to empty"
    default ""
    long_form "password"
  end

  optional_flag "uf" do
    description "Name of the userfield for article connection. Defaults to USERFELD_01"
    default "USERFELD_01"
    long_form "userfield-name"
  end

  optional_switch_flag "dr" do
    description "Dryrun. Don't actually write into the db"
    long_form "dry-run"
  end

  optional_switch_flag "d" do
    description "Debug. Output some Debug infos."
    long_form "debug"
  end

  usage_flag "h", "help", "?"

  and_process!

end


def value_join(array)
  out = ""

  array.each do |x|

    #Stringbereinigung
    if x.is_a?(String) && !x.include?("()")
      out +="\'#{x}\'"
    else
      out += x.to_s
    end

    out +=","

  end

  out.chop!

  return out
end


def auftragsliste(client_connection, verknuepfungsfeld)

  auftraege = client_connection.query(
      "select auftrag.* from JOURNAL as auftrag,
    JOURNALPOS as liste,
    ARTIKEL as artikel
    where auftrag.REC_ID = liste.JOURNAL_ID
    and liste.ARTNUM = artikel.ARTNUM
    and artikel.#{verknuepfungsfeld} IS NOT NULL
    and artikel.MENGE_AKT <= liste.MENGE
    and auftrag.QUELLE != 1
    group by auftrag.REC_ID
    ")

  return auftraege

end

def einkaufsliste(client_connection, verknuepfungsfeld)
  
  query ="select * from EKBESTELL as auftrag,
  EKBESTELL_POS as liste,
  ARTIKEL as artikel
    where auftrag.REC_ID = liste.EKBESTELL_ID
    and liste.ARTNUM = artikel.ARTNUM
    and artikel.#{verknuepfungsfeld} IS NOT NULL
    and artikel.MENGE_AKT <= liste.MENGE
    group by auftrag.REC_ID
   "
   
   return client_connection.query(query)
   
end

def get_art_kunde(client_connection, kundennummer)

  query = "select * from ADRESSEN
  where KUNNUM1 = #{kundennummer}
  "

  puts query if DBConnection.flags.d?

  kunde = client_connection.query(query)

  return kunde
end

def exchange_kunde(kunde, journal_eintrag)
  
  kundenfelder = {
    :KUN_NUM => :KUNNUM1 ,
    :KUN_ANREDE => :ANREDE ,
    :KUN_NAME1 => :NAME1 ,
    :KUN_NAME2 => :NAME2 ,
    :KUN_NAME3 => :NAME3 ,
    :KUN_ABTEILUNG => :ABTEILUNG ,
    :KUN_STRASSE => :STRASSE ,
    :KUN_LAND => :LAND ,
    :KUN_PLZ => :PLZ ,
    :KUN_ORT  => :ORT
  }
  
  kundenfelder.each do |feld_journal, feld_adresse|
    
    journal_eintrag[feld_journal] = kunde[feld_adresse]
    
  end

  return journal_eintrag
end

def auftrag_postenliste(client_connection, auftrag)
  
  #Herraussuchen der entsprechenden Artikel aus JOURNALPOS
  query = "select * from JOURNALPOS where JOURNAL_ID = #{auftrag[:REC_ID]}
    and ARTIKELTYP!='F'
    "

  puts query if DBConnection.flags.d?


  liste = client_connection.query(query)


  return liste

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


def zusammengesetzer_artikel(client_connection, listen_artikel)
  query =
      "select * from ARTIKEL
  where ARTNUM = #{listen_artikel[:ARTNUM]}
      "
  puts "zusammengesetzer_artikel:"+query if DBConnection.flags.d?

  zusammengesetzer_artikel = client_connection.query(query)
  zusammengesetzer_artikel.first[:MENGE] = listen_artikel[:MENGE]
  
  puts "zusammengesetzer_artikel.first[:MENGE] = #{zusammengesetzer_artikel.first[:MENGE]}"

  return zusammengesetzer_artikel.first
end

def stuecklisten_artikel(client_connection, zusammengesetzer_artikel, verknuepfungsfeld)

  if zusammengesetzer_artikel[verknuepfungsfeld.to_sym].nil?
    return zusammengesetzer_artikel
  else
    query =
	"select * from ARTIKEL
      where ARTNUM = #{zusammengesetzer_artikel[verknuepfungsfeld.to_sym]}
	"

    puts "stueckliste: "+query if DBConnection.flags.d?

    stuecklisten_artikel = client_connection.query(query)
    
    #Menge mitschleifen!
    stuecklisten_artikel.first[:MENGE] = zusammengesetzer_artikel[:MENGE]
    
    puts "stuecklisten_artikel.first[:MENGE]: = #{stuecklisten_artikel.first[:MENGE]}"
    
    return stuecklisten_artikel.first
    
  end
end

def insert_posten_einkauf(client_connection, posten, neuer_einkauf)

  #REC_ID ist primär Key!
  #posten[:ARTIKEL_ID] = posten[:REC_ID]
  posten.delete :REC_ID

  #Loesche leere Daten
  posten.delete_if { |key, value| (value.nil? || value.to_s.empty? || (value == -1) || (value == 0.0)) }

  bestell_pos_fields = "
 PREISANFRAGE BELEGNUM ADDR_ID LIEF_ADDR_ID PROJEKT_ID REC_ID POSITION VIEW_POS WARENGRUPPE ARTIKELTYP MENGE
 ARTIKEL_ID MATCHCODE ARTNUM BARCODE LAENGE BREITE HOEHE GROESSE DIMENSION GEWICHT ME_EINHEIT PR_EINHEIT VPE
 EPREIS GPREIS RABATT1 RABATT2 RABATT3 E_RABATT_BETRAG G_RABATT_BETRAG STEUER_CODE ALTTEIL_PROZ ALTTEIL_STCODE GEGENKTO
 BEZEICHNUNG ALTTEIL_FLAG BRUTTO_FLAG STADIUM
 "

  posten.delete_if { |key, value| !bestell_pos_fields.include? key.to_s }

  #Neuer Posten soll EKBESTELL_ID von neuer Bestellung haben  
  posten[:EKBESTELL_ID] = neuer_einkauf

  #Mengenanpassung
  query = "select MENGE_AKT from ARTIKEL where ARTNUM =#{posten[:ARTNUM]}"
  
  puts "Mengenanpassung Query = #{query}" if DBConnection.flags.d? 
  #pp posten
  
  vorhandene_menge = client_connection.query(query).first[:MENGE_AKT]

  if vorhandene_menge < posten[:MENGE]
    posten[:MENGE] -= vorhandene_menge
  end

  puts "posten[:MENGE]: #{posten[:MENGE]}" if DBConnection.flags.d?

  insert_query ="insert into EKBESTELL_POS
    (#{posten.keys.join(',')})
    VALUES(#{value_join(posten.values)})
    "
  puts "insert_query neuer einkaufsposten: #{insert_query}" if DBConnection.flags.d?

  return client_connection.query(insert_query) unless DBConnection.flags.dr?

end

def insert_posten_auftrag(client_connection, posten, neuer_auftrag)

  journal_pos_fields = "
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
 
 posten.delete_if { |key, value| !journal_pos_fields.include? key.to_s }

  #Loesche leere Daten
  posten.delete_if { |key, value| (value.nil? || value.to_s.empty? || (value == -1) || (value == 0.0)) }

  #Neuer Posten soll JOURNAL_ID von neuer Bestellung haben  
  posten[:JOURNAL_ID] = neuer_auftrag

  #Mengenanpassung
  query = "select MENGE_AKT from ARTIKEL where ARTNUM =#{posten[:ARTNUM]}"

  puts "query: #{query}" if DBConnection.flags.d?
  #pp posten
  
  vorhandene_menge = client_connection.query(query).first[:MENGE_AKT]

  if vorhandene_menge < posten[:MENGE]
    posten[:MENGE] -= vorhandene_menge
  end

  puts "posten[:MENGE]: #{posten[:MENGE]}" if DBConnection.flags.d?

  insert_query ="insert into JOURNALPOS
    (#{posten.keys.join(',')})
    VALUES(#{value_join(posten.values)})
    "
  puts "insert_query neuer auftragsposten : #{insert_query}" if DBConnection.flags.d?

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
  auftrag[:TERMIN] = "CURDATE()"
  #auftrag[:RDATUM] = "NOW()"
  auftrag[:BEST_DATUM] = "CURDATE()"
  auftrag[:ORGNUM] = vrenum

  insert_query ="insert into EKBESTELL
    (#{auftrag.keys.join(',')})
    VALUES(#{value_join(auftrag.values)})
    "

  #pp auftrag.keys if DBConnection.flags.d?

  puts "insert_query neuer einkauf: #{insert_query}" if DBConnection.flags.d?

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
  
  auftrag.delete_if { |key,value| !journal_felder.include? key.to_s }
  
  
  #Datum
  auftrag[:ADATUM] = "CURDATE()"
  auftrag[:LDATUM] = "CURDATE()"
  auftrag[:RDATUM] = "NOW()"
  auftrag[:BEST_DATUM] = "CURDATE()"
  auftrag[:QUELLE]  ||= 8
  auftrag[:VRENUM] = vrenum
  
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


def exchange_artikel(listen_artikel, stuecklisten_artikel)


  listen_artikel.each do |key, value|

    #REC_ID ist primär Key!
    next if key == :REC_ID

    #keine leeren Felder
    next if stuecklisten_artikel[key].nil?

    #nicht den selben Wert
    next if value == stuecklisten_artikel[key]

    #Menge bleibt bestehen!
    next if [:MENGE].include? key

    puts "Aendere #{key} von #{value} in #{stuecklisten_artikel[key]}" if DBConnection.flags.d?

    listen_artikel[key] = stuecklisten_artikel[key] if stuecklisten_artikel.has_key? key

  end

  #Aendere die ARTIKEL_ID
  listen_artikel[:ARTIKEL_ID] = stuecklisten_artikel[:REC_ID]

  #Aendere die Beschreibung
  listen_artikel[:BEZEICHNUNG] = stuecklisten_artikel[:LANGNAME]

  return listen_artikel
end

def copy_file_link(client_connection, journal_item_from, journal_item_to)

  insert_query =
 "insert into LINK (MODUL_ID,PFAD,DATEI,BEMERKUNG,LAST_CHANGE,LAST_CHANGE_USER,OPEN_FLAG,OPEN_USER,OPEN_TIME,REC_ID)
 select MODUL_ID,PFAD,DATEI,BEMERKUNG,LAST_CHANGE,LAST_CHANGE_USER,OPEN_FLAG,OPEN_USER,OPEN_TIME, #{journal_item_to}
 from LINK
 where REC_ID=#{journal_item_from}
 "
    
 return client_connection.query(insert_query) unless DBConnection.flags.dr?
 
end

def backup_last_journal(client_connection)

  query = "select LAST_INSERT_ID()"

  puts query if DBConnection.flags.d?

  last_entry = client_connection.query(query).first

  if DBConnection.flags.d?
    puts "letzter eingefuegter Eintrag"
    #pp last_entry
  end

  return last_entry.values.last

end


def init_db_connection(db)

  client = Mysql2::Client.new(
      :host => db.flags.H,
      :username => db.flags.u,
      :database => db.flags.db,
      :password => db.flags.p
  )

#Symbolize keys
  client.query_options.merge!(:symbolize_keys => true)

  return client
end

def stueckliste(client_connection, artikel)
  #puts "stueckliste Artikel"
  #pp artikel
  
  out = []
  
  query = "
  select * from ARTIKEL_STUECKLIST
  where REC_ID =
  (select REC_ID from ARTIKEL where ARTNUM=#{artikel[:ARTNUM]})
  "
  
  puts "stuecklisten query = #{query}"
  
  articles = client_connection.query(query)
  
  articles.each do |art|
    query = "select * from ARTIKEL where REC_ID = #{art[:ART_ID]}"
    
    blah = client_connection.query(query).first
    blah[:MENGE] = artikel[:MENGE] * art[:MENGE]
    #puts "stueckliste blah"
    #pp blah
    puts "stueckliste blah[:MENGE] = #{blah[:MENGE]}"
    out << blah
  end
  
  return out
end

def init_einkauf_stueckliste(client, liste, neuer_einkauf)
  liste.each do |stueck|
    insert_posten_einkauf(client, stueck, neuer_einkauf)
  end
end

def process_einkauf(client)
    #Oeffne Datei mit bereits bearbeiteten Auftraegen
  save_file = File.new(DBConnection.flags.s, 'a+')
  
  bearbeitete_auftraege = save_file.read
  
  #Datenbankanfrage nach zu bearbeitenden Auftraegen
  auftraege = einkaufsliste(client, DBConnection.flags.uf).to_a
  
  puts "EK-Bestellung vorher: #{auftraege.count}"
  
  #Loesche bereits bearbeite raus
  auftraege.each do |auftrag| 
    auftraege.delete(auftrag) if bearbeitete_auftraege.include? auftrag[:REC_ID].to_s
  end
  
  puts "EK-Bestellung nachher: #{auftraege.count}"
  
  default_kunde = get_art_kunde(client, DBConnection.flags.kn).first

  puts "Anzahl zu bearbeitender EK-Bestellung: #{auftraege.count}" if DBConnection.flags.d?

  pp auftraege
  
  auftraege.each do |auftrag|

    liste = einkauf_postenliste(client, auftrag)

    auftrags_id = auftrag[:REC_ID]
    
    puts "Anzahl der zu bearbeitenden Posten im EK-Bestellung #{auftrag[:ORGNUM]} : #{liste.count}" if DBConnection.flags.d?

    if liste.count > 0
      selbst_auftrag = exchange_kunde(default_kunde, auftrag)
      
      #puts "selbst auftrag"
      #pp selbst_auftrag

      init_auftrag(client, selbst_auftrag, auftrag[:ORGNUM])
      
      neuer_auftrag = backup_last_journal(client)
      
      init_einkauf(client, selbst_auftrag, auftrag[:ORGNUM])

      neuer_einkauf = backup_last_journal(client)

      liste.each do |posten|
	
	

	zusammengesetzer_artikel = zusammengesetzer_artikel(client, posten)

	stuecklisten_artikel = stuecklisten_artikel(client, zusammengesetzer_artikel, DBConnection.flags.uf)

	exchange_artikel = exchange_artikel(posten, stuecklisten_artikel)

	insert_posten_auftrag(client, zusammengesetzer_artikel, neuer_auftrag) if !zusammengesetzer_artikel[:USERFIELD_01].nil?
	
	sl = stueckliste(client, stuecklisten_artikel)
	
	init_einkauf_stueckliste(client, sl, neuer_einkauf)
	
	
	
      end

      #Eventuelle Dateilinks mitkopieren
      #copy_file_link(client, auftrags_id, neuer_einkauf)
      copy_file_link(client, auftrags_id, neuer_auftrag)

      #Fuege Auftrag in die Liste der bearbeiteten Auftraege ein
    end
    
    save_file.puts auftrags_id.to_s
    
  end
end

def process_auftraege(client)
  
  #Oeffne Datei mit bereits bearbeiteten Auftraegen
  save_file = File.new(DBConnection.flags.s, 'a+')
  
  bearbeitete_auftraege = save_file.read
  
  #Datenbankanfrage nach zu bearbeitenden Auftraegen
  auftraege = auftragsliste(client, DBConnection.flags.uf).to_a
 
  
  puts "auftraege vorher: #{auftraege.count}"
  
  #Loesche bereits bearbeite raus
  auftraege.each do |auftrag| 
    auftraege.delete(auftrag) if bearbeitete_auftraege.include? auftrag[:REC_ID].to_s
  end
  
  puts "auftraege nachher: #{auftraege.count}"
  
  default_kunde = get_art_kunde(client, DBConnection.flags.kn).first

  puts "Anzahl zu bearbeitender Auftraege: #{auftraege.count}" if DBConnection.flags.d?

  auftraege.each do |auftrag|

    liste = auftrag_postenliste(client, auftrag)

    auftrags_id = auftrag[:REC_ID]
    
    vrenum = auftrag[:VRENUM]
    
    puts "Anzahl der zu bearbeitenden Posten im Auftrag #{auftrag[:VRENUM]} : #{liste.count}" if DBConnection.flags.d?

    selbst_auftrag = exchange_kunde(default_kunde, auftrag)
    
    #puts "selbst auftrag"
    #pp selbst_auftrag

    init_auftrag(client, selbst_auftrag, auftrag[:VRENUM])
    
    neuer_auftrag = backup_last_journal(client)
    
    init_einkauf(client, selbst_auftrag, auftrag[:VRENUM])

    neuer_einkauf = backup_last_journal(client)

    liste.each do |posten|
      
      

      zusammengesetzer_artikel = zusammengesetzer_artikel(client, posten)

      stuecklisten_artikel = stuecklisten_artikel(client, zusammengesetzer_artikel, DBConnection.flags.uf)
      

      exchange_artikel = exchange_artikel(posten, stuecklisten_artikel)

      insert_posten_auftrag(client, exchange_artikel, neuer_auftrag)
      
      sl = stueckliste(client, stuecklisten_artikel)
      
      init_einkauf_stueckliste(client, sl, neuer_einkauf)
      
      
      
    end

    #Eventuelle Dateilinks mitkopieren
    #copy_file_link(client, auftrags_id, neuer_einkauf)
    copy_file_link(client, auftrags_id, neuer_auftrag)

    #Fuege Auftrag in die Liste der bearbeiteten Auftraege ein
    
    save_file.puts auftrags_id.to_s
    
  end
end


client = init_db_connection(DBConnection)

process_auftraege(client)

puts "------------------------------------------------------------------------------------------------"

process_einkauf(client)



