#!/usr/bin/env ruby

require 'rubygems'
require 'mysql2'
require 'pp'
require 'optiflag'

module DBConnection extend OptiFlagSet
  
  flag "db" do
    description "Database name for database connection. No default"
    long_form "database-name"
  end
  
  flag "kn" do
    description "KundenNr for new created items"
    long_form "kundennummer"
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
  
  usage_flag "h","help","?"

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
    and artikel.MENGE_AKT < liste.MENGE
    and auftrag.QUELLE != 1
    ")
  
  return auftraege

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
  
  journal_eintrag.each do |key, value|
    journal_eintrag[key] = kunde[key] if kunde.has_key? key 
  end
  
  return journal_eintrag
end

def postenliste(client_connection, auftrag)
  
  #Herraussuchen der entsprechenden Artikel aus JOURNALPOS
  query = "select * from JOURNALPOS where JOURNAL_ID = #{auftrag[:REC_ID]}
    and ARTIKELTYP!='F'
    "
  
    puts query  if DBConnection.flags.d?
    
  
  liste = client_connection.query(query)
    
 
  return liste
  
end

def zusammengesetzer_artikel(client_connection, listen_artikel)
  query = 
    "select * from ARTIKEL
  where ARTNUM = #{listen_artikel[:ARTNUM]}
  "
  puts query if DBConnection.flags.d?
  
  zusammengesetzer_artikel = client_connection.query(query)
  
  return zusammengesetzer_artikel
end

def stuecklisten_artikel(client_connection, zusammengesetzer_artikel, verknuepfungsfeld)
  
  query = 
    "select * from ARTIKEL
    where ARTNUM = #{zusammengesetzer_artikel[verknuepfungsfeld.to_sym]}    
    "
    
  puts query if DBConnection.flags.d?
  
  stuecklisten_artikel = client_connection.query(query)      
      
   return stuecklisten_artikel
end

def insert_posten(client_connection, posten)
  
  #REC_ID ist primär Key!
  posten.delete :REC_ID
  
  #Loesche leere Daten
  posten.delete_if { |key, value| (value.nil? || value.to_s.empty? || (value == -1)  || (value == 0.0)) }
  
  bestell_pos_fields = "
 PREISANFRAGE BELEGNUM ADDR_ID LIEF_ADDR_ID PROJEKT_ID REC_ID POSITION VIEW_POS WARENGRUPPE ARTIKELTYP
 ARTIKEL_ID MATCHCODE ARTNUM BARCODE LAENGE BREITE HOEHE GROESSE DIMENSION GEWICHT ME_EINHEIT PR_EINHEIT VPE MENGE
 EPREIS GPREIS RABATT1 RABATT2 RABATT3 E_RABATT_BETRAG G_RABATT_BETRAG STEUER_CODE ALTTEIL_PROZ ALTTEIL_STCODE GEGENKTO
 BEZEICHNUNG ALTTEIL_FLAG BRUTTO_FLAG STADIUM
 "
 
 posten.delete_if { |key,value| !bestell_pos_fields.include? key.to_s }
  
  #Neuer Posten soll EKBESTELL_ID von neuer Bestellung haben  
  posten[:EKBESTELL_ID] = "LAST_INSERT_ID()"
  
  insert_query ="insert into EKBESTELL_POS
    (#{posten.keys.join(',')})
    VALUES(#{value_join(posten.values)})
    "
   puts "insert_query: #{insert_query}" if DBConnection.flags.d?
   
  return client_connection.query(insert_query) unless DBConnection.flags.dr?
  
end

def init_einkauf(client_connection, auftrag)
  
  #REC_ID ist primär Key!
  auftrag.delete :REC_ID
  
  #Loesche leere Daten
  auftrag.delete_if { |key, value| (value.nil? || value.to_s.empty? || (value == -1)  || (value == 0.0)) }
  
  #Loesche unbekannte Felder
  
  ekbestell_fields = "
  REC_ID         
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
 
  auftrag[:ERSTELLT] = "CURDATE()"
          
  insert_query ="insert into EKBESTELL
    (#{auftrag.keys.join(',')})
    VALUES(#{value_join(auftrag.values)})
    "
    
    pp auftrag.keys if DBConnection.flags.d?
    
   puts "insert_query: #{insert_query}" if DBConnection.flags.d?
   
  return client_connection.query(insert_query)  unless DBConnection.flags.dr?
   
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

client = Mysql2::Client.new(
  :host => DBConnection.flags.H,
  :username => DBConnection.flags.u,
  :database => DBConnection.flags.db,
  :password => DBConnection.flags.p  
)

#Symbolize keys
client.query_options.merge!(:symbolize_keys => true)

auftraege = auftragsliste(client, DBConnection.flags.uf)

default_kunde = get_art_kunde(client, DBConnection.flags.kn).first

puts "Anzahl zu bearbeitender Auftraege: #{auftraege.count}" if DBConnection.flags.d?

auftraege.each do |auftrag|
    liste = postenliste(client, auftrag)
    puts "Anzahl der zu bearbeitenden Posten im Auftrag #{auftrag[:VRENUM]} : #{liste.count}" if DBConnection.flags.d?
    
    liste.each do |posten|
      
      zusammengesetzer_artikel = zusammengesetzer_artikel(client, posten)
      
      stuecklisten_artikel = stuecklisten_artikel(client, zusammengesetzer_artikel.first, DBConnection.flags.uf)

      if DBConnection.flags.d? 
	puts "stuecklisten_artikel: " 
	pp stuecklisten_artikel.first
      end

      exchange_artikel = exchange_artikel(posten, stuecklisten_artikel.first)

      if DBConnection.flags.d?
	puts "exchange_artikel: "
	pp exchange_artikel
      end
      
      selbst_auftrag = exchange_kunde(default_kunde, auftrag)

      init_einkauf(client, selbst_auftrag)

      insert_posten(client, exchange_artikel)

    end
end








