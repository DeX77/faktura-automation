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


def auftragsliste(client_connection, verknuepfungsfeld)

  auftraege = client_connection.query(
    "select auftrag.* from JOURNAL as auftrag,
    JOURNALPOS as liste,
    ARTIKEL as artikel
    where auftrag.REC_ID = liste.JOURNAL_ID
    and liste.ARTNUM = artikel.ARTNUM
    and artikel.#{verknuepfungsfeld} IS NOT NULL
    and artikel.MENGE_AKT < liste.MENGE
    ")
  
  return auftraege

end

def postenliste(client_connection, auftrag)
  
  #Herraussuchen der entsprechenden Artikel aus JOURNALPOS
  liste = client_connection.query(
    "select * from JOURNALPOS where JOURNAL_ID = #{auftrag[:REC_ID]}
    ")
 
  return liste
  
end

def zusammengesetzer_artikel(client_connection, listen_artikel)
  zusammengesetzer_artikel = client_connection.query(
    "select * from ARTIKEL
  where ARTNUM = #{listen_artikel[:ARTNUM]}
  ")
  
  return zusammengesetzer_artikel
end

def stuecklisten_artikel(client_connection, zusammengesetzer_artikel, verknuepfungsfeld)
  
  stuecklisten_artikel = client_connection.query(
    "select * from ARTIKEL
    where ARTNUM = #{zusammengesetzer_artikel[verknuepfungsfeld.to_sym]}    
    ")      
      
   return stuecklisten_artikel
end

def insert_posten(client_connection, posten)
  
  #Neuer Posten soll JOURNAL_ID von neuem Auftrag haben  
  posten[:JOURNAL_ID] = "LAST_INSERT_ID()"
  
  #Datum für neuen Posten
  posten[:ERSTELLT] = "CURDATE()"
  posten[:RDATUM] = 
  
  insert_query ="insert into JOURNALPOS
    (#{posten.keys.join(',')})
    VALUES(#{posten.values.join(',')})
    "
   puts "insert_query: #{insert_query}" if DBConnection.flags.d?
   
  return client_connection.query(insert_query) unless DBConnection.flags.dr?
  
end

def copy_auftrag(client_connection, auftrag)
  
  #REC_ID ist primär Key!
  auftrag.delete :REC_ID
  
  auftrag.delete_if { |key, value| (value.nil? || value.to_s.empty? || (value == -1)  || (value == 0.0)) }
  
  insert_query ="insert into JOURNAL
    (#{auftrag.keys.join(',')})
    VALUES(#{auftrag.values.join(',')})
    "
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
    
    listen_artikel[key] = stuecklisten_artikel[key]    
    
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

puts "Anzahl zu bearbeitender Auftraege: #{auftraege.count}" if DBConnection.flags.d?


liste = postenliste(client, auftraege.first)

puts "Anzahl der zu bearbeitenden Posten im 1. Auftrag: #{liste.count}" if DBConnection.flags.d?

zusammengesetzer_artikel = zusammengesetzer_artikel(client, liste.first)

stuecklisten_artikel = stuecklisten_artikel(client, zusammengesetzer_artikel.first, DBConnection.flags.uf)

if DBConnection.flags.d? 
  puts "stuecklisten_artikel: " 
  pp stuecklisten_artikel.first
end

exchange_artikel = exchange_artikel(liste.first, stuecklisten_artikel.first)

if DBConnection.flags.d?
  puts "exchange_artikel: "
  pp exchange_artikel
end

copy_auftrag(client, auftraege.first)

insert_posten(client, exchange_artikel)

