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
require 'lib/db_connection'
require 'lib/einkauf'
require 'lib/artikel'
require 'lib/auftrag'
require 'lib/kunde'
require 'lib/link'

include Artikel
include Auftrag
include Einkauf
include Kunde
include Link

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


def backup_last_journal(client_connection)

  query = "select LAST_INSERT_ID()"

  puts query if DBConnection.flags.d?

  last_entry = client_connection.query(query).first

  if DBConnection.flags.d?
    puts "letzter eingefuegter Eintrag"
    pp last_entry
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


def process_auftraege(client)

  #Oeffne Datei mit bereits bearbeiteten Auftraegen
  save_file = File.new(DBConnection.flags.s, 'a+')

  bearbeitete_auftraege = save_file.read

  #Datenbankanfrage nach zu bearbeitenden Auftraegen
  auftraege = auftragsliste(client, DBConnection.flags.uf).to_a


  puts "auftraege vorher: #{auftraege.count}" if DBConnection.flags.d?

  #Loesche bereits bearbeite raus
  auftraege.delete_if { |auftrag| bearbeitete_auftraege.include? auftrag[:REC_ID].to_s }

  puts "auftraege nachher: #{auftraege.count}" if DBConnection.flags.d?

  default_kunde = get_art_kunde(client, DBConnection.flags.kn).first

  puts "Anzahl zu bearbeitender Auftraege: #{auftraege.count}" if DBConnection.flags.d?

  auftraege.each do |auftrag|

    liste = auftrag_postenliste(client, auftrag)

    auftrags_id = auftrag[:REC_ID]

    vrenum = auftrag[:VRENUM]

    puts "Anzahl der zu bearbeitenden Posten im Auftrag #{auftrags_id} : #{liste.count}" if DBConnection.flags.d?

    selbst_auftrag = exchange_kunde(default_kunde, auftrag)

    #puts "selbst auftrag"
    #pp selbst_auftrag

    init_auftrag(client, selbst_auftrag, auftrag[:VRENUM])

    neuer_auftrag = backup_last_journal(client)

    #init_einkauf(client, selbst_auftrag, auftrag[:VRENUM])

    #neuer_einkauf = backup_last_journal(client)

    liste.each do |posten|


      zusammengesetzer_artikel = zusammengesetzer_artikel(client, posten)

      stuecklisten_artikel = stuecklisten_artikel(client, zusammengesetzer_artikel, DBConnection.flags.uf)

      #exchange_artikel = exchange_artikel(posten, stuecklisten_artikel)

      sl = stueckliste(client, stuecklisten_artikel)

      if sl
        sl.each do |sl_posten|
          insert_posten_auftrag(client, sl_posten, neuer_auftrag)
          update_mengen(client, sl_posten)
        end
      else
        insert_posten_auftrag(client, posten, neuer_auftrag)
        update_mengen(client, posten)
      end


      #init_einkauf_stueckliste(client, sl, neuer_einkauf)


    end

    #Eventuelle Dateilinks mitkopieren
    #copy_file_link(client, auftrags_id, neuer_einkauf)
    copy_file_link(client, auftrags_id, neuer_auftrag)

    #Fuege Auftrag in die Liste der bearbeiteten Auftraege ein

    save_file.puts auftrags_id.to_s

  end
end


def process_einkauf(client)
  #Oeffne Datei mit bereits bearbeiteten Auftraegen
  save_file = File.new(DBConnection.flags.s, 'a+')

  bearbeitete_auftraege = save_file.read

  #Datenbankanfrage nach zu bearbeitenden Auftraegen
  auftraege = einkaufsliste(client, DBConnection.flags.uf).to_a

  puts "EK-Bestellung vorher: #{auftraege.count}" if DBConnection.flags.d?

  #Loesche bereits bearbeite raus
  auftraege.delete_if { |auftrag| bearbeitete_auftraege.include? auftrag[:REC_ID].to_s }

  puts "EK-Bestellung nachher: #{auftraege.count}" if DBConnection.flags.d?

  default_kunde = get_art_kunde(client, DBConnection.flags.kn).first

  puts "Anzahl zu bearbeitender EK-Bestellung: #{auftraege.count}" if DBConnection.flags.d?

  auftraege.each do |auftrag|

    liste = einkauf_postenliste(client, auftrag)

    auftrags_id = auftrag[:REC_ID]

    puts "Anzahl der zu bearbeitenden Posten im EK-Bestellung #{auftrags_id} : #{liste.count}" if DBConnection.flags.d?

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

        insert_posten_auftrag(client, exchange_artikel, neuer_auftrag)

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


client = init_db_connection(DBConnection)

process_auftraege(client)

puts "------------------------------------------------------------------------------------------------" if DBConnection.flags.d?

#process_einkauf(client)



