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
      :host     => db.flags.H,
      :username => db.flags.u,
      :database => db.flags.db,
      :password => db.flags.p,
      :socket =>   db.flags.s
  )

#Symbolize keys
  client.query_options.merge!(:symbolize_keys => true)

  return client
end


def process_auftraege(client, auftraege)

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

    position = 0

    liste.each do |posten|

      zusammengesetzer_artikel = zusammengesetzer_artikel(client, posten)

      stuecklisten_artikel = stuecklisten_artikel(client, zusammengesetzer_artikel, DBConnection.flags.uf)

      #exchange_artikel = exchange_artikel(posten, stuecklisten_artikel)

      sl                   = stueckliste(client, stuecklisten_artikel)

      sl.each do |sl_posten|
        insert_posten_auftrag(client, sl_posten, neuer_auftrag, position)
        update_mengen(client, sl_posten)
        position +=1
      end


    end

    #Eventuelle Dateilinks mitkopieren
    #copy_file_link(client, auftrags_id, neuer_einkauf)
    copy_file_link(client, auftrags_id, neuer_auftrag)

    #Fuege Auftrag in die Liste der bearbeiteten Auftraege ein
    insert_auftrag_done(client, auftrags_id)

  end
end

def get_auftraege(client)

  #Datenbankanfrage nach zu bearbeitenden Auftraegen

  auftraege = auftragsliste(client, DBConnection.flags.uf).to_a

  while (auftraege.count > 0)
    process_auftraege(client, auftraege)
    auftraege = auftragsliste(client, DBConnection.flags.uf).to_a
  end

end


client = init_db_connection(DBConnection)

get_auftraege(client)


