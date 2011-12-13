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

module Artikel

  def zusammengesetzer_artikel(client_connection, listen_artikel)
    query =
        "select * from ARTIKEL
  where ARTNUM = #{listen_artikel[:ARTNUM]}
        "
    puts "zusammengesetzer_artikel:"+query if DBConnection.flags.d?

    zusammengesetzer_artikel = client_connection.query(query)
    zusammengesetzer_artikel.first[:MENGE] = listen_artikel[:MENGE]

    puts "zusammengesetzer_artikel.first[:MENGE] = #{zusammengesetzer_artikel.first[:MENGE]}"  if DBConnection.flags.d?

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

      puts "stuecklisten_artikel.first[:MENGE]: = #{stuecklisten_artikel.first[:MENGE]}"  if DBConnection.flags.d?

      return stuecklisten_artikel.first

    end
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

  def stueckliste(client_connection, artikel)
    #puts "stueckliste Artikel"
    #pp artikel

    out = []

    query = "
  select * from ARTIKEL_STUECKLIST
  where REC_ID =
  (select REC_ID from ARTIKEL where ARTNUM=#{artikel[:ARTNUM]})
  AND ARTIKEL_ART= 'STL'
  "

    puts "stuecklisten query = #{query}"  if DBConnection.flags.d?

    articles = client_connection.query(query)

    articles.each do |art|
      query = "select * from ARTIKEL where REC_ID = #{art[:ART_ID]}"

      blah = client_connection.query(query).first
      blah[:MENGE] = artikel[:MENGE] * art[:MENGE]
      #puts "stueckliste blah"
      #pp blah
      puts "stueckliste blah[:MENGE] = #{blah[:MENGE]}"   if DBConnection.flags.d?
      out << blah
    end

    return out
  end

end
