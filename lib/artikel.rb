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
        "select art.*,me.BEZEICHNUNG as ME_EINHEIT
        from ARTIKEL as art join MENGENEINHEIT as me on art.ME_ID = me.REC_ID
        where ARTNUM = #{listen_artikel[:ARTNUM]}
        "
    puts "zusammengesetzer_artikel:"+query if DBConnection.flags.d?

    zusammengesetzer_artikel = client_connection.query(query)
    zusammengesetzer_artikel.first[:MENGE] = listen_artikel[:MENGE]

    puts "zusammengesetzer_artikel.first[:MENGE] = #{zusammengesetzer_artikel.first[:MENGE]}" if DBConnection.flags.d?

    return zusammengesetzer_artikel.first
  end

  def stuecklisten_artikel(client_connection, zusammengesetzer_artikel, verknuepfungsfeld)

    if zusammengesetzer_artikel[verknuepfungsfeld.to_sym].nil?
      return zusammengesetzer_artikel
    else
      query =
          "select art.*,me.BEZEICHNUNG as ME_EINHEIT
      from ARTIKEL as art join MENGENEINHEIT as me on art.ME_ID = me.REC_ID
      where ARTNUM = #{zusammengesetzer_artikel[verknuepfungsfeld.to_sym]}
          "

      puts "stueckliste: "+query if DBConnection.flags.d?

      stuecklisten_artikel = client_connection.query(query)

      #Menge mitschleifen!
      stuecklisten_artikel.first[:MENGE] = zusammengesetzer_artikel[:MENGE]

      puts "stuecklisten_artikel.first[:MENGE]: = #{stuecklisten_artikel.first[:MENGE]}" if DBConnection.flags.d?

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

    puts "stuecklisten query = #{query}" if DBConnection.flags.d?

    articles = client_connection.query(query)

    articles.each do |art|
      query = "
        select art.*,me.BEZEICHNUNG as ME_EINHEIT
        from ARTIKEL as art join MENGENEINHEIT as me on art.ME_ID = me.REC_ID
        where art.REC_ID = #{art[:ART_ID]}"

      blah = client_connection.query(query).first
      blah[:MENGE] = artikel[:MENGE] * art[:MENGE]
      out << blah
    end

    return out
  end

  def update_mengen(client_connection, artikel)

    insert_query = "
    insert into ARTIKEL_BDATEN
    (ARTIKEL_ID, QUELLE, JAHR, MONAT, SUM_MENGE)
    VALUES((select REC_ID from ARTIKEL where ARTNUM = #{artikel[:ARTNUM]}),
    28, 0, 0, #{artikel[:MENGE]})
    ON DUPLICATE KEY UPDATE SUM_MENGE=SUM_MENGE+#{artikel[:MENGE]}
    "

    puts "insert_query: #{insert_query}" if DBConnection.flags.d?

    return client_connection.query(insert_query) unless DBConnection.flags.dr?

  end

end
