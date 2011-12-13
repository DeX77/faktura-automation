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

module Kunde
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
        :KUN_NUM => :KUNNUM1,
        :KUN_ANREDE => :ANREDE,
        :KUN_NAME1 => :NAME1,
        :KUN_NAME2 => :NAME2,
        :KUN_NAME3 => :NAME3,
        :KUN_ABTEILUNG => :ABTEILUNG,
        :KUN_STRASSE => :STRASSE,
        :KUN_LAND => :LAND,
        :KUN_PLZ => :PLZ,
        :KUN_ORT => :ORT
    }

    kundenfelder.each do |feld_journal, feld_adresse|

      journal_eintrag[feld_journal] = kunde[feld_adresse]

    end

    return journal_eintrag
  end
end
