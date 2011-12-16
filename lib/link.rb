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

module Link

  def copy_file_link(client_connection, journal_item_from, journal_item_to)

    insert_query =
        "insert into LINK (MODUL_ID,PFAD,DATEI,BEMERKUNG,LAST_CHANGE,LAST_CHANGE_USER,OPEN_FLAG,OPEN_USER,OPEN_TIME,REC_ID)
   select MODUL_ID,PFAD,DATEI,BEMERKUNG,LAST_CHANGE,LAST_CHANGE_USER,OPEN_FLAG,OPEN_USER,OPEN_TIME, #{journal_item_to}
   from LINK
   where REC_ID=#{journal_item_from}
        "

    return client_connection.query(insert_query) unless DBConnection.flags.dr?

  end

end
