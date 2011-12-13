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
