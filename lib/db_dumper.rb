# = DbDumper Module
#
# Module and classes to help dump a database without locking tables for 
# extended periods.
#
# The classes provide a simple wrapper around the mysqldump command, such that;
#
# * Each table is dumped individually.
# * Tables are dumped either whole or daily. 
#
# When dumped whole, the archive file created will include table drop and 
# create statements.
#
# When dumped daily, archive files are created for each day's worth of data
# in the table, and no drop/create statements are included in the archive 
# files. So, it should be safe to restore a day at a time without affecting
# the rest of the data in the table.
#
# NB: This is only suitable for tables where data is written/changed on a 
# single day, and then only read thereafter. If 'old' days might be changed
# on a later date, this class will not be smart enough to re-dump the changed
# days.
#
# Daily archives are created for each day from 'start_date' to yesterday, and 
# are only created where the corresponding daily archive file does not exist
# already.
#
# == Requires
#
# - Unix-like OS (uses pipes)
# - mysqldump and gzip in the PATH.
# 
# == Limitations
#
# - Hard-coded to use gzip 
# - Only works on localhost. 
#
# Changing both of those should be trivial.
#
# Author:: David Salgado
# Copyright:: Copyright (c) 2009 David Salgado
# License:: Distributes under the same terms as Ruby

require 'date'

class Date
  def self.yesterday
    today - 1
  end

  # Formats the date for use in SQL statements
  def for_db
    strftime("%Y-%m-%d")
  end
end

module DbDumper

  # Class to represent a table which should be dumped whole.
  class Table
    attr_reader :name

    # Constructor method for table objects. Takes a :name attribute.
    #
    #   tbl = DbDumper::Table.new :name => 'mytable'
    #
    def initialize(p = {})
      @name = p[:name]
    end
  end

  # Class to represent a table which should be dumped a day at a time.
  # 
  # Inherits from DbDumper::Table
  #
  class DailyTable < Table
    attr_reader :date_field, :start_date

    # Constructor method for daily-dumped table objects. 
    #
    #   tbl = DbDumper::Table.new(
    #     :name => 'mytable',
    #     :date_field => 'date',  # Defaults to 'day'
    #     :start_date => ...      # Date object. Defaults to yesterday
    #   )
    #
    def initialize(p = {})
      super

      @start_date = p[:start_date]
      @start_date ||= Date.yesterday

      @date_field = p[:date_field]
      @date_field ||= 'day'
    end
  end

  # Main class. Encapsulates database parameters and knows how to dump
  # tables.
  class Database
    attr_reader :database, :user, :password, :verbose
    attr_accessor :tables

    # Constructor
    #
    #   db = DbDumper::Database.new(
    #     :database => 'mydatabase',
    #     :user => 'dbuser',
    #     :password => 'dbpassword',
    #     :verbose => true   # Defaults to false. Causes commands to be printed out before executing.
    #
    #   )
    #
    # Tables to dump should be added to the 'tables' array;
    #
    # * db.tables << Table.new(:name => 'foo')
    # * db.tables << DailyTable.new(:name => 'foo', :start_date => Date.parse('2009-12-13'))
    #
    #
    def initialize(params)
      @database = params[:database]
      @user = params[:user]
      @password = params[:password]

      @tables = params[:tables]
      @tables ||= []

      @verbose = params[:verbose]
      @verbose ||= false
    end

    # Dumps database structure, whole table archives and daily table 
    # archives which do not exist already.
    # 
    # Structure and whole table archives are overwritten on every call.
    #
    # Directories and files will be created for database 'foo' such that;
    #
    # * foo/structure.sql  -- structure of the database
    # * foo/mytable.sql.gz  -- archive of the whole table 'mytable'
    # * foo/daily.2009-12-13.sql.gz  -- daily archive of table 'daily' for 2009/12/13
    # 
    def dump
      if valid?
        mkdir! database
        dump_structure
        dump_tables
      end
    end

    private

    def whole_tables
      tables.find_all {|t| !t.kind_of?(DailyTable)}
    end

    def daily_tables
      tables.find_all {|t| t.kind_of?(DailyTable)}
    end

    def dump_tables
      whole_tables.each {|t| dump_whole_table(t)}
      daily_tables.each {|t| dump_daily_table(t)}
    end

    def dump_daily_table(table)
      dir = "#{database}/#{table.name}"
      mkdir! dir
      (table.start_date .. Date.yesterday).each do |date| 
        output = "#{dir}/#{table.name}.#{date.for_db}"
        dump_daily_data(table, date, output) unless FileTest.file?(compressed(output))
      end
    end

    private

    def mkdir!(dir)
      execute "mkdir -p #{dir} 2>/dev/null"
      raise "Unable to use directory #{dir}" unless FileTest.directory?(dir)
    end

    def dump_daily_data(table, date, output)
      pre = %[--skip-add-drop-table --no-create-info --where="#{table.date_field}='#{date.for_db}'"]
      execute "#{mysqldump(:pre => pre, :post => table.name)} | #{compress output}"
    end

    def dump_whole_table(table)
      output = "#{database}/#{table.name}"
      execute "#{mysqldump(:post => table.name)} | #{compress output}"
    end

    def dump_structure
      execute "#{mysqldump(:pre => '--no-data')} > #{database}/structure.sql"
    end

    def valid?
      !!(@database && @user)  # Password might be absent
    end

    def execute(cmd)
      puts cmd if verbose
      `#{cmd}`
    end

    private

    def compress(filename)
      "gzip -c > #{compressed filename}"
    end

    def compressed(filename)
      "#{filename}.sql.gz"
    end

    def mysqldump(options = {})
      passwd = password.nil? ? '' : "--password=#{password}"
      "mysqldump --user=#{user} #{passwd} #{options[:pre]} #{database} #{options[:post]}"
    end

  end

end

