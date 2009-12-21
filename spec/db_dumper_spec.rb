require 'spec/spec_helper'

# These specs assume a mysql instance, with no root password, running
# on localhost. They will create and drop a database called 'testdb'
# multiple times, and create and recursively wipe a directory with
# the same name.

describe DbDumper::Database do

  before(:each) do
    @params = {
      :database => 'testdb',
      :user => 'dbuser',
      :password => 'dbpasswd'
    }
    `rmdir testdb 2>/dev/null`
    `mysql -u root -e "drop database if exists #{@params[:database]}"`
    `mysql -u root -e "create database #{@params[:database]}"`
    `mysql -u root -e "grant all on #{@params[:database]}.* to #{@params[:user]}@localhost identified by '#{@params[:password]}'"`
  end

  after(:each) do
    `rm -rf testdb 2>/dev/null`
  end

  it "should overwrite existing, whole-table archive files" do
    create_table @params[:database], 'foo'
    dbu = DbDumper::Database.new @params
    foo = DbDumper::Table.new(:name => 'foo')
    dbu.tables << foo

    `mkdir -p #{@params[:database]}`
    `touch #{@params[:database]}/foo.sql.gz`

    dbu.dump

    FileTest.file?('testdb/foo.sql.gz').should be_true
    FileTest.zero?('testdb/foo.sql.gz').should be_false
  end

  it "should not dump today's data" do
    create_daily_table @params[:database], 'foo'
    add_daily_data @params[:database], 'foo', Date.yesterday
    add_daily_data @params[:database], 'foo', Date.today

    dbu = DbDumper::Database.new @params
    foo = DbDumper::DailyTable.new(:name => 'foo')
    dbu.tables << foo
    dbu.dump

    file = "#{@params[:database]}/foo/foo.#{Date.yesterday.for_db}.sql.gz"
    FileTest.exists?(file).should be_true

    file = "#{@params[:database]}/foo/foo.#{Date.today.for_db}.sql.gz"
    FileTest.exists?(file).should be_false
  end

  it "should not dump daily archive files which already exist" do
    create_daily_table @params[:database], 'foo'

    yesterday = Date.yesterday
    older = Date.today - 2
    oldest = Date.today - 3

    [yesterday, older, oldest].map do |d|
      add_daily_data @params[:database], 'foo', d
    end

    dbu = DbDumper::Database.new @params
    foo = DbDumper::DailyTable.new(:name => 'foo', :start_date => oldest)
    dbu.tables << foo

    # pretend one day has already been dumped
    `mkdir -p #{@params[:database]}/foo`
    `touch #{@params[:database]}/foo/foo.#{older.for_db}.sql.gz`

    dbu.dump

    [yesterday, older, oldest].map do |d|
      file = "#{@params[:database]}/foo/foo.#{d.for_db}.sql.gz"
      FileTest.exists?(file).should be_true
    end

    # One file should not have been recreated - it was already there
    file = "#{@params[:database]}/foo/foo.#{older.for_db}.sql.gz"
    FileTest.zero?(file).should be_true
  end
  
  it "should not have drop/create table statements in daily dumps" do
    create_daily_table @params[:database], 'foo'
    add_daily_data @params[:database], 'foo', Date.yesterday
    dbu = DbDumper::Database.new @params
    foo = DbDumper::DailyTable.new(:name => 'foo')
    dbu.tables << foo
    dbu.dump
    contents = `zcat testdb/foo/foo.#{Date.yesterday.for_db}.sql.gz`
    contents.should_not match(/DROP TABLE IF EXISTS `foo`/)
    contents.should_not match(/CREATE TABLE `foo`/)
  end

  it "should create daily archive files since start date" do
    create_daily_table @params[:database], 'foo'

    yesterday = Date.yesterday
    older = Date.today - 2
    oldest = Date.today - 3

    [yesterday, older, oldest].map do |d|
      add_daily_data @params[:database], 'foo', d
    end

    dbu = DbDumper::Database.new @params
    foo = DbDumper::DailyTable.new(:name => 'foo', :start_date => oldest)
    dbu.tables << foo

    dbu.dump

    [yesterday, older, oldest].map do |d|
      FileTest.file?("testdb/foo/foo.#{d.for_db}.sql.gz").should be_true
    end
  end

  it "should create daily archive file" do
    create_daily_table @params[:database], 'foo'
    add_daily_data @params[:database], 'foo', Date.yesterday
    dbu = DbDumper::Database.new @params
    foo = DbDumper::DailyTable.new(:name => 'foo')
    dbu.tables << foo
    dbu.dump
    date = db_date(Date.yesterday)
    FileTest.file?("testdb/foo/foo.#{date}.sql.gz").should be_true
  end

  it "should (partially) dump daily tables" do
    dbu = DbDumper::Database.new @params
    one = DbDumper::DailyTable.new(:name => 'one')
    dbu.tables << one
    dbu.should_receive(:dump_daily_table).with(one)
    dbu.dump
  end

  it "should have drop & create table statements in whole table dumps" do
    create_table @params[:database], 'foo'
    dbu = DbDumper::Database.new @params.merge(:tables => [DbDumper::Table.new(:name => 'foo')])
    dbu.dump
    contents = `zcat testdb/foo.sql.gz`
    contents.should match(/DROP TABLE IF EXISTS `foo`/)
    contents.should match(/CREATE TABLE `foo`/)
  end

  it "should not dump tables that were not specified" do
    dbu = DbDumper::Database.new @params
    create_table @params[:database], 'foo'
    one = DbDumper::Table.new(:name => 'one')
    foo = DbDumper::Table.new(:name => 'foo')
    dbu.tables << one
    dbu.should_receive(:dump_whole_table).with(one)
    dbu.should_not_receive(:dump_whole_table).with(foo)
    dbu.dump
  end

  it "should create archive file for whole tables" do
    create_table @params[:database], 'foo'
    dbu = DbDumper::Database.new @params.merge(:tables => [DbDumper::Table.new(:name => 'foo')])
    dbu.dump
    FileTest.file?('testdb/foo.sql.gz').should be_true
  end

  it "should dump whole tables" do
    tbl = DbDumper::Table.new(:name => 'foo')
    dbu = DbDumper::Database.new @params.merge(:tables => [tbl])
    dbu.should_receive(:dump_whole_table).with(tbl)
    dbu.dump
  end

  it "should dump the entire database structure" do
    DbDumper::Database.new(@params).dump
    FileTest.file?('testdb/structure.sql').should be_true
  end


  it "should fail if unable to use directory" do
    `touch testdb`
    dbu = DbDumper::Database.new @params
    lambda { dbu.dump }.should raise_error
  end

  it "should create directory for database" do
    File.directory?('testdb').should be_false
    dbu = DbDumper::Database.new @params
    dbu.dump
    File.directory?('testdb').should be_true
  end

  it "should dump_tables" do
    dbu = DbDumper::Database.new @params
    dbu.should_receive(:dump_tables)
    dbu.dump
  end

  it "should dump" do
    dbu = DbDumper::Database.new @params
    dbu.should respond_to(:dump)
  end

  it "should take a database user" do
    dbu = DbDumper::Database.new(:user => 'dbuser')
    dbu.user.should == 'dbuser'
  end

  it "should take a database name" do
    dbu = DbDumper::Database.new(:database => 'mydb')
    dbu.database.should == 'mydb'
  end

  it "should instantiate" do
    DbDumper::Database.new({}).should be_kind_of(DbDumper::Database)
  end

end

def add_daily_data(database, table, date)
  # `mysql -u root #{database} -e "create table #{table} (#{definition})"`
  mysql_exec database, "insert into #{table} (day) values (#{db_date(date)})"
end

def db_date(date)
  date.strftime("%Y-%m-%d")
end

def create_table(database, table)
  create_tbl database, table, 'bar varchar(255)'
end

def create_daily_table(database, table)
  create_tbl database, table, 'day date'
end

def create_tbl(database, table, definition)
  mysql_exec database, "create table #{table} (#{definition})"
end

def mysql_exec(database, cmd)
  `mysql -u root #{database} -e "#{cmd}"`
end

