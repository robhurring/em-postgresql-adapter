require 'bundler'
Bundler.setup

require 'active_record'
require 'em-postgresql-adapter'

Dir[File.join(File.dirname(__FILE__), "support/**/*.rb")].each {|f| require f }
