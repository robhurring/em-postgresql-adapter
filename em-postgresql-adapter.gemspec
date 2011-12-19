$:.push File.expand_path("../lib", __FILE__)
require "em-postgresql-adapter/version"

Gem::Specification.new do |s|
  s.name     = "em-postgresql-adapter"
  s.version  = EmPostgresqlAdapter::VERSION
  s.date     = "2011-11-27"
  s.summary  = "PostgreSQL fiber-based ActiveRecord 3.1 connection adapter for Ruby 1.9"
  s.email    = "ruben@leftbee.net"
  s.homepage = "http://github.com/leftbee/em-postgresql-adapter"
  s.authors  = ["Ruben Nine", "Christopher J. Bottaro", "Bruce Chu", "Anton Orel", "Laust Rud Jacobsen"]
  s.files    = [
    "em-postgresql-adapter.gemspec",
    "lib/active_record/connection_adapters/em_postgresql_adapter.rb",
    "lib/em-postgresql-adapter/fibered_postgresql_connection.rb"
  ] + Dir['Rakefile',
    'README*',
    'LICENSE*'] & `git ls-files -z`.split("\0")
  s.add_dependency('pg', '>= 0.8.0')
  s.add_dependency('activerecord', '>= 3.1.0')
  s.add_dependency('eventmachine')
#  s.add_dependency('em-synchrony', '>= 1.x.x)
end
