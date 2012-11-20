require 'em-synchrony/activerecord'
require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/postgresql_adapter'
require 'em-postgresql-adapter/fibered_postgresql_connection'

if ActiveRecord::VERSION::STRING < "3.1"
  raise "This version of em-postgresql-adapter requires ActiveRecord >= 3.1"
end

module ActiveRecord
  module ConnectionAdapters
    class EMPostgreSQLAdapter < ActiveRecord::ConnectionAdapters::PostgreSQLAdapter

      def adapter_name
        'EMPostgreSQL'
      end

      class Client < ::EM::DB::FiberedPostgresConnection
        include EM::Synchrony::ActiveRecord::Client
      end

      class ConnectionPool < EM::Synchrony::ActiveRecord::ConnectionPool
        # via method_missing async_exec will be recognized as async method
        def async_exec(*args, &blk)
          execute(false) do |conn|
            conn.send(:try_query_using_fibers, *args, &blk)
          end
        end
        alias_method :async_query, :async_exec

        def prepare(*args, &blk)
          # Prepare statement across all the connection instances in the pool
          # NOTE: how much of a performance hit will this cause on large pools (i.e. > 200)?)
          [connection, @available, @pending].flatten.each do |conn|
            conn.send(:prepare, *args, &blk)
          end
        end
      end

      include EM::Synchrony::ActiveRecord::Adapter

      def connect
        @connection
      end
    end
  end # ConnectionAdapters

  class Base
    DEFAULT_POOL_SIZE = 5

    def self.clean_config!(config)
      if (config[:prepared_statements].kind_of? String)
        config[:prepared_statements] = config[:prepared_statements] == "true"
      end
    end

    # Establishes a connection to the database that's used by all Active Record objects
    def self.em_postgresql_connection(config) # :nodoc:
      config = config.symbolize_keys
      clean_config! config
      host     = config[:host]
      port     = config[:port] || 5432
      username = config[:username].to_s
      password = config[:password].to_s
      poolsize = config[:pool] ? config[:pool].to_i : DEFAULT_POOL_SIZE

      if config.has_key?(:database)
        database = config[:database]
      else
        raise ArgumentError, "No database specified. Missing argument: database."
      end
      adapter = ActiveRecord::ConnectionAdapters::EMPostgreSQLAdapter
      options = [host, port, nil, nil, database, username, password]

      client = adapter::ConnectionPool.new(size: poolsize) do
        conn = adapter::Client.connect(*options)

        if config[:encoding]
          conn.set_client_encoding(config[:encoding])
        end

        # If using Active Record's time zone support configure the connection to return
        # TIMESTAMP WITH ZONE types in UTC.
        if ActiveRecord::Base.default_timezone == :utc
          conn.exec("SET time zone 'UTC'")
        elsif @local_tz
          conn.exec("SET time zone '#{@local_tz}'")
        end

        conn.exec("SET client_min_messages TO '#{config[:min_messages]}'") if config[:min_messages]
        conn.exec("SET schema_search_path TO '#{config[:schema_search_path]}'") if config[:schema_order]

        # Use standard-conforming strings if available so we don't have to do the E'...' dance.
        conn.exec('SET standard_conforming_strings = on') rescue nil

        conn
      end

      # Money type has a fixed precision of 10 in PostgreSQL 8.2 and below, and as of
      # PostgreSQL 8.3 it has a fixed precision of 19. PostgreSQLColumn.extract_precision
      # should know about this but can't detect it there, so deal with it here.
      ActiveRecord::ConnectionAdapters::PostgreSQLColumn.money_precision = (client.server_version >= 80300) ? 19 : 10

      adapter.new(client, logger, options, config)
    end
  end

end # ActiveRecord
