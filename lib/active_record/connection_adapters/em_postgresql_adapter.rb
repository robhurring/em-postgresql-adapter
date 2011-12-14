require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/postgresql_adapter'
require 'em-postgresql-adapter/fibered_postgresql_connection'

if ActiveRecord::VERSION::STRING < "3.1"
  raise "This version of em-postgresql-adapter requires ActiveRecord >= 3.1"
end

module ActiveRecord
  module ConnectionAdapters
    class EMPostgreSQLAdapter < ActiveRecord::ConnectionAdapters::PostgreSQLAdapter
      # Returns 'FiberedPostgreSQL' as adapter name for identification purposes.
      ADAPTER_NAME = 'EMPostgreSQL'

      def connect
        @connection = ::EM::DB::FiberedPostgresConnection.connect(*@connection_parameters[1..(@connection_parameters.length-1)])

        # Money type has a fixed precision of 10 in PostgreSQL 8.2 and below, and as of
        # PostgreSQL 8.3 it has a fixed precision of 19. PostgreSQLColumn.extract_precision
        # should know about this but can't detect it there, so deal with it here.
        PostgreSQLColumn.money_precision = (postgresql_version >= 80300) ? 19 : 10

        configure_connection
        @connection.use_fibers!
      end

      # Close then reopen the connection.
      def reconnect!
        disconnect!
        connect
      end
    end
  end # ConnectionAdapters

  class Base
    # Establishes a connection to the database that's used by all Active Record objects
    def self.em_postgresql_connection(config) # :nodoc:
      config = config.symbolize_keys
      host     = config[:host]
      port     = config[:port] || 5432
      username = config[:username].to_s
      password = config[:password].to_s
      size     = config[:connections] || 4

      if config.has_key?(:database)
        database = config[:database]
      else
        raise ArgumentError, "No database specified. Missing argument: database."
      end

      # The postgres drivers don't allow the creation of an unconnected PGconn object,
      # so just pass a nil connection object for the time being.
      ::ActiveRecord::ConnectionAdapters::EMPostgreSQLAdapter.new(nil, logger, [size, host, port, nil, nil, database, username, password], config)
    end
  end

end # ActiveRecord
