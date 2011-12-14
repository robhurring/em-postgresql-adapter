require 'fiber'
require 'eventmachine'
require 'pg'

module EM
  module DB
    # Patching our PGConn-based class to wrap async_exec (alias for async_query) calls into Ruby Fibers
    # ActiveRecord 3.1 calls PGConn#async_exec and also PGConn#send_query_prepared (the latter hasn't been patched here yet -- see below)
    class FiberedPostgresConnection < PGconn

      module Watcher
        def initialize(client, deferrable)
          @client = client
          @deferrable = deferrable
        end

        def notify_readable
          detach
          begin
            @client.consume_input while @client.is_busy
            @deferrable.succeed(@client.get_last_result)
          rescue Exception => e
            @deferrable.fail(e)
          end
        end
      end
      
      attr_accessor :use_fibers # decide whether fibers should be used

      def initialize(*args)
        self.use_fibers = false # default use_fibers to false
        super(*args)
      end
      
      def use_fibers! # convenience method
        self.use_fibers = true
      end
      
      def can_use_fibers?
        ::EM.reactor_running? && self.use_fibers
      end
      
      def send_query_using_fibers(sql, *opts)
        send_query(sql, *opts)
        deferrable = ::EM::DefaultDeferrable.new
        ::EM.watch(self.socket, Watcher, self, deferrable).notify_readable = true
        fiber = Fiber.current
        deferrable.callback do |result|
          fiber.resume(result)
        end
        deferrable.errback do |err|
          fiber.resume(err)
        end
        Fiber.yield.tap do |result|
          raise result if result.is_a?(Exception)
        end
      end

      def async_exec(sql, *opts)
        if can_use_fibers?
          send_query_using_fibers(sql, *opts)
        else
          super(sql, *opts)
        end
      end
      alias_method :async_query, :async_exec

      # TODO: Figure out whether patching PGConn#send_query_prepared will have a noticeable effect and implement accordingly
      # NOTE: ActiveRecord 3.1 calls PGConn#send_query_prepared from ActiveRecord::ConnectionAdapters::PostgreSQLAdapter#exec_cache.
      # def send_query_prepared(statement_name, *params)
      # end

    end #FiberedPostgresConnection
  end #DB
end #EM
