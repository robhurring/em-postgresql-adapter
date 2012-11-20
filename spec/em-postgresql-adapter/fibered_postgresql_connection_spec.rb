require 'spec_helper'

describe EM::DB::FiberedPostgresConnection, 'plain integration into active_record' do

  before :each do
    ActiveRecord::Base.establish_connection(database_connection_config)
  end

  it "is a sane database.yml file" do
    database_connection_config['adapter'].should == 'em_postgresql'
  end

  it "uses an identifiable driver" do
    ActiveRecord::Base.connection.should be_instance_of(ActiveRecord::ConnectionAdapters::EMPostgreSQLAdapter)
  end

  it "returns the correct adapter_name" do
    ActiveRecord::Base.connection.adapter_name.should == "EMPostgreSQL"
  end

  it "can connect outside of eventmachine" do
    result = ActiveRecord::Base.connection.execute('SELECT 42')
    result.values.should == [['42']]
  end

  context "no pool parameter in connection options" do
    let(:no_pool_size_options) { database_connection_config.merge('pool' => nil) }

    it "still connects" do
      ActiveRecord::Base.establish_connection(no_pool_size_options)
      ActiveRecord::Base.connection.should be_instance_of(ActiveRecord::ConnectionAdapters::EMPostgreSQLAdapter)
    end

    it "defaults to a connection pool size of 5" do
      ActiveRecord::Base.establish_connection(no_pool_size_options)

      Fiber.new {
        instance = EventMachine::Synchrony::ConnectionPool.new(:size => 0)
        EventMachine::Synchrony::ConnectionPool.
          should_receive(:new).
          with(:size => 5).
          and_return(instance)

        ActiveRecord::Base.connection
      }.resume
    end
  end
end

describe EM::DB::FiberedPostgresConnection, 'integration with active_record inside an event_machine context', :type => :eventmachine do
  SLEEP_TIME = 0.2

  before(:each) do
    ActiveRecord::Base.establish_connection(database_connection_config)
  end

  it "can establish a connection and execute a query while running inside eventmachine" do
    result = nil
    em {
      Fiber.new {
        result = ActiveRecord::Base.connection.execute('SELECT 42').values
        done
      }.resume
    }
    result.should == [['42']]
  end

  it "can execute multiple queries on parallel fibers concurrently" do
    results = []
    start = Time.now
    em {
      Fiber.new {
        results << ActiveRecord::Base.connection.execute("SELECT pg_sleep(#{SLEEP_TIME}), 42").values
        done if results.length == 2
      }.resume

      Fiber.new {
        results << ActiveRecord::Base.connection.execute("SELECT pg_sleep(#{SLEEP_TIME}), 43").values
        done if results.length == 2
      }.resume
    }
    finish = Time.now

    (finish - start).should be_within(SLEEP_TIME * 0.25).of(SLEEP_TIME)
  end


  it "can collect values from multiple queries on a single fiber" do
    results = []
    em {
      Fiber.new {
        results << ActiveRecord::Base.connection.execute("SELECT 42").values
        results << ActiveRecord::Base.connection.execute("SELECT 43").values
        done
      }.resume
    }
    results.flatten.should include('42')
    results.flatten.should include('43')
  end

  it "raises an error if not called inside a fiber" do
    expect {
      em {
        ActiveRecord::Base.connection.execute('SELECT 42')
        done
      }
    }.to raise_error(ActiveRecord::StatementInvalid, /FiberError/)
  end

end
