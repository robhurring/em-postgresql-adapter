require 'evented-spec'

RSpec.configuration.include EventedSpec::SpecHelper, :type => :eventmachine
