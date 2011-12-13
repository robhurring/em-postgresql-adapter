module DatabaseConfigHelper

  def database_connection_config
    YAML.load_file(File.expand_path('../../database.yml', __FILE__))['test']
  end

end

RSpec.configuration.include DatabaseConfigHelper
