require 'mongo'
require 'erb'
require 'active_support'
require 'active_support/core_ext'

# TODO: Dry this class with logger class
module MongodbLogger
  class ServerConfig

    DEFAULT_COLLECTION_SIZE = 250.megabytes

    class << self

      def set_config(config_path)
        if File.file?(config_path)
          config_file = File.new(config_path)
          config = YAML.load(ERB.new(config_file.read).result)
        else
          raise "Config file not found"
        end

        # default to localhost
        @mongo_cfg = {
          'host' => 'localhost',
          'port' => 27017
        }.merge(config)

        # specify a collection or default to name based on environment
        @mongo_cfg["collection"] ||= "#{ Rails.env }_log"

        # configure mongo based hosts (replset) or host (standalone)
        if @mongo_cfg['hosts']
          replset  = Mongo::MongoReplicaSetClient.new(@mongo_cfg['hosts'].map { |h| "#{h}:#{@mongo_cfg['port']}" },
                                                     :refresh_mode => false,
                                                     :read         => :secondary_preferred)
          @mongodb = replset.db(@mongo_cfg["database"])
        else
          # if in dev or staging mode, it's ok to connect to a mongo slave server
          slave_ok  = Rails.env.development? || Rails.env.staging?
          @mongodb  = Mongo::MongoClient.new(@mongo_cfg["host"], @mongo_cfg["port"], :slave_ok => slave_ok).db(@mongo_cfg["database"])
        end

        # see if we need to authenticate
        @mongodb.authenticate(MONGO_CONFIG["username"], MONGO_CONFIG["password"]) unless MONGO_CONFIG["username"].nil?

        set_collection
      end

      def set_config_for_testing(config_path)
        set_config(config_path)
        create_collection unless @mongodb.collection_names.include?(@mongo_cfg["collection"])
        set_collection
      end

      def create_collection
        capsize = DEFAULT_COLLECTION_SIZE
        capsize = @mongo_cfg['capsize'].to_i if @mongo_cfg['capsize']
        @mongodb.create_collection(@mongo_cfg["collection"],
                                            {:capped => true, :size => capsize})
      end


      def set_collection
        @collection = @mongodb[@mongo_cfg["collection"]]
      end

      def get_config
        @mongo_cfg
      end

      def authenticated?
        @authenticated
      end

      def collection_name
        @mongo_cfg["collection"]
      end

      def db
        @mongodb
      end

      def collection
        @collection
      end
    end
  end
end
