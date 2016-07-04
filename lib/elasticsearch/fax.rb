require "elasticsearch/fax/version"

module Elasticsearch
  module Fax

    def self.reload!
      @configs = Hashie::Mash.new(YAML.load_file(Pathname.new(Dir.pwd).join('config/elasticsearch.yml')))
    end

    def self.port_offset
      @port_offset ||= rand(5000)
    end

    def self.copy_index(src_client, dst_client, src_index, dst_index)
      r = src_client.search(index: src_index, scroll: '5m', search_type: 'scan', size: 100, body: { query: { match_all: {} } })
      while r = src_client.scroll(scroll: '5m', scroll_id: r['_scroll_id']) and not r['hits']['hits'].empty? do
        dst_client.bulk(body: r['hits']['hits'].map do |hit|
          { index: { _index: dst_index, _type: hit['_type'], _id: hit['_id'], data: hit['_source'] } }
        end)
      end
      nil
    end

    def self.with_ssh_tunnel(gateway_user, gateway_host, local_port, remote_host, remote_port, &block)
      gateway = Net::SSH::Gateway.new(gateway_host, gateway_user)
      gateway.open(remote_host, remote_port, local_port)
      begin
        yield
      ensure
        gateway.shutdown!
      end
    end

    def self.sources
      @configs.keys.select { |i| @configs[i].server.present? && @configs[i].server_username.present? }
    end

    def self.destinations
      @configs.keys - sources
    end

    def self.copy(src_conf_name, dst_conf_name, index_base_name)
      src_conf, dst_conf = [ @configs[src_conf_name], @configs[dst_conf_name] ]
      src_uri            = URI.parse(src_conf.url)
      local_port         = src_uri.port + port_offset
      with_ssh_tunnel(src_conf.server_username, src_conf.server, local_port, src_uri.host, src_uri.port) do
        src_client = Elasticsearch::Client.new(url: "http://localhost:#{local_port}")
        dst_client = Elasticsearch::Client.new(url: dst_conf.url)
        copy_index(src_client, dst_client, "#{index_base_name}_#{src_conf.index_suffix}",  "#{index_base_name}_#{dst_conf.index_suffix}")
      end
    end

    def self.define_methods!
      reload!
      sources.each do |src|
        destinations.each do |dst|
          define_singleton_method("copy_from_#{src}_to_#{dst}") do |base_name|
            copy(src, dst, base_name)
          end
        end
      end
    end

  end
end
