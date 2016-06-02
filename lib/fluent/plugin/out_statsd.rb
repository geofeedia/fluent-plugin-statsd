require 'statsd-ruby'

module Fluent
  class StatsdOutput < BufferedOutput
    Fluent::Plugin.register_output('statsd', self)

    config_param :flush_interval, :time, :default => 1
    config_param :host, :string, :default => '127.0.0.1'
    config_param :port, :string, :default => '8125'

    attr_reader :statsd

    def initialize
      super
    end

    def configure(conf)
      super
      @statsd = Statsd.new(host, port)
    end

    def start
      super
    end

    def shutdown
      super
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      chunk.msgpack_each do |tag, time, record|
        level = tag.split('.').last.downcase
        if level != info
          next # all our metrics are logged at an info level
        end

        # prep the placement tags
        tags = []
        %w(cloud hostname instanceid podname region zone).each do |tag|
          if record.has_key? "placement.#{tag}"
            tags << tag + '=' + record['placement.' + tag]
          end
        end
        tags = tags.join(',')
        if not tags.empty?
          tags = ',' + tags # prefix a comma for below
        end

        record.each_key do |field|

          # timer
          if field.match /took<(long|int)>$/
            @statsd.timing field.sub /<(long|int)>$/, '' + tags, record[field].to_i

          # counters
          elsif field.match /count<(long|int)>$/
            @statsd.increment field.sub /<(long|int)>$/, '' + tags, record[field].to_i
          elsif field.match /success<int>$/
            @statsd.increment field.sub /<int>$/, '' + tags, record[field].to_i
          elsif field.match /error<int>$/
            @statsd.increment field.sub /<int>$/, '' + tags, record[field].to_i
          
          end
        end

      end
    end

  end
end
