require 'csv'

module Background
  module Jobs
    module ProcessFile

      DEFAULT_BATCH_SIZE = 1000.freeze

      attr_accessor :list, :options, :batch_size, :file_name, :line_count

      def initialize(*args)
        @args = args
        process_args(*args)
      end

      def self.queue
        :process_files
      end

      def each_batch
        count = 0
        batch = []
        file  = File.open(file_name, 'r')
        begin
          while (line = file.gets)
            batch << line.parse_csv
            count += 1
            if batch.size == batch_size || count == line_count
              begin
                yield batch
              ensure
                batch = []
              end
            end
          end
        ensure
          file.close
        end
      end

      private

      def process_args(_, options)
        @options = options
        @list_id = options[:list_id]
        @batch_size = options[:batch_size]
      end
    end
  end
end
