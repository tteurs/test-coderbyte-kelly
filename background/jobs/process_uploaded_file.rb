require './background/jobs/process_file'
require './models/list'

module Background
  module Jobs
    class ProcessUploadedFile

      include ProcessFile

      DEFAULT_BATCH_SIZE = 1000.freeze

      attr_accessor :s3_key

      def self.queue
        :process_files
      end

      def perform

        options     = (self.options || {})
        @batch_size = options[:batch_size] || DEFAULT_BATCH_SIZE
        @s3_key     = options[:key]
        @list       = List.find(options[:list_id])
        unless @list
          raise "Could not find List #{list}"
        end

        @file_name  = download_file!
        @line_count = %x{wc -l < "#{@file_name}"}.to_i

        each_batch do |batch|
          insert_batch(batch)
        end
      end

      def insert_batch(batch)
        @list.add_items(batch)
      end

      def download_file!
        raise 'No S3 key available' unless s3_key
        s3   = Aws::S3::Client.new(retry_limit: 10)
        temp = Tempfile.new(File.basename(s3_key))
        File.open(temp, 'wb') do |file|
          options = {
            bucket:          "suppressionlist",
            key:             s3_key,
            response_target: file
          }
          s3.get_object(options)
        end
        temp.path
      end

    end
  end
end
