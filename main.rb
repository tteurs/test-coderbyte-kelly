require 'test/unit'
require './background/jobs/process_uploaded_file'
require './models/list'
require 'tempfile'
require 'digest'

module Background
  module Jobs
    class ProcessFileTest < Test::Unit::TestCase

      def setup
        @list = List.create!(name: 'Foo', type: 'none', url_name: 'foo')
        @file = Tempfile.new('upload.csv')
        write_file
        @worker = Background::Jobs::ProcessUploadedFile.new(nil, list_id: @list.id, batch_size: 2)
        @worker.instance_variable_set '@path', @file.path
        def @worker.download_file!
          @path
        end
      end

      def teardown
        List.destroy_all
      end

      def write_file(separator="\n")
        File.open(@file, 'w+') do |csv|
          0.upto(99) do |i|
            value = "email#{i}@test.com"
            time = Time.now
            line = [value, time].compact.join(',')
            csv << "#{line}#{separator}"
          end
        end
      end

      def test_iterates_files_in_batches_of_correct_size
        @worker.instance_variable_set '@count', 0
        def @worker.insert_batch(*args)
          @count += 1
        end
        @worker.perform
        assert_equal 50, @worker.instance_variable_get('@count')
      end

      def test_job_parses_files_with_windows_line_endings
        assert_equal 0, @list.items.count
        write_file("\r")
        @worker.perform
        items = @list.items.to_a
        assert_equal 100, items.count
      end

      def test_skips_invalid_lines
        File.open(@file, 'w+') do |csv|
          csv << "bob@burgers.com\n"
          csv << "\"joe,\n"
          csv << "marie@burgers.com\n"
        end
        @worker.perform
        items = @list.items.to_a

        assert_equal 2, items.count
        assert_equal "bob@burgers.com", items[0][:value]
        assert_equal "marie@burgers.com", items[1][:value]
      end

      def test_handles_multiline_csv_values
        File.open(@file, 'w+') do |csv|
          csv << "bob@burgers.com\n"
          csv << "\"joe\n@burgers.com\"\n"
          csv << "marie@burgers.com\n"
        end
        @worker.perform
        items = @list.items.to_a

        assert_equal 3, items.count
        assert_equal "bob@burgers.com", items[0][:value]
        assert_equal "joe\n@burgers.com", items[1][:value]
        assert_equal "marie@burgers.com", items[2][:value]
      end

    end
  end
end
