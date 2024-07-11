require 'digest'

class List

  @@lists = [ ]

  attr_accessor :id, :name, :url_name, :type, :ttl, :items

  def initialize(name:, url_name:, type:, ttl: nil)
    @name = name
    @id = @@lists.length + 1
    @url_name = url_name
    @type = type.to_sym
    @ttl = ttl
    @items = []
    @rejected_items = []
    @count_by_date = Hash.new { |h, k| h[k] = 0 }
  end

  def self.create!(name:, url_name:, type:, ttl: nil)
    list = new(name: name, url_name: url_name, type: type, ttl: ttl)
    list.validate!
    @@lists << list
    list
  end

  def self.destroy_all
    @@lists.clear
  end

  def self.find(id)
    @@lists.find { |list| list.id == id }
  end

  def validate!
    raise ArgumentError, "Name can't be blank" if name.nil? || name.empty?
    raise ArgumentError, "Url name can't be blank" if url_name.nil? || url_name.empty?
    raise ArgumentError, "Url name must only contain alphanumeric characters and underscores" if url_name.match?(/[^\w_]/)
  end


  def add_items(values)
    values.each do |value|
      normalized_value = normalize(value[0])
      @items << { value: normalized_value, added_at: Time.now }
      set_count_for_today!
    end
  end

  def delete_items(values)
    deleted_count = 0
    values.each do |value|
      normalized_value = normalize(value)
      index = @items.index { |item| item[:value] == normalized_value }
      if index
        @items.delete_at(index)
        deleted_count += 1
      end
    end
    deleted_count
  end

  def member(item)
    normalized_value = normalize(item)
    item = @items.find { |i| i[:value] == normalized_value }
    return nil unless item

    { added_at: item[:added_at], value: Digest.hexencode(item[:value]) }
  end

  def member?(item)
    !!member(item)
  end
  alias include? member?

  def clear!
    @items.clear
    @rejected_items.clear
    @count_by_date.clear
  end

  def count
    @items.length
  end
  alias length count

  def fetch_and_clear_rejects!
    rejects = @rejected_items.dup
    @rejected_items.clear
    rejects
  end

  def fetch_reject_sample(max_count = 50)
    @rejected_items.sample(max_count)
  end

  def validate_and_normalize(values, normalizer)
    accepted, rejected = [], []

    values.each do |value|
      normalized_value = normalizer.normalize(value[0])
      normalized_time = Time.now.utc
      if normalized_value && normalized_time
        accepted << [normalized_value, normalized_time]
      else
        rejected << [value[0]]
      end
    end

    [accepted, rejected]
  end

  private

  def normalize(value)
    case type
    when :md5
      Digest::MD5.hexdigest(value)
    else
      value
    end
  end

  def set_count_for_today!
    today = Time.now.utc.strftime('%Y-%m-%d')
    @count_by_date[today] += 1
  end
end
