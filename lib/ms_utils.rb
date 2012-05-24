require "ms_utils_version"
require 'hpricot'
require 'pp'
require 'pry'
require 'builder'
require 'gooddata'
require 'logger'
require 'rainbow'
require 'hpricot'
require 'fastercsv'

module MsUtils
    
  def self.synchronize_datasets(files, options={})
    pid       = options[:pid]
    user      = options[:user]
    password  = options[:password]
    execute   = options[:pid]
    fail "Execute is set to true but no pid provided." if execute && pid.nil? 
    
    files.each do |file| 
      maql = generate_synchronization_maql(file)
      if execute 
        GoodData.logger = Logger.new(STDOUT)
        GoodData.connect user password, nil, :timeout => 0
        #GoodData.use pid          
        GoodData.post("https://secure.gooddata.com/gdc/md/#{pid}/ldm/manage", { 'manage' => { 'maql' => maql } })
      else 
        puts maql
      end
    end
  end

  def self.generate_synchronization_maql(file)
    doc = Hpricot.XML(File.read(file))
    "SYNCHRONIZE {dataset." + doc.at('schema > name').inner_html.downcase + "};"
  end

  def self.dim_to_date(dimension_id)
    timestamp = ((dimension_id - 25568) * 86400)
    puts Time.at(timestamp).utc.strftime("%F")
  end

  def self.fix_line_breaks!(file_name)
    fail "File #{file_name} does not exist" unless File.exists?(file_name)
    text = File.read(file_name)
    File.open(file_name, "w") {|file| file.write(text.gsub("\r", "\n")) }
  end

  def self.set_timestamps(timestamp, source_dir, output_dir)
    files = Dir::glob("#{source_dir}/*.csv")
      files.each do |file|
        FasterCSV.open("#{output_dir}/" + File.basename(file), "w") do |output|
          FasterCSV.foreach(file, :headers => true, :return_headers => true, :header_converters => :symbol) do |line|
            if line.header_row?
              output << line
            else
              fail "Column Timestamp doesn't exist in #{file}" if line[:timestamp].nil?se
              if Integer(line[:timestamp]) <= timestamp 
                line[:timestamp] = timestamp.to_s
                output << line 
              end
            end
          end
        end
      end
  end


end