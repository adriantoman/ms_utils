require "ms_utils_version"
require 'hpricot'
require 'pp'
require 'builder'
require 'gooddata'
require 'logger'
require 'rainbow'
require 'hpricot'
require 'fastercsv'
require 'es'
require 'date'
require 'pry'
require 'pathname'

module MsUtils
    
  def self.synchronize_datasets(files, options={})
    pid       = options[:pid]
    login     = options[:login]
    password  = options[:password]
    execute   = options[:execute]
    fail "Execute is set to true but no pid provided." if execute && pid.nil? 
    fail "Execute is set to true but no password or login is provided." if execute && (password.nil? || login.nil?)
    files.each do |file| 
      maql = generate_synchronization_maql(file)
      if execute 
        gooddata_login(login, password)
        #GoodData.use pid          
        GoodData.post("/gdc/md/#{pid}/ldm/manage", { 'manage' => { 'maql' => maql } })
      else 
        puts maql
      end
    end
  end
  
  def self.gooddata_login(login, password) 
    GoodData.logger = Logger.new(STDOUT)
    gd_server = "https://secure.gooddata.com"
    gd_webdav = "https://secure-di.gooddata.com"
    begin
      GoodData.connect login, password, gd_server, {
        :timeout       => 60,
        :webdav_server => gd_webdav
      }
    rescue RestClient::BadRequest => e
      fail "Login to GoodData Failed"
      exit 1
    end
  end

  def self.generate_synchronization_maql(file)
    doc = Hpricot.XML(File.read(file))
    "SYNCHRONIZE {dataset." + doc.at('schema > name').inner_html.downcase + "};"
  end

  def self.dim_to_date(dimension_id)
    timestamp = dim_to_timestamp(dimension_id)
    Time.at(timestamp).utc.strftime("%F")
  end

  def self.date_to_dim(date)
    date = Date.strptime(date, '%Y-%m-%d') unless date.class == Date
    timestamp = Time.utc(date.year, date.month, date.day).to_i
    timestamp_to_dim(timestamp)
  end
  
  def self.dim_to_timestamp(dimension_id)
    ((dimension_id - 25568) * 86400)
  end
  
  def self.timestamp_to_dim(timestamp)
    (timestamp/86400 + 25568)
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
              fail "Column Timestamp doesn't exist in #{file}" unless line.header?(:timestamp)
              output << line
            else
              if Integer(line[:timestamp]) <= timestamp 
                line[:timestamp] = timestamp.to_s
                output << line 
              end
            end
          end
        end
      end
  end
  
  def self.create_deleted(options={})
    es_dir          = options[:es_dir]
    fail "es_dir must be specified" if es_dir.nil?
    sf_dir          = options[:sf_dir]
    fail "sf_dir must be specified" if sf_dir.nil?
    descriptor_file = options[:descriptor_file]
    base_dir        = options[:base_dir]
    mapping         = options[:mapping]
    output_dir      = Pathname.new(options[:output_dir] || ".")
    timestamp       = options[:timestamp]
    es_name         = options[:es_name]
    pid             = options[:pid]
    gd_login        = options[:gd_login]
    gd_password     = options[:gd_password]
    sf_login        = options[:sf_login]
    sf_password     = options[:sf_password]
    
    sync = MsUtils::Synchronization.new()

    fail "You have to provide base_dir if you have no data in es_dir" if is_empty_dir?(es_dir) && base_dir.nil?
    fail "You have to provide path to descriptor_file if you have no data in sf_dir" if is_empty_dir?(sf_dir) && descriptor_file.nil?

    sync.get_es_ids(:password => gd_password, :login => gd_login, :es_name => es_name, :base_dir => base_dir, :output_dir => es_dir, :pid => pid) if is_empty_dir?(es_dir)
    sync.get_sf_ids(:password => sf_password, :login => sf_login, :output_dir => sf_dir, :descriptor => descriptor_file) if is_empty_dir?(sf_dir)

    left_in_sf = []
    left_in_es = []
    mapping, left_in_sf, left_in_es = sync.create_mapping(sf_dir, es_dir) if mapping.nil?
    if left_in_es.size > 0 || left_in_sf.size > 0
      puts "Mapping couldn't be created for all the files. Processed will be only files with corresponding names. Rename the rest of the files or run with explicit mapping provided.".color(:green)
      puts "Unmapped or not exact match in sf dir(will not be processed):".color(:red)
      pp left_in_sf
      puts "Unmapped or not exact match in es dir(will not be processed):".color(:red)
      pp left_in_es
    end

    puts "Using mapping".color(:red)
    pp mapping
    
    mapping.each do |map|
      output_file_name = output_dir + (File.basename(map[:file_es],".csv") + "_deleted.csv")
      sync.diff_files(map[:file_es], map[:file_sf], output_file_name, timestamp)
    end
  end
  
  def self.is_empty_dir?(dir, pattern="*.csv")
    dir = Pathname.new(dir).expand_path unless dir.class == Pathname
    files = Dir.glob(dir + pattern)
    
    files.size == 0
  end
  
  def self.reconstruct_deleted(options)
    mirgration = MsUtils::Migration.new()

    mirgration.process_file(options[:input], options[:output], options[:id], options[:snapshot])
  end
  
  class Migration
  
    def process_file(file, output_file, id_col, snapshot_col)
      id_col = id_col ? id_col.to_sym : :id
      snapshot_col = snapshot_col ? snapshot_col.to_sym : :snapshot
      previous_array = []
      actual_array = []
      actual_snapshot = nil
      FasterCSV.open(output_file, "w") do |csv|
        csv << ["timestamp", "Id", "IsDeleted"]
      end
      FasterCSV.foreach( file, :headers           => true,
                        :header_converters => :symbol,
                        :return_headers    => true) do |line|
        if line.header_row?
          fail "Column #{snapshot_col} or #{id_col} doesn't exist in #{file}" unless line.header?(snapshot_col) && line.header?(id_col)
        else  
          actual_array << line[id_col] if actual_snapshot.eql?(line[snapshot_col]) 
          if !actual_snapshot.eql?(line[snapshot_col]) 
        
            previous_array, actual_array = process_arrays(previous_array, actual_array, output_file, MsUtils::dim_to_timestamp(Integer(actual_snapshot))) 
            actual_snapshot = line[snapshot_col]
            actual_array << line[id_col]
          end
        end
      end
      process_arrays(previous_array, actual_array, output_file, MsUtils::dim_to_timestamp(Integer(actual_snapshot))) 
    end
    
    def process_arrays(older_array, newer_array, output_file, timestamp)
      if !older_array.empty? && !newer_array.empty? 
        diff = older_array - newer_array
        FasterCSV.open(output_file, "a") do |csv|
          diff.each do |id|
            csv << [timestamp.to_s, id, "true"]
          end
        end
      end
      return newer_array, []
    end
    
  end
  
  class Synchronization
  
    def get_es_ids(options={})
      password      = options[:password]
      login         = options[:login]
      es_name       = options[:es_name]
      base_dir      = options[:base_dir]
      entity_list   = options[:entity_list] || []
      output_dir    = Pathname.new(options[:output_dir] || ".").expand_path
      
      pid           = options[:pid]
      fail "pid must be specified" if pid.nil?
      
      timezone      = options[:timezone] || "UTC"
      
      output_deleted = options[:output_deleted] || false
      
      fail "base_dir must be specified" if base_dir.nil?
      
      entity_list = get_entity_list(base_dir)
      
      timeframe = output_deleted ? Es::Timeframe.parse("latest") : Es::Timeframe.parse({:to => 'tomorrow', :from => 'yesterday'})
      
      
      entity_list.each do |entity|
        next unless entity_list.include?(entity.name)
        spec = {}
        spec[:entity] = entity.name
        spec[:file] = (output_dir + "#{entity.name}.csv").to_s
        field = entity.fields.detect {|field| field.is_attribute? || field.is_fact? || field.is_date?}
        next if field.nil?
        spec[:fields] = [{:name => "Id", :type => "recordid"}]
        spec[:fields] << {:name => field.name, :type => field.type} unless output_deleted
        entity = Es::Entity.parse(spec)
        entity.timeframes = [timeframe]
        entity.timezone = timezone
        entity.fields << Es::SnapshotField.new("snapshot", "snapshot") unless output_deleted
        
        gooddata_login(login, password)
        
        entity.extract(pid, es_name)       
      end    
    end 
    
    def get_sf_ids(options={})
      password = options[:password]
      login = options[:login]
      obejct_list = options[:object_list] || []
      output_dir = Pathname.new(options[:output_dir] || ".").expand_path
      descriptor = options[:descriptor]
      soql_list = options[:soql_list] || get_SOQLs(descriptor)
      
      client = Salesforce::Client.new(login, password)
      
      soql_list.each do |soql| 
        next unless obejct_list.include?(soql[:soql].scan(/[Ff]rom\s(.*?)\s/).first.last)
        file_name = output_dir + soql[:file_name]
        FasterCSV.open(file_name.to_s, 'w') do |csv|
          csv << ["Id"]
          
          values = [:Id]
          
          query = "SELECT Id #{soql[:soql]}"
          puts query
          puts file_name
          #binding.pry
          client.query(query, {:values => values, :output => csv})
        end  
      end
    end
    
    # Scans descriptor file for all occurances of .soql and .file. Uses returns array of hashes containing SOQL starting with From leaving out date restriction and file name.
    # Params:
    # +desciptor_file+:: SFDC descriptor file to be parsed
    def get_SOQLs(descriptor_file)
      text = File.read(descriptor_file)
      pattern = /.soql\(".*?([fF]rom.*?)(?i:\sand\sCreatedDate|\sand\sSystemModstamp|\swhere\sCreatedDate|\swhere\sSystemModstamp).*?\.file\("(.*?).csv"\)/m
      matches = text.scan(pattern)
      (matches.map {|item| {:soql => (item.first).gsub(/\s+/, " "), :file_name => item.last + ".csv"}}).uniq!
    end  
      
    def diff_files(es_file, sf_file, diff_file, timestamp)
      ids = []
      i = 0
      FasterCSV.foreach(es_file, :headers  => true,
                        :header_converters => :symbol) do |line|
        ids << line[:id] unless line[:id].nil? || line[:id].empty?
        i = i+1
      end
      
      FasterCSV.foreach(sf_file, :headers  =>true,
                        :header_converters => :symbol) do |line|
        ids = ids - [line[:id]]
      end
      
      FasterCSV.open(diff_file, "w") do |csv|
        csv << ["Timestamp", "Id", "IsDeleted"]
        ids.each do |id|
          csv << [timestamp.to_s, id, "true"]
        end
      end    
    end
      
    def create_mapping(dir_sf, dir_es)
      files_sf = Dir.glob(dir_sf + "/*.csv").map {|item| File.basename(item)}
      sf_copy = Dir.glob(dir_sf + "/*.csv").map {|item| File.basename(item)}
      files_es = Dir.glob(dir_es + "/*.csv").map {|item| File.basename(item)}
      map = []
      
      files_sf.each do |file_sf|
        if files_es.include?(file_sf)
          map << {:file_sf => (Pathname.new(dir_sf).realpath + file_sf).to_s, :file_es => (Pathname.new(dir_es).realpath + file_sf).to_s}
          files_es.delete(file_sf)
          break if files_es.empty?
          sf_copy.delete(file_sf)
        end 
      end
      
      return map, sf_copy, files_es
    end
      
    def get_entity_list(base_dir)
      files = Dir.glob("#{base_dir}/gen_load*.json")
      entity_list = []
      
      entities = files.reduce([]) do |memo, filename|
        fail "File #{filename} cannot be found" unless File.exist?(filename)
        load_config = Es::Helpers.load_config(filename)
        load = Es::Load.parse(load_config)
        memo.concat(load.entities)
      end
      
      hyper_load = Es::Load.new(entities)
      entity_names = hyper_load.entities.map {|e| e.name}.uniq
      
      entity_names.each do |entity_name|
        entity_list << hyper_load.get_merged_entity_for(entity_name)
      end
      
      return entity_list
    end
  end
  
end