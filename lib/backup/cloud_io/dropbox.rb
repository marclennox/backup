# encoding: utf-8
require 'dropbox_sdk'
require 'backup/cloud_io/base'
require 'fileutils'
require 'digest/sha1'
require 'yaml'

module Backup
  module CloudIO
    class Dropbox < Base
      class Error < Backup::Error;
      end

      MUTEX = Mutex.new
      METADATA_CACHE_MUTEX = Mutex.new
      MAX_FILE_SIZE = 1024**3 * 5 # 5 GiB
      MAX_MULTIPART_SIZE = 1024**4 * 5 # 5 TiB

      ##
      # Dropbox API credentials
      attr_accessor :api_key, :api_secret

      ##
      # Path to store cached authorized session.
      #
      # Relative paths will be expanded using Config.root_path,
      # which by default is ~/Backup unless --root-path was used
      # on the command line or set in config.rb.
      #
      # By default, +cache_path+ is '.cache', which would be
      # '~/Backup/.cache/' if using the default root_path.
      attr_accessor :cache_path

      ##
      # Dropbox Access Type
      # Valid values are:
      #   :app_folder (default)
      #   :dropbox (full access)
      attr_accessor :access_type

      ##
      # Chunk size, specified in MiB, for the ChunkedUploader.
      attr_accessor :chunk_size

      def initialize(options = {})
        super

        @api_key = options[:api_key]
        @api_secret = options[:api_secret]
        @cache_path = options[:cache_path]
        @access_type = options[:access_type]
        @chunk_size = options[:chunk_size]
      end

      # The Syncer may call this method in multiple threads.
      # However, #objects is always called prior to multithreading.
      def upload(src, dest, options = {})
        result={}
        File.open(src, 'r') do |file|
          with_retries("UPLOAD file '#{src}' to remote '#{dest}'") do
            # Only do chunked uploads for large enough files
            # This is to prevent an error on zero byte files plus it made sense
            if file.stat.size > (1024**2 * chunk_size)
              uploader = connection.get_chunked_uploader(file, file.stat.size)
              while uploader.offset < uploader.total_size
                uploader.upload(1024**2 * chunk_size)
              end
              result = uploader.finish(dest, true)
            else
              result = connection.put_file(dest, file, true, nil)
            end
          end
        end
        # Store the metadata cache with the local file md5
        store_metadata_cache!(result.merge!('md5' => Digest::MD5.file(src).hexdigest))
        result
      end

      # Returns all objects in the folder with the given prefix.
      #
      def objects(prefix)
        objects = []
        prefix = prefix.chomp('/') + '/'
        folder_entries(prefix).each do |obj_data|
          unless obj_data['is_dir']
            objects << Object.new(self, obj_data)
          end
        end

        objects
      end

      # Delete object(s) from the bucket.
      #
      # - Called by the Storage (with objects) and the Syncer (with keys)
      def delete(objects_or_keys)
        keys = Array(objects_or_keys).dup
        keys.map!(&:path) if keys.first.is_a?(Object)
        keys.each do |path|
          with_retries("DELETE remote object '#{path}'") do
            connection.file_delete(path) and delete_metadata_cache(path)
          end
        end
      end

      ##
      # Delete an existing folder.
      def delete_folder(path)
        connection.file_delete(path)
      end

      private

      ##
      # The initial connection to Dropbox will provide the user with an
      # authorization url. The user must open this URL and confirm that the
      # authorization successfully took place. If this is the case, then the
      # user hits 'enter' and the session will be properly established.
      # Immediately after establishing the session, the session will be
      # serialized and written to a cache file in +cache_path+.
      # The cached file will be used from that point on to re-establish a
      # connection with Dropbox at a later time. This allows the user to avoid
      # having to go to a new Dropbox URL to authorize over and over again.
      def connection
        return @connection if @connection

        unless session = cached_session
          Logger.info "Creating a new session!"
          session = create_write_and_return_new_session!
        end

        # will raise an error if session not authorized
        @connection = DropboxClient.new(session, access_type)

      rescue => err
        raise Error.wrap(err, 'Authorization Failed')
      end

      ##
      # Attempt to load a cached session
      def cached_session
        session = nil
        if File.exist?(session_cache_file)
          begin
            session = DropboxSession.deserialize(File.read(session_cache_file))
            Logger.info "Session data loaded from cache!"
          rescue => err
            session = nil
            Logger.warn Error.wrap(err, <<-EOS)
              Could not read session data from cache.
              Cache data might be corrupt.
            EOS
          end
        end
        session
      end

      ##
      # Create a new session, write a serialized version of it to the
      # .cache directory, and return the session object
      def create_write_and_return_new_session!
        require 'timeout'

        session = DropboxSession.new(api_key, api_secret)

        # grab the request token for session
        session.get_request_token

        template = Backup::Template.new(
          {:session => session, :cached_file => session_cache_file}
        )
        template.render("cloud/dropbox/authorization_url.erb")

        # wait for user to hit 'return' to continue
        Timeout::timeout(180) { STDIN.gets }

        # this will raise an error if the user did not
        # visit the authorization_url and grant access
        #
        # get the access token from the server
        # this will be stored with the session in the cache file
        with_retries("GET access token") { session.get_access_token }

        template.render("cloud/dropbox/authorized.erb")
        cache_session!(session)
        template.render("cloud/dropbox/cache_file_written.erb")

        session

      rescue => err
        raise Error.wrap(err, 'Could not create or authenticate a new session')
      end

      def session_cache_file
        cache_file = File.join(cache_path.start_with?('/') ? cache_path : File.join(Config.root_path, cache_path), "#{api_key}#{api_secret}")
        FileUtils.mkdir_p(File.dirname(cache_file))
        cache_file
      end

      ##
      # Serializes and writes the Dropbox session to a cache file
      def cache_session!(session)
        File.open(session_cache_file, "w") do |cache_file|
          cache_file.write(session.serialize)
        end
      end

      def metadata_cache_file
        "#{session_cache_file}.yml"
      end

      ##
      # Stores the specified file's local cached metadata
      def store_metadata_cache!(metadata)
        METADATA_CACHE_MUTEX.synchronize do
          cache_key = metadata['path'].sub(/^\//, '').chomp('/').downcase
          cache = YAML.load_file(metadata_cache_file) rescue nil
          # Get the file revision list without the revision in question
          revisions = ((cache ||= {})[cache_key] ||= []).reject{|e| e['rev'] == metadata['rev']}
          revisions.unshift('rev' => metadata['rev'], 'md5' => metadata['md5'])
          cache[cache_key] = revisions
          cache = cache.keys.sort.inject({}) {|h,k| h[k] = cache[k]; h}
          File.open(metadata_cache_file, "w") {|f| f.write(cache.to_yaml)}
          metadata
        end
      end

      ##
      # Loads the specified file's local cached metadata
      def augment_metadata_from_cache(metadata)
        cache_key = metadata['path'].sub(/^\//, '').chomp('/').downcase
        cache = YAML.load_file(metadata_cache_file) rescue nil
        revisions = ((cache ||= {})[cache_key] ||= [])
        rev_cache = revisions.find{|r| r['rev'] == metadata['rev']} || {}
        metadata['md5'] = rev_cache['md5']
        metadata
      end

      ##
      # Deletes the specified file's local cached metadata
      def delete_metadata_cache(file_path)
        METADATA_CACHE_MUTEX.synchronize do
          cache_key = file_path.sub(/^\//, '').chomp('/').downcase
          cache = YAML.load_file(metadata_cache_file) rescue nil
          (cache ||= {}).reject!{|k,v| k == cache_key}
          File.open(metadata_cache_file, "w") {|f| f.write(cache.to_yaml)}
          file_path
        end
      end

      ##
      # Get the metadata for an existing folder or create it if not in existence.
      def folder_entries(path)
        begin
          connection.metadata(path)
        rescue ::DropboxError
          # Folder doesn't exist
          connection.file_create_folder(path)
        end
        entries = []
        result = nil
        cursor = nil
        while result.nil? || result['has_more']
          result = connection.delta(cursor, "/#{path}".chomp('/'))
          cursor = result['cursor']
          cursor_entries=result['entries'].select{|e| !e[1]['is_deleted']}.map{|e| augment_metadata_from_cache(e[1])}
          entries.concat cursor_entries
        end
        entries
      end

      class Object
        attr_reader :key, :md5, :path

        def initialize(cloud_io, data)
          @cloud_io = cloud_io
          @key = data['path'].downcase
          @md5 = data['md5']
          @path = data['path']
        end

      end

    end
  end
end
