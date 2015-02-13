# encoding: utf-8
require 'google/api_client'
require 'backup/cloud_io/base'
require 'digest/sha1'
require 'fileutils'
require 'mime/types'

Faraday.default_adapter = :excon

module Backup
  module CloudIO
    class GoogleDrive < Base
      class Error < Backup::Error; end

      APPLICATION_NAME = 'Backup Rubygem'
      TAG_PROPERTY_VALUE = 'backup_gem'
      REMOTE_FOLDER_MUTEX=Mutex.new
      METADATA_CACHE_MUTEX = Mutex.new

      ##
      # Google Drive API credentials
      attr_accessor :client_id, :client_secret

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
      # Chunk size, specified in MiB, for the ChunkedUploader.
      attr_accessor :chunk_size

      def initialize(options = {})
        super

        @client_id = options[:client_id]
        @client_secret = options[:client_secret]
        @cache_path = options[:cache_path]
        @chunk_size = options[:chunk_size]
      end

      # The Syncer may call this method in multiple threads.
      # However, #objects is always called prior to multithreading.
      def upload(src, dest, options = {})
        backup_sync_tag = Digest::SHA1.hexdigest(options[:remote_base]) if options[:remote_base]
        existing_file_id = get_file_id(dest, backup_sync_tag)
        parent_id = get_folder_id(File.dirname(dest), backup_sync_tag) unless existing_file_id

        mimeType = MIME::Types.type_for(src).first.to_s || "*/*"
        media = Google::APIClient::UploadIO.new(src, mimeType, File.basename(dest))
        media.chunk_size = 1024**2 * chunk_size
        file_properties={
          'title' => File.basename(dest),
          'mimeType' => mimeType
        }
        file_properties.merge!('parents' => [{'id' => parent_id}]) if parent_id
        file_properties.merge!('properties' => [{ 'key' => backup_sync_tag, 'value' => TAG_PROPERTY_VALUE }]) if backup_sync_tag

        if existing_file_id
          with_retries("UPLOAD updated file '#{src}' to remote '#{dest}'") do
            file = drive.files.update.request_schema.new(file_properties)
            request = Google::APIClient::Request.new({
              api_method: drive.files.update,
              media: media,
              body_object: file,
              parameters: {
                fileId: existing_file_id,
                newRevision: false,
                uploadType: 'resumable',
                alt: 'json'
              }
            })
            result=nil
            result=connection.execute(request)
            while (upload=result.resumable_upload) && upload.resumable?
              result=connection.execute(upload)
            end
            result.data.to_hash
          end
        else
          with_retries("UPLOAD new file '#{src}' to remote '#{dest}'") do
            file = drive.files.insert.request_schema.new(file_properties)
            request = Google::APIClient::Request.new({
              api_method: drive.files.insert,
              media: media,
              body_object: file,
              parameters: {
                uploadType: 'resumable',
                alt: 'json'
              }
            })
            result=nil
            result=connection.execute(request)
            while (upload=result.resumable_upload) && upload.resumable?
              result=connection.execute(upload)
            end
            result.data.to_hash
          end
        end
      end

      ##
      # Delete an existing folder by path.
      def delete_folder(path)
        delete_item(get_folder_id(path))
      end

      ##
      # Delete an existing file by path.
      def delete_file(path)
        delete_item(get_file_id(path))
      end

      # Returns all objects in the folder with the given prefix.
      #
      def objects(prefix)
        objects = []
        prefix = prefix.chomp('/')
        folder_entries(prefix).each do |obj_data|
          objects << Object.new(self, obj_data)
        end

        objects
      end

      # Delete object(s) from the bucket.
      #
      # - Called by the Storage (with objects) and the Syncer (with keys)
      # - Missing objects will be ignored.
      def delete(objects_or_keys)
        Array(objects_or_keys).dup.map{|e| e.is_a?(Object) ? e.key : get_file_id(e)}.each do |key|
          delete_item(key)
        end
      end

      private

      ##
      # To authorize an application for access to a user's Google Drive,
      # an authorization code must be provided in exchange for an
      # access token and refresh token. The access token can be used
      # immediately for API access, and the refresh token stored to
      # request new access tokens in the future. To obtain the
      # authorization code, a user must visit a URL and grant access
      # to the application.
      def connection
        return @connection if @connection

        unless session = cached_session
          Logger.info "Creating a new session!"
          session = create_write_and_return_new_session!
        end

        # will raise an error if session not authorized
        @connection = session

      rescue => err
        raise Error.wrap(err, 'Authorization Failed')
      end

      def drive
        connection.nil? ? nil : connection.discovered_api('drive', 'v2')
      end

      ##
      # Attempt to load a cached session
      def cached_session
        session = nil
        if File.exist?(session_cache_file)
          begin
            # Read the cached session and fetch an access token
            refresh_token = File.open(session_cache_file, 'r') { |f| f.read }
            session = new_session
            session.authorization.refresh_token = refresh_token
            with_retries("FETCH access token") { session.authorization.fetch_access_token! }
            Logger.info "Successfully retrieved access token from cached session!"
          rescue => err
            session = nil
            Logger.warn Error.wrap(err, <<-EOS)
              Could not retrieve access token from cached session.
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

        # Initialize the session with the provided authorization code
        session = new_session

        # Send the user to get the authorization code
        template = Backup::Template.new(
          {:session => session, :cached_file => session_cache_file}
        )
        template.render("cloud/google_drive/authorization_url.erb")

        # Wait for user to enter the code and hit 'return' to continue
        Timeout::timeout(180) { session.authorization.code = STDIN.gets.chomp }

        # This will raise an error if the user did not
        # visit the authorization_url and grant access
        #
        # get the access token from the server
        # this will be stored with the session in the cache file
        with_retries("FETCH access token") { session.authorization.fetch_access_token! }

        # Render an informational message and cached the new session
        template.render("cloud/google_drive/authorized.erb")
        cache_session!(session)
        template.render("cloud/google_drive/cache_file_written.erb")

        session

      rescue => err
        raise Error.wrap(err, 'Could not create or authenticate a new session')
      end

      ##
      # Initialize and return a new API client session
      def new_session
        # Initialize the session object with secrets and urls
        session = Google::APIClient.new(
          application_name: APPLICATION_NAME,
          application_version: Backup::VERSION
        )
        session.authorization.client_id = client_id
        session.authorization.client_secret = client_secret
        session.authorization.scope = 'https://www.googleapis.com/auth/drive'
        session.authorization.redirect_uri = 'urn:ietf:wg:oauth:2.0:oob'
        session
      end

      ##
      # Return a file object for the session cache file
      def session_cache_file
        cache_file = File.join(cache_path.start_with?('/') ? cache_path : File.join(Config.root_path, cache_path), client_id + client_secret)
        FileUtils.mkdir_p(File.dirname(cache_file))
        cache_file
      end

      ##
      # Serializes and writes the Google Drive session to a cache file
      def cache_session!(session)
        File.open(session_cache_file, "w") do |cache_file|
          cache_file.write(session.authorization.refresh_token)
        end
      end

      def metadata_cache_file
        "#{session_cache_file}.yml"
      end

      ##
      # Stores the specified object's local cached metadata
      def store_metadata_cache!(metadata)
        METADATA_CACHE_MUTEX.synchronize do
          cache_key = metadata['path'].sub(/^\//, '').chomp('/')
          cache = YAML.load_file(metadata_cache_file) rescue nil
          attrs = ((cache ||= {})[cache_key] ||= {})
          attrs['id'] = metadata['id']
          cache[cache_key] = attrs
          cache = cache.keys.sort_by{|k| k.downcase}.inject({}) {|h,k| h[k] = cache[k]; h}
          File.open(metadata_cache_file, "w") {|f| f.write(cache.to_yaml)}
          metadata
        end
      end

      ##
      # Loads the specified file's local cached metadata
      def augment_metadata_from_cache(metadata)
        cache_key = metadata['path'].sub(/^\//, '').chomp('/')
        cache = YAML.load_file(metadata_cache_file) rescue nil
        attrs = ((cache ||= {})[cache_key] ||= {})
        metadata['id'] = attrs['id'] if attrs['id']
        metadata
      end

      ##
      # Deletes the specified file's local cached metadata
      def delete_metadata_cache(file_path)
        METADATA_CACHE_MUTEX.synchronize do
          cache_key = file_path.sub(/^\//, '').chomp('/')
          cache = YAML.load_file(metadata_cache_file) rescue nil
          (cache ||= {}).reject!{|k,v| k == cache_key}
          File.open(metadata_cache_file, "w") {|f| f.write(cache.to_yaml)}
          file_path
        end
      end

      ##
      # Get the metadata for an existing folder or create it if not in existence.
      def folder_entries(path)
        folder_id = get_folder_id(path)
        pageToken = nil
        all_items = []
        result = nil
        until result && pageToken.nil? do
          with_retries("Load all entries for path '#{path}'") do
            request = Google::APIClient::Request.new({
              api_method: drive.files.list,
              parameters: {
                'max_results' => 9999,
                'pageToken' => pageToken,
                'q' => <<-QUERY
                  trashed = false
                  AND
                  hidden = false
                  AND
                  (
                    '#{folder_id}' IN parents
                    OR
                    properties HAS {
                      key='#{Digest::SHA1.hexdigest(path)}'
                      AND
                      value='#{TAG_PROPERTY_VALUE}'
                      AND
                      visibility='PRIVATE'
                    }
                  )
                QUERY
              }
            })
            result = connection.execute(request)
            all_items += result.data.items.map(&:to_hash)
            pageToken = result.data['nextPageToken']
          end
        end

        build_folder_entries(folder_id, all_items.inject({}){|h,i| h[i['id']] = i; h})
      end

      def build_folder_entries(folder_id, all_items)
        # Select only those items that are direct descendents of the specified folder
        folder_items=[]
        all_items.values.each { |item| folder_items << item if item["parents"].count > 0 && item["parents"].first["id"] == folder_id }

        # Separate all those folder items into files and folders
        files=folder_items.select { |item| item["mimeType"] != "application/vnd.google-apps.folder" }
        folders=folder_items.select { |item| item["mimeType"] == "application/vnd.google-apps.folder" }

        # Construct the base path for this folder
        base_path = build_path(folder_id, all_items)

        # Iterate through each file and add a constructed file path key
        entries=[]
        files.each do |file|
          file['path'] = base_path + file["title"]
          entries << file
        end

        # Iterate through each folder and call this method recursively to catch all nested files
        folders.each do |folder|
          entries += build_folder_entries(folder["id"], all_items)
        end

        entries
      end

      ##
      # Creates the path by traversing up a given file parents until the root is reached
      def build_path(folder_id, entries)
        folder = entries[folder_id]
        return "" if folder.nil? || folder["parents"].count == 0
        build_path(folder["parents"].first["id"], entries) + "#{folder["title"]}/"
      end

      ##
      # Delete an existing folder or file by id
      def delete_item(id)
        with_retries("DELETE remote object '#{id}'") do
          request = Google::APIClient::Request.new({
            api_method: drive.files.trash,
            parameters: {
              'fileId' => id
            }
          })
          result = connection.execute(request)
          result.data.to_hash
        end
      end

      def get_folder_id(path, backup_sync_tag = nil)
        folder_id = nil
        path.split('/').reject{|e| e == ""}.each do |path_part|
          begin
            folder_id = find_folder(path_part, folder_id, backup_sync_tag)
            store_metadata_cache!('path' => path, 'id' => folder_id) if folder_id
          rescue => err
            raise Error.wrap(err, "Unable to get folder #{path}")
          end
        end
        folder_id
      end

      def get_file_id(path, backup_sync_tag = nil)
        file_id = folder_id = nil
        File.dirname(path).split('/').reject{|e| e == ""}.each do |path_part|
          begin
            folder_id = find_folder(path_part, folder_id, backup_sync_tag)
          rescue => err
            raise Error.wrap(err, "Unable to get file #{path}")
          end
        end
        with_retries("Retrieve file metadata for '#{path}'") do
          request = Google::APIClient::Request.new({
            api_method: drive.files.list,
            parameters: {
              'q' => <<-QUERY
                trashed = false
                AND
                hidden = false
                AND
                mimeType != 'application/vnd.google-apps.folder'
                AND
                title = '#{File.basename(path)}'
                AND
                '#{folder_id}' IN parents
              QUERY
            }
          })
          result = connection.execute(request)
          if result.data.items && result.data.items.length > 0
            # Google Drive allows files with the same title. Return only the first result
            file_id = result.data.items[0].id
            store_metadata_cache!('path' => path, 'id' => file_id)
          end
        end
        file_id
      end

      def find_folder(title, parent_id = 'root', backup_sync_tag = nil)
        parent_id = 'root' unless parent_id
        existing_folder = nil
        folder_id = nil

        REMOTE_FOLDER_MUTEX.synchronize do

          # Attempt to retrieve an existing folder
          with_retries("Retrieve folder metadata for '#{parent_id}' subfolder '#{title}'") do
            folder_attempts = 0
            while existing_folder.nil? && folder_attempts < 5
              request = Google::APIClient::Request.new({
                api_method: drive.files.list,
                parameters: {
                  'q' => <<-QUERY
                    trashed = false
                    AND
                    hidden = false
                    AND
                    mimeType = 'application/vnd.google-apps.folder'
                    AND
                    title = '#{title}'
                    AND
                    '#{parent_id}' IN parents
                  QUERY
                }
              })
              result = connection.execute(request)
              existing_folder = result.data.items[0] if result.data.items && result.data.items.length > 0
              folder_attempts += 1
            end
          end

          folder_id = existing_folder.id if existing_folder

          begin

            # Create a new folder or update an existing folder if we have a sync tag
            if backup_sync_tag
              folder_properties={
                'title' => title,
                'mimeType' => 'application/vnd.google-apps.folder',
                'parents' => [{ 'id' => parent_id }],
                'properties' => [{ 'key' => backup_sync_tag, 'value' => TAG_PROPERTY_VALUE }]
              }
              if existing_folder
                unless (p=existing_folder.to_hash['properties']) && p.any?{|h| h[backup_sync_tag] == TAG_PROPERTY_VALUE}
                  with_retries("Update existing subfolder '#{title}' in '#{parent_id}'") do
                    folder = drive.files.update.request_schema.new(folder_properties)
                    request = Google::APIClient::Request.new({
                      api_method: drive.files.update,
                      body_object: folder,
                      parameters: {
                        fileId: existing_folder.id,
                        alt: 'json'
                      }
                    })
                    result = connection.execute(request)
                  end
                end
              else
                with_retries("Create new subfolder '#{title}' in '#{parent_id}'") do
                  folder = drive.files.insert.request_schema.new(folder_properties)
                  request = Google::APIClient::Request.new({
                    api_method: drive.files.insert,
                    body_object: folder
                  })
                  result = connection.execute(request)
                  folder_id = result.data.id
                end
              end
            end
          rescue => err
            raise Error.wrap(err, "Unable to create or update folder #{title}")
          end
        end

        folder_id

      end

      class Object
        attr_reader :key, :md5, :path

        def initialize(cloud_io, data)
          @cloud_io = cloud_io
          @key = data['id']
          @md5 = data['md5Checksum']
          @path = data['path']
        end

      end

    end
  end
end
