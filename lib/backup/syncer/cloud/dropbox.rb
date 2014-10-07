# encoding: utf-8
require 'backup/cloud_io/dropbox'

module Backup
  module Syncer
    module Cloud
      class Dropbox < Base
        class Error < Backup::Error; end

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

        ##
        # Creates a new instance of the storage object
        def initialize(syncer_id = nil)
          super

          @cache_path  ||= '.cache'
          @access_type ||= :app_folder
          @chunk_size  ||= 4 # MiB

          check_configuration
        end

        protected

        def sync_directory(dir)
          remote_base = path.empty? ? File.basename(dir) :
                                      File.join(path, File.basename(dir))
          Logger.info "Gathering remote data for '#{ remote_base }'..."
          remote_files = get_remote_files(remote_base)
          Logger.info("Gathering local data for '#{ File.expand_path(dir) }'...")
          local_files = LocalFile.find(dir, excludes)
          relative_paths = (local_files.keys | remote_files.keys).sort
          # This needs to get moved into the generic base class somehow
          relative_paths = remote_files.keys.map{|p| p.downcase.sub("/#{remote_base.downcase}/", '')}.inject(local_files.keys) { |a, k|
            a << k unless a.map(&:downcase).include?(k.downcase)
            a
          }.sort_by { |a| a.downcase }
          if relative_paths.empty?
            Logger.info 'No local or remote files found'
          else
            Logger.info 'Syncing...'
            sync_block = Proc.new do |relative_path|
              local_file  = local_files[relative_path]
              remote_md5  = remote_files[relative_path]
              # This needs to get moved into the generic base class somehow
              remote_md5  = remote_files[relative_path.downcase]
              remote_path = File.join(remote_base, relative_path)
              sync_file(local_file, remote_path, remote_md5, remote_base)
            end

            if thread_count > 0
              sync_in_threads(relative_paths, sync_block)
            else
              relative_paths.each(&sync_block)
            end
          end
        end

        private

        def cloud_io
          @cloud_io ||= CloudIO::Dropbox.new(
            :api_key       => api_key,
            :api_secret    => api_secret,
            :cache_path    => cache_path,
            :access_type   => access_type,
            :max_retries   => max_retries,
            :retry_waitsec => retry_waitsec,
            # Syncer can not use multipart upload.
            :chunk_size    => chunk_size
          )
        end

        def get_remote_files(remote_base)
          hash = {}
          cloud_io.objects(remote_base).each do |object|
            relative_path = object.path.downcase.sub("/#{remote_base.downcase}/", '')
            hash[relative_path] = object.md5
          end
          hash
        end

        def check_configuration
          # Add Dropbox default excludes
          excludes.concat %w( **/.DS_Store )
        end
      end
    end
  end

end
