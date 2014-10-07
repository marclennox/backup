# encoding: utf-8
require 'backup/cloud_io/google_drive'

module Backup
  module Syncer
    module Cloud
      class GoogleDrive < Base
        class Error < Backup::Error; end

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

        ##
        # Creates a new instance of the storage object
        def initialize(syncer_id = nil)
          super

          @cache_path ||= '.cache'
          @chunk_size ||= 4 # MiB
        end

        private

        def cloud_io
          @cloud_io ||= CloudIO::GoogleDrive.new(
            :client_id     => client_id,
            :client_secret => client_secret,
            :cache_path    => cache_path,
            :max_retries   => max_retries,
            :retry_waitsec => retry_waitsec,
            # Syncer can not use multipart upload.
            :chunk_size    => chunk_size
          )
        end

        def get_remote_files(remote_base)
          hash = {}
          cloud_io.objects(remote_base).each do |object|
            relative_path = object.path.sub(remote_base + '/', '')
            hash[relative_path] = object.md5
          end
          hash
        end

      end
    end
  end

end
