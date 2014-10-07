# encoding: utf-8
require 'backup/cloud_io/google_drive'

module Backup
  module Storage
    class GoogleDrive < Base
      include Storage::Cycler
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
      # Number of times to retry failed operations.
      #
      # Default: 10
      attr_accessor :max_retries

      ##
      # Time in seconds to pause before each retry.
      #
      # Default: 30
      attr_accessor :retry_waitsec

      ##
      # Creates a new instance of the storage object
      def initialize(model, storage_id = nil)
        super

        @path           ||= 'backups'
        @cache_path     ||= '.cache'
        @chunk_size     ||= 4 # MiB
        @max_retries    ||= 10
        @retry_waitsec  ||= 30
        path.sub!(/^\//, '')
      end

      private

      def cloud_io
        @cloud_io ||= CloudIO::GoogleDrive.new(
          :client_id     => client_id,
          :client_secret => client_secret,
          :cache_path    => cache_path,
          :max_retries   => max_retries,
          :retry_waitsec => retry_waitsec,
          :chunk_size    => chunk_size
        )
      end

      ##
      # Transfer each of the package files to Google Drive in chunks of +chunk_size+.
      # Each chunk will be retried +chunk_retries+ times, pausing +retry_waitsec+
      # between retries, if errors occur.
      def transfer!
        # Upload each package
        package.filenames.each do |filename|
          src = File.join(Config.tmp_path, filename)
          dest = File.join(remote_path, filename)
          Logger.info "Storing '#{ dest }'..."
          # Upload the file
          cloud_io.upload(src, dest, remote_base: remote_path_for(package))
        end
      end

      # Called by the Cycler.
      # Any error raised will be logged as a warning.
      def remove!(package)
        Logger.info "Removing backup package dated #{ package.time }..."

        remote_path = remote_path_for(package)
        objects = cloud_io.objects(remote_path)

        raise Error, "Package at '#{ remote_path }' not found" if objects.empty?
        cloud_io.delete(objects)
        cloud_io.delete_folder(remote_path)
      end

    end
  end
end
