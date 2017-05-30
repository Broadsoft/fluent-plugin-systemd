require "systemd/journal"
require "fluent/input"
require "fluent/plugin/systemd/pos_writer"

module Fluent
  class SystemdInput < Input
    Fluent::Plugin.register_input("systemd", self)

    config_param :path, :string, default: "/var/log/journal"
    config_param :filters, :array, default: []
    config_param :pos_file, :string, default: nil
    config_param :read_from_head, :bool, default: false
    config_param :strip_underscores, :bool, default: false
    config_param :tag, :string

    # When true, plugin will join log messages marked with CONTAINER_PARTIAL_MESSAGE flag
    # See https://github.com/moby/moby/issues/32923 for details
    config_param :join_docker_partials, :bool, default: false

    # To avoid loosing partial log messages, a buffer will preserve data
    config_param :docker_partials_file, :string, default: nil

    # To avoid memory leak, old partial messages will be dropped upon exceeding configured timeout
    config_param :docker_partials_cleanup_retention_time, :time, default: 30

    # Cleanup will be started with defined probability (float 0 <= p <= 1) for each journald message
    config_param :docker_partials_cleanup_probability, :float, default: 0.01

    def configure(conf)
      super
      @pos_writer = PosWriter.new(@pos_file)
      # use same class for storing partials buffer
      @partials_writer = @join_docker_partials ? PosWriter.new(@docker_partials_file) : nil
    end

    def start
      super
      @running = true
      @partials = {}
      @pos_writer.start
      @partials_writer.start if @join_docker_partials
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      super
      @running = false
      @thread.join
      @pos_writer.shutdown
      if @join_docker_partials
        docker_partials_cleanup
        @partials_writer.shutdown
      end
    end

    private

    def init_journal
      @journal = Systemd::Journal.new(path: @path)
      # make sure initial call to wait doesn't return :invalidate
      # see https://github.com/ledbettj/systemd-journal/issues/70
      @journal.wait(0)
      @journal.filter(*@filters)
      seek
      if @join_docker_partials
        @partials = Marshal::load(@partials_writer.cursor) rescue {}
      end
    end

    def seek
      seek_to(@pos_writer.cursor || read_from)
    rescue Systemd::JournalError
      log.warn("Could not seek to cursor #{@pos_writer.cursor} found in pos file: #{@pos_writer.path}")
      seek_to(read_from)
    end

    # according to https://github.com/ledbettj/systemd-journal/issues/64#issuecomment-271056644
    # and https://bugs.freedesktop.org/show_bug.cgi?id=64614, after doing a seek(:tail),
    # you must move back in such a way that the next move_next will return the last
    # record
    def seek_to(pos)
      @journal.seek(pos)
      return unless pos == :tail
      @journal.move(-2)
    end

    def read_from
      @read_from_head ? :head : :tail
    end

    def run
      init_journal
      Thread.current.abort_on_exception = true
      watch do |entry|
        begin
          full_entry = @join_docker_partials ? join_docker_partials(entry.to_h) : entry.to_h
          router.emit(@tag, entry.realtime_timestamp.to_i, formatted(full_entry)) if full_entry != nil
        rescue => e
          log.error("Exception emitting record: #{e}")
        end
      end
    end

    def formatted(entry)
      return entry unless @strip_underscores
      Hash[entry.map { |k, v| [k.gsub(/\A_+/, ""), v] }]
    end

    # Accepts entry that may be full or partial and returns full entry or nil
    # Partial entries are accumulated in buffer and "message" field in concatenated
    def join_docker_partials(entry)
      # skip non-docker entries
      id = entry['CONTAINER_ID_FULL']
      return entry unless id

      result = nil
      updated = false
      if entry['CONTAINER_PARTIAL_MESSAGE'] == 'true'
        # If this is partial message
        if @partials[id] != nil
          # Append to message from same container existing in buffer
          log.debug("Appended partial from #{id}")
          @partials[id]['MESSAGE'] += entry['MESSAGE']
          updated = true
        else
          # Set as head of full message in partials buffer
          log.debug("Started partial from #{id}")
          @partials[id] = entry
          updated = true
        end
      else
        if @partials[id] != nil
          # If there's partial message in buffer from same container, assume this is the tail of partial message
          # Append it to existing partial in buffer and return
          log.debug("Ended partial from #{id}")
          @partials[id]['MESSAGE'] += entry['MESSAGE']
          @partials[id].delete('CONTAINER_PARTIAL_MESSAGE')
          result = @partials[id]
          @partials.delete(id)
          updated = true
        else
          # Simply return the entry
          result = entry
        end
      end
      # Run cleanup with specified probability
      docker_partials_cleanup if rand <= @docker_partials_cleanup_probability
      # Persist buffer to file
      @partials_writer.update(Marshal::dump(@partials)) if updated
      result
    end

    # Cleans up partial message buffer
    def docker_partials_cleanup
      log.debug("Executing docker partial logs cleanup")
      @partials.each do |id, entry|
        entry_time = entry['_SOURCE_REALTIME_TIMESTAMP'] || entry['SOURCE_REALTIME_TIMESTAMP']
        delta = Time.now.to_i - (entry_time.to_i / 1000000)
        if delta > @docker_partials_cleanup_retention_time
          log.warn("Dropping docker partial message (event age=#{delta}, timeout=#{@docker_partials_cleanup_retention_time}): #{entry}")
          @partials.delete(id)
        end
      end
    end

    def watch
      while @running
        init_journal if @journal.wait(0) == :invalidate
        while @journal.move_next && @running
          yield @journal.current_entry
          @pos_writer.update(@journal.cursor)
        end
        # prevent a loop of death
        sleep 1
      end
    end
  end
end
