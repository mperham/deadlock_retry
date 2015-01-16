require 'active_support/core_ext/module/attribute_accessors'

module DeadlockRetry
  def self.included(base)
    base.extend(ClassMethods)
    base.class_eval do
      class << self
        alias_method_chain :transaction, :deadlock_handling
      end
    end
  end

  mattr_accessor :innodb_status_cmd

  module ClassMethods
    DEADLOCK_ERROR_MESSAGES = [
      "Deadlock found when trying to get lock",
      "deadlock detected"
    ] # removed "Lock wait timeout exceeded" as retrying is potentially suicidal

    MAXIMUM_RETRIES_ON_DEADLOCK = 3


    def transaction_with_deadlock_handling(*objects, &block)
      retry_count = 0

      check_innodb_status_available

      begin
        transaction_without_deadlock_handling(*objects, &block)
      rescue ActiveRecord::StatementInvalid => error
        raise if in_nested_transaction?
        if DEADLOCK_ERROR_MESSAGES.any? { |msg| error.message =~ /#{Regexp.escape(msg)}/ }
          raise if retry_count >= MAXIMUM_RETRIES_ON_DEADLOCK
          retry_count += 1
          logger.info "Deadlock detected on retry #{retry_count}, restarting transaction"
          log_innodb_status if DeadlockRetry.innodb_status_cmd
          exponential_pause(retry_count)
          retry
        else
          raise
        end
      end
    end

    private

    WAIT_TIMES = [0, 1, 2, 4, 8, 16, 32]

    def exponential_pause(count)
      sec = WAIT_TIMES[count-1] || 32
      # sleep 0, 1, 2, 4, ... seconds up to the MAXIMUM_RETRIES.
      # Cap the pause time at 32 seconds.
      sleep(sec) if sec != 0
    end

    def in_nested_transaction?
      # open_transactions was added in 2.2's connection pooling changes.
      connection.open_transactions != 0
    end

    def show_innodb_status
       self.connection.select_value(DeadlockRetry.innodb_status_cmd)
    end

    # Should we try to log innodb status -- if we don't have permission to,
    # we actually break in-flight transactions, silently (!)
    def check_innodb_status_available
      return unless DeadlockRetry.innodb_status_cmd == nil

      if self.connection.adapter_name == "MySQL"
        begin
          mysql_version = self.connection.select_rows('show variables like \'version\'')[0][1]
          cmd = if mysql_version < '5.5'
            'show innodb status'
          else
            'show engine innodb status'
          end
          self.connection.select_value(cmd)
          DeadlockRetry.innodb_status_cmd = cmd
        rescue
          logger.info "Cannot log innodb status: #{$!.message}"
          DeadlockRetry.innodb_status_cmd = false
        end
      else
        DeadlockRetry.innodb_status_cmd = false
      end
    end

    def log_innodb_status
      # show innodb status is the only way to get visiblity into why
      # the transaction deadlocked.  log it.
      lines = show_innodb_status
      logger.warn "INNODB Status follows:"
      lines.each_line do |line|
        logger.warn line
      end
    rescue => e
      # Access denied, ignore
      logger.info "Cannot log innodb status: #{e.message}"
    end

  end
end

ActiveRecord::Base.send(:include, DeadlockRetry) if defined?(ActiveRecord)
