require 'active_support/core_ext/module/attribute_accessors'
require 'base64'

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
    STATEMENT_INVALID_ERROR_MESSAGES = [
      "Try restarting transaction",
      "Duplicate entry"
    ]

    MAX_RETRIES_ON_STATEMENT_INVALID = 5


    def transaction_with_deadlock_handling(*objects, &block)
      retry_count = 0

      check_innodb_status_available

      begin
        transaction_without_deadlock_handling(*objects, &block)
      rescue ActiveRecord::StatementInvalid => error
        raise if in_nested_transaction?
        if STATEMENT_INVALID_ERROR_MESSAGES.any? { |msg| error.message =~ /#{Regexp.escape(msg)}/i }
          raise if retry_count >= MAX_RETRIES_ON_STATEMENT_INVALID
          retry_count += 1
          log(retry_count)
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
      open_transactions != 0
    end

    def open_transactions
      connection.open_transactions
    end

    def show_innodb_status
       self.connection.select_one(DeadlockRetry.innodb_status_cmd)["Status"]
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
          self.connection.select_one(cmd)
          DeadlockRetry.innodb_status_cmd = cmd
        rescue Exception => e
          logger.info "Cannot log innodb status: #{e.message}"

          DeadlockRetry.innodb_status_cmd = false
        end
      else
        DeadlockRetry.innodb_status_cmd = false
      end
    end

    def log(retry_count)
      logger.warn "retry_tx.attempt=#{retry_count} retry_tx.max_attempts=#{MAX_RETRIES_ON_STATEMENT_INVALID} retry_tx.opentransactions=#{open_transactions} retry_tx.innodbstatusb64=#{base64_innodb_status}"
    end

    def base64_innodb_status
      # show innodb status is the only way to get visiblity into why
      # the transaction deadlocked.  log it.
      Base64.encode64(show_innodb_status).gsub("\n","")
    rescue => e
      logger.info "Cannot log innodb status: #{e.message}"
    end

  end
end

ActiveRecord::Base.send(:include, DeadlockRetry) if defined?(ActiveRecord)
