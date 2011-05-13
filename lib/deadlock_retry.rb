module DeadlockRetry
  def self.included(base)
    base.extend(ClassMethods)
    base.class_eval do
      class << self
        alias_method_chain :transaction, :deadlock_handling
      end
    end
  end

  @@innodb_status_available = nil

  def self.innodb_status_available?
    @@innodb_status_available
  end

  def self.innodb_status_available=(bool)
    @@innodb_status_available = bool
  end

  module ClassMethods
    DEADLOCK_ERROR_MESSAGES = [
      "Deadlock found when trying to get lock",
      "Lock wait timeout exceeded"
    ]

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
          log_innodb_status if DeadlockRetry.innodb_status_available?
          retry
        else
          raise
        end
      end
    end

    private

    def in_nested_transaction?
      # open_transactions was added in 2.2's connection pooling changes.
      connection.open_transactions != 0
    end

    def show_innodb_status
       self.connection.select_value("show innodb status")
    end

    # Should we try to log innodb status -- if we don't have permission to,
    # we actually break in-flight transactions, silently (!)
    def check_innodb_status_available
      return unless DeadlockRetry.innodb_status_available?.nil?

      begin
        show_innodb_status
        DeadlockRetry.innodb_status_available = true
      rescue
        DeadlockRetry.innodb_status_available = false
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
    rescue Exception => e
      # Access denied, ignore
      logger.warn "Cannot log innodb status: #{e.message}"
    end

  end
end

ActiveRecord::Base.send(:include, DeadlockRetry)
