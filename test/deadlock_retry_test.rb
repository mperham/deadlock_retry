require 'rubygems'

# Change the version if you want to test a different version of ActiveRecord
gem 'activerecord', ENV['ACTIVERECORD_VERSION'] || ' ~>3.0'
require 'active_record'
require 'active_record/version'
puts "Testing ActiveRecord #{ActiveRecord::VERSION::STRING}"

require 'minitest'
require 'minitest/autorun'
require 'mocha/mini_test'
require 'logger'
require_relative  "../lib/deadlock_retry"

class MockModel
  @@open_transactions = 0

  def self.transaction(*objects)
    @@open_transactions += 1
    yield
  ensure
    @@open_transactions -= 1
  end

  def self.open_transactions
    @@open_transactions
  end

  def self.connection
    self
  end

  def self.logger
    @logger ||= Logger.new(nil)
  end

  def self.show_innodb_status
    "1607bf000 INNODB MONITOR OUTPUT"
  end

  def self.select_rows(sql)
    [['version', '5.1.45']]
  end

  def self.select_one(sql)
    true
  end

  def self.adapter_name
    "MySQL"
  end

  include DeadlockRetry
end

class DeadlockRetryTest < MiniTest::Test

  DEADLOCK_ERROR = "MySQL::Error: Deadlock found when trying to get lock. Try restarting transaction"
  TIMEOUT_ERROR = "MySQL::Error: Lock wait timeout exceeded. Try restarting transaction"
  DUPLICATE_ERROR = "ActiveRecord::RecordNotUnique: Duplicate entry"

  def setup
    MockModel.stubs(:exponential_pause)
  end

  def test_no_errors
    assert_equal :success, MockModel.transaction { :success }
  end

  def test_no_errors_with_deadlock
    errors = [ DEADLOCK_ERROR ] * DeadlockRetry::ClassMethods::MAX_RETRIES_ON_STATEMENT_INVALID
    assert_equal :success, MockModel.transaction { raise ActiveRecord::StatementInvalid, errors.shift unless errors.empty?; :success }
    assert errors.empty?
  end

  def test_no_errors_with_lock_timeout
    errors = [ TIMEOUT_ERROR ] * DeadlockRetry::ClassMethods::MAX_RETRIES_ON_STATEMENT_INVALID
    assert_equal :success, MockModel.transaction { raise ActiveRecord::StatementInvalid, errors.shift unless errors.empty?; :success }
    assert errors.empty?
  end

  def test_no_errors_with_duplicate
    errors = [ DUPLICATE_ERROR ] * DeadlockRetry::ClassMethods::MAX_RETRIES_ON_STATEMENT_INVALID
    assert_equal :success, MockModel.transaction { raise ActiveRecord::StatementInvalid, errors.shift unless errors.empty?; :success }
    assert errors.empty?
  end

  def test_error_if_limit_exceeded
    assert_raises(ActiveRecord::StatementInvalid) do
      MockModel.transaction { raise ActiveRecord::StatementInvalid, DEADLOCK_ERROR }
    end
  end

  def test_error_if_unrecognized_error
    assert_raises(ActiveRecord::StatementInvalid) do
      MockModel.transaction { raise ActiveRecord::StatementInvalid, "Something else" }
    end
  end

  def test_included_by_default
    assert ActiveRecord::Base.respond_to?(:transaction_with_deadlock_handling)
  end

  def test_innodb_status_availability
    DeadlockRetry.innodb_status_cmd = nil
    MockModel.transaction {}
    assert_equal "show innodb status", DeadlockRetry.innodb_status_cmd
  end

  def test_failure_logging
    mock_logger = mock
    MockModel.expects(:logger).returns(mock_logger)
    mock_logger.expects(:warn).with("retry_tx.attempt=1 retry_tx.max_attempts=5 retry_tx.opentransactions=0 retry_tx.innodbstatusb64=MTYwN2JmMDAwIElOTk9EQiBNT05JVE9SIE9VVFBVVA==")
    errors = [ TIMEOUT_ERROR ]
    assert_equal :success, MockModel.transaction { raise ActiveRecord::StatementInvalid, errors.shift unless errors.empty?; :success }
    assert errors.empty?
  end

  def test_error_in_nested_transaction_should_retry_outermost_transaction
    tries = 0
    errors = 0

    MockModel.transaction do
      tries += 1
      MockModel.transaction do
        MockModel.transaction do
          errors += 1
          raise ActiveRecord::StatementInvalid, TIMEOUT_ERROR unless errors > DeadlockRetry::ClassMethods::MAX_RETRIES_ON_STATEMENT_INVALID
        end
      end
    end

    assert_equal DeadlockRetry::ClassMethods::MAX_RETRIES_ON_STATEMENT_INVALID + 1, tries
  end
end
