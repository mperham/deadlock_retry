# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{deadlock_retry}
  s.version = "1.0.1"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Jamis Buck", "Mike Perham"]
  s.date = %q{2009-02-07}
  s.description = %q{Provides automatical deadlock retry and logging functionality for ActiveRecord and MySQL}
  s.email = %q{mperham@gmail.com}
  s.files = ["README", "Rakefile", "VERSION.yml", "lib/deadlock_retry.rb", "test/deadlock_retry_test.rb", "CHANGELOG"]
  s.has_rdoc = true
  s.homepage = %q{http://github.com/mperham/deadlock_retry}
  s.rdoc_options = ["--inline-source", "--charset=UTF-8"]
  s.require_paths = ["lib"]
  s.rubygems_version = %q{1.3.1}
  s.summary = %q{Provides automatical deadlock retry and logging functionality for ActiveRecord and MySQL}

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 2

    if Gem::Version.new(Gem::RubyGemsVersion) >= Gem::Version.new('1.2.0') then
    else
    end
  else
  end
end
