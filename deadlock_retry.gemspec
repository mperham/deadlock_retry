# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{deadlock_retry}
  s.version = "1.1.1"
  s.authors = ["Jamis Buck", "Mike Perham"]
  s.description = s.summary = %q{Provides automatic deadlock retry and logging functionality for ActiveRecord and MySQL}
  s.email = %q{mperham@gmail.com}
  s.files = `git ls-files`.split("\n")
  s.homepage = %q{http://github.com/mperham/deadlock_retry}
  s.require_paths = ["lib"]
end
