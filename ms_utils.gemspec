# Ensure we require the local version and not one we might have installed already
require File.join([File.dirname(__FILE__),'lib','ms_utils_version.rb'])
spec = Gem::Specification.new do |s| 
  s.name = 'ms_utils'
  s.version = MsUtils::VERSION
  s.author = 'Your Name Here'
  s.email = 'your@email.address.com'
  s.homepage = 'http://your.website.com'
  s.platform = Gem::Platform::RUBY
  s.summary = 'A description of your project'
# Add your other files here if you make them
  s.files = %w(
bin/ms_utils
lib/ms_utils_version.rb
lib/ms_utils.rb
  )
  s.require_paths << 'lib'
  s.has_rdoc = true
  s.extra_rdoc_files = ['README.rdoc','ms_utils.rdoc']
  s.rdoc_options << '--title' << 'ms_utils' << '--main' << 'README.rdoc' << '-ri'
  s.bindir = 'bin'
  s.executables << 'ms_utils'
  s.add_development_dependency('rake')
  s.add_development_dependency('rdoc')
  s.add_runtime_dependency('gli')
  s.add_dependency('gooddata')
  s.add_dependency('rainbow')
  s.add_dependency('hpricot')
end
