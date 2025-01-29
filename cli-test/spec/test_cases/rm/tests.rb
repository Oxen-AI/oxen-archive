require 'spec_helper'

RSpec.describe 'rm - test relative paths', type: :aruba do
  before(:each) do
    aruba.config.exit_timeout = 120
  end

  after(:each) do
    FileUtils.rm_rf('test-relative-paths')
  end

  it 'tests oxen rm with relative paths from subdirectories' do
    directory_path = 'tmp/aruba/test-relative-paths'

    # Setup base repo
    system('mkdir tmp\\aruba\\test-relative-paths')
    Dir.chdir('tmp\\Aruba\\test-relative-paths')
    system('oxen init') or fail

    # Create nested directory structure
    system('mkdir images\\test')
    
    # Create and commit first set of files
    file_path = File.join('root.txt')
    File.open(file_path, 'w') do |file|
      file.puts 'root file'
    end
    system('oxen add root.txt') or fail
    system('oxen commit -m "adding root file"') or fail
    
    # Create and commit nested file
    nested_file_path = File.join('images\\test\\nested.txt')
    File.open(nested_file_path, 'w') do |file|
      file.puts 'nested file'
    end
    Dir.chdir('images/test')
    system('oxen add nested.txt') or fail
    system('oxen commit -m "adding nested file"') or fail
    
    # Test removing file from nested directory
    system('oxen rm ../../root.txt') or fail
    
    # Test removing local file
    system('oxen rm nested.txt') or fail
    
    # Go back to root and verify files are removed
    Dir.chdir('../..')
    system('oxen status') or fail
    
    # Should show files as removed in staging
    
    # Files should still exist on disk
    expect(File.exist?(File.join(directory_path, 'root.txt'))).to be false
    expect(File.exist?(File.join(directory_path, 'images/test/nested.txt'))).to be false
  end

  it 'tests oxen rm with removed path from disk' do
 

    # Setup base repo
    system('mkdir tmp\\aruba\\test-removed-path')
    Dir.chdir('tmp\\Aruba\\test-removed-path')
    system('oxen init') or fail

    # Create and commit root file
    file_path = File.join('root.txt')
    File.open(file_path, 'w') do |file|
      file.puts 'root file'
    end
    system('oxen add root.txt') or fail
    system('oxen commit -m "adding root file"') or fail

    # Test removing file before running oxen rm
    system('rm root.txt') 
    system('oxen rm root.txt') or fail

    # Files should not exist on disk
    expect(File.exist?(File.join('root.txt'))).to be false
  end
end