require 'spec_helper'

RSpec.describe 'add - test relative paths', type: :aruba do
  before(:each) do
    puts 0
    aruba.config.exit_timeout = 120
  end

  after(:each) do
    FileUtils.rm_rf('test-relative-paths')
  end

  it 'tests oxen add with relative paths from subdirectories' do
    puts 1
    directory_path = 'tmp/aruba/test-relative-paths'
    puts 2
    # Setup base repo
    run_command_and_stop('mkdir test-relative-paths')
    puts 3
    cd 'test-relative-paths'
    puts  4
    run_command_and_stop('oxen init')
    puts 5
    # Create nested directory structure
    run_command_and_stop('mkdir -p images/test')
    puts 6
    file_path = File.join(directory_path, 'hi.txt')
    puts 7
    File.open(file_path, 'a') do |file|
      file.puts 'This is a simple text file.'
    end
    puts 8
    # Create a file in root from nested directory
    cd 'images/test'
    puts 9
    # Add file using relative path from nested directory
    run_command_and_stop('oxen add ../../hi.txt')
    puts 10
    # Create another file in nested directory
    puts 11
    file_path = File.join(directory_path, 'images/test/nested.txt')
    File.open(file_path, 'w') do |file|
      file.puts 'nested'
    end
    puts 12
    # Add file from current directory
    run_command_and_stop('oxen add nested.txt')
    puts 13
    # Go back to root and verify files
    cd '../..'
    run_command_and_stop('oxen status')
    puts 14
    expect(last_command_started).to have_output(/hi\.txt/)
    puts 15
    expect(last_command_started).to have_output(/images\/test\/nested\.txt/)
    puts  16
    # Verify file contents
    expect(File.read(File.join(directory_path, 'hi.txt'))).to eq("This is a simple text file.\n")
    puts 17
    expect(File.read(File.join(directory_path, 'images/test/nested.txt'))).to eq("nested\n")
    puts 18
  end
end
