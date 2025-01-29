require 'spec_helper'
require 'pathname'

RSpec.describe 'add - test relative paths', type: :aruba do
  before(:each) do
    aruba.config.exit_timeout = 120
  end

  after(:each) do
    FileUtils.rm_rf('test-relative-paths')
  end

  it 'tests oxen add with relative paths from subdirectories' do
    directory_path = "tmp\\aruba\\test-relative-paths"
    # Setup base repo
   
    system("mkdir tmp\\aruba\\test-relative-paths")
    Dir.chdir('tmp\\Aruba\\test-relative-paths')
    system('oxen init') or fail

    # Create nested directory structure
    file_path = File.join('hi.txt')
   
    File.open(file_path, 'a') do |file|
      file.puts 'This is a simple text file.'
    end

    system('mkdir Images\\Test')
  

    # Add file using relative path from nested directory
    Dir.chdir('images\\test')

    system("oxen add ..\\..\\hi.txt") or fail

    # Create another file in nested directory
    file_path = File.join('nested.txt')
    File.open(file_path, 'w') do |file|
      file.puts 'nested'
    end

    # Add file from current directory
    system("oxen add nested.txt") or fail
  

    # Go back to root
    Dir.chdir("..\\..")

    # Verify file contents
    system('oxen status') or fail
  
    # Verify file contents
    expect(File.read(File.join('hi.txt'))).to eq("This is a simple text file.\n")
    expect(File.read(File.join('images/test/nested.txt'))).to eq("nested\n")
  end
end
