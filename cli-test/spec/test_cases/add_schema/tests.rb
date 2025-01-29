require 'spec_helper'

RSpec.describe 'schemas add - test relative paths', type: :aruba do
  before(:each) do
    aruba.config.exit_timeout = 120
  end

  after(:each) do
    FileUtils.rm_rf('test-schema-paths')
  end

  it 'tests oxen schemas add with relative paths from subdirectories' do
  
    # Setup base repo
    system('mkdir tmp\\Aruba\\test-schema-paths')
    
    Dir.chdir('tmp\\Aruba\\test-schema-paths')
    system('oxen init')

    # Create nested directory structure
    system('mkdir data\\frames')

    csv_path = File.join('root.csv')
    File.open(csv_path, 'w') do |file|
      file.puts 'id,image,description'
      file.puts '1,/path/to/img1.jpg,test image 1'
      file.puts '2,/path/to/img2.jpg,test image 2'
    end

    system('oxen add root.csv') or fail
    system('oxen commit -m "adding root csv"') or fail


    # Create a CSV file in the nested directory
    csv_path = File.join('data/frames/test.csv')
    File.open(csv_path, 'w') do |file|
      file.puts 'id,image,description'
      file.puts '1,/path/to/img1.jpg,test image 1'
      file.puts '2,/path/to/img2.jpg,test image 2'
    end

    Dir.chdir('data\\frames')

    # Add and commit the CSV file
    system('oxen add test.csv') or fail
    system('oxen commit -m "adding test csv"') or fail

    json = '{"oxen": "{"render": "{"func": "image"}"}"}'


    puts 1
    puts 1
    puts 1
    puts 1
    puts 1
    puts 1
    puts 1
    puts 1
    puts 1
    puts 1
    puts 1
    puts 1
    puts 1
    puts 1
    puts 1
    
    system('oxen schemas add -c image -m #{json} test.csv') or fail
    
    system("oxen schemas add ../../root.csv -c image -m #{json}") or fail

    # Verify schema changes
    Dir.chdir('..')
    system('oxen status') or fail

  end
end