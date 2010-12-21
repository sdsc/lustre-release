#! /usr/bin/env ruby

require "optparse"
require "socket"


def main()
 # This hash will hold all of the options
  # parsed from the command-line by
  # OptionParser.
  options = {}

  optparse = OptionParser.new do|opts|
    # Set a banner, displayed at the top
    # of the help screen.
    opts.banner = "Usage: test_results.rb [options] LOGDIRECTORY"

    # Define the options, and what they do
    options[:verbose] = false
    opts.on( '-v', '--verbose', 'Output more information' ) do
      options[:verbose] = true
    end

    options[:erase] = false
    opts.on( '-e', '--erase', 'Erase files after processing' ) do
       options[:erase] = true
    end

    options[:logdir] = nil
    opts.on( '-d', '--logdir DIRECTORY', 'Directory containing logs' ) do |directory|
       options[:logdir] = directory
    end

    # This displays the help screen, all programs are
    # assumed to have this option.
    opts.on( '-h', '--help', 'Display this screen' ) do
      puts opts
      exit
    end
  end

  # Parse the command-line. Remember there are two forms
  # of the parse method. The 'parse' method simply parses
  # ARGV, while the 'parse!' method parses ARGV and removes
  # any options found there, as well as any parameters for
  # the options. What's left is the file or directory name to process.
  optparse.parse!

  puts "Processing directory #{options[:logdir]}";

  time = Time.new()
  file_name = "Lustre-Test-Result-#{Socket.gethostname()}-#{time.year}#{time.month}#{time.day}#{time.hour}#{time.min}#{time.sec}.tgz"
  pwd = Dir.pwd()

  Dir.chdir(options[:logdir])

  `tar cf - * | gzip -c > #{file_name}`
  
  # To make this work you'll need to go to whamcloud.no-ip.org, register as a user.
  # Having done that you will need to add the contents of .ssh/id_rsa.pub [or other if you wish]
  # to your profile.
  `scp #{file_name} maloo@whamcloud.no-ip.org:incoming/#{file_name}`

  File.unlink(file_name)
ensure
  Dir.chdir(pwd)
end

main
