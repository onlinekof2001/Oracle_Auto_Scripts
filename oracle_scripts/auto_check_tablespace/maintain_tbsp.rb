#!/usr/bin/ruby

require 'rubygems'
require 'net/http'
require 'json'
require 'net/ssh'
require File.join File.dirname(__FILE__),  'function.rb'
require File.join File.dirname(__FILE__),  'ora_lib.rb'

SSH_USER="rundeck"

unless ARGV.length == 2
  puts "Dude,not the right number of arguments."
  puts "Usage: operaion_instance.rb servername itemname operation\n"
  exit
end

#real time log for rundeck
$stdout.sync = true

path=ARGV[0]
serlist=ARGV[1]

serlist.split(';').each do |tns|
    instid=tns.split('_').first
    server=tns.split('_').last

url = URI.parse("http://127.0.0.1:4567/getAppServersByDatabase?Database=#{server}".gsub(' ', '%20'))
req = Net::HTTP::Get.new(url.to_s)
res = Net::HTTP.start(url.host, url.port) {|http|
      http.request(req)
}

##just keep the first apps.
v=JSON.parse(res.body)[0]

puts "Connect_to_DB: #{server} - #{instid} - #{path}" #- #{v['Platform']}

dbact=INST.new(server,SSH_USER)
dbact.checkdisk(path)
end
