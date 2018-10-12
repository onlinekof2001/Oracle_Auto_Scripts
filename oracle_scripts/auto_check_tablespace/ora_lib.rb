require 'rubygems'
require 'net/ssh'

require File.join File.dirname(__FILE__),  'function.rb'

class INST
    def initialize (server,user)
        @server = server
        @user   = user
    end

    def checkdisk(path)
        begin
            Net::SSH.start(@server, @user) do |ssh|
                diskrate=ssh_exec!(ssh, "/usr/local/nagios/libexec/check_disk -w 20% -c 10% -p #{path} | awk -F';' '{print $1}'")
                puts "\n#{diskrate[0]}"
            end
        rescue Net::SSH::AuthenticationFailed => e
            puts "[ERROR] #{@server} AUTH ERROR: "+e.message
            return false
        end
    end
end
