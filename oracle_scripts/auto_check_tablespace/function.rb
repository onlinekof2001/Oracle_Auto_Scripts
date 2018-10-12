def ssh_exec!(ssh, command)
  stdout_data = ""
  stderr_data = ""
  exit_code = nil
  exit_signal = nil
  ssh.open_channel do |channel|

    channel.request_pty do |ch , success|
      raise "I can't get pty rquest" unless success
        ch.exec(command) do |ch, success|
          unless success
            abort "FAILED: couldn't execute command (ssh.channel.exec)"
          end
          channel.on_data do |ch,data|
            stdout_data+=data
          end

          channel.on_extended_data do |ch,type,data|
            stderr_data+=data
          end

          channel.on_request("exit-status") do |ch,data|
            exit_code = data.read_long
          end

          channel.on_request("exit-signal") do |ch, data|
            exit_signal = data.read_long
          end
        end
    end
  end
  ssh.loop
  [stdout_data, stderr_data, exit_code, exit_signal]
end

