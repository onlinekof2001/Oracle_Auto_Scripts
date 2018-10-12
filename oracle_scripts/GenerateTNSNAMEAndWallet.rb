require 'rubygems'
require 'dbi'
require 'json'
require 'oci8'

# export ORACLE_HOME=/usr/lib/oracle/12.1/client64
# export LD_LIBRARY_PATH=$ORACLE_HOME/lib

# Genere un fichier TNSNAME.ORA et un script permettant la creation du wallet (PRD/PP)
# A lancer en prod
# Copier le script généré puis l'executer sur un serveur possedant mkstore. (like rtdka2odb02)
# Puis rappatrier les 2 fichiers wallet sur le prod et PP


begin
  dbh = DBI.connect("DBI:OCI8://infra2odb50.hosting.eu:1531/bronx69_infra.hosting.eu","sysman","SYSMAN4ever")
  req = dbh.prepare("SELECT * 
FROM mgmt$group_flat_memberships mb
inner join mgmt$target t on t.target_guid = mb.member_target_guid
where mb.composite_target_name ='RDK_GRP'
and t.target_type='oracle_database'")

  req.execute()
  res = []
  req.fetch_hash do |row|
    res << row
  end

  file = File.open("#{ENV['ORACLE_HOME']}/network/admin/tnsnames.ora", 'w') 
  mkstorefile = File.open("#{ENV['ORACLE_HOME']}/network/admin/generatewallet.sh", 'w') 
  mkstorefile.write("#!/bin/bash\necho 'Decathlon01 Decathlon01' |/u01/app/oracle/product/11204/db11g01/bin/mkstore -wrl /tmp/wallet -create\n")

  res.each{|json|
    tns=json['MEMBER_TARGET_NAME'].split('.')[0]
    server=tns.split('_')[1]
    schema=tns.split('_')[0]

    version=json['TYPE_QUALIFIER1']
    port=1531
    case version
    when /^10/
      port=1530
    when /^11/
      port=1531
    when /^12/
      port=1532
    else 
      puts "[WARNING] #{server} NO VERSION FOUND - Default port = #{port}"
    end

    wallet_dir="/tmp/wallet"
    mkstore_bin="echo 'Decathlon01' |/u01/app/oracle/product/11204/db11g01/bin/mkstore"

    # no user - just TNS entry
    file.write("#{schema}_#{server}   =(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=#{server})(PORT=#{port})))(CONNECT_DATA=(SID=#{schema})))\n")

    # oracleuser
    user="oracleuser"
    password=""
    if json['MEMBER_TARGET_NAME'] =~ /preprod.org/
        password="oracleuser"
    else
        password="oraclepwd"
    end

    file.write("#{schema}_#{server}_#{user}   =(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=#{server})(PORT=#{port})))(CONNECT_DATA=(SID=#{schema})))\n")
    mkstorefile.write("#{mkstore_bin} -wrl #{wallet_dir} -createCredential #{tns}_#{user} #{user} #{password}\n")

    # nbo 
    next if not "#{server}" =~ /rtdk/
    next if not "#{schema}" =~ /tetrix02/
    
    if json['MEMBER_TARGET_NAME'] =~ /preprod.org/
      pool = ['02','04','05','06','14','22','26','25','30']
      pool.each do |number|
        user = "nbo00#{number}"
        password = "nbo00#{number}"
        file.write("#{schema}_#{server}_#{user}   =(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=#{server})(PORT=#{port})))(CONNECT_DATA=(SID=#{schema})))\n")
        mkstorefile.write("#{mkstore_bin} -wrl #{wallet_dir} -createCredential #{tns}_#{user} #{user} #{password}\n")
      end
    else
      user="nbo"
      password="vh113e60"
      file.write("#{schema}_#{server}_#{user}   =(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=#{server})(PORT=#{port})))(CONNECT_DATA=(SID=#{schema})))\n")
      mkstorefile.write("#{mkstore_bin} -wrl #{wallet_dir} -createCredential #{tns}_#{user} #{user} #{password}\n")
    end

    # stcom
    next if not "#{server}" =~ /rtdk/
    next if not "#{schema}" =~ /tetrix02/

    if json['MEMBER_TARGET_NAME'] =~ /preprod.org/
      pool = ['02','04','05','06','14','22','26','25','30']
      pool.each do |number|
        user = "stcom00#{number}"
        password = "stcom00#{number}"
        file.write("#{schema}_#{server}_#{user}   =(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=#{server})(PORT=#{port})))(CONNECT_DATA=(SID=#{schema})))\n")
        mkstorefile.write("#{mkstore_bin} -wrl #{wallet_dir} -createCredential #{tns}_#{user} #{user} #{password}\n")
      end
    else
      user="stcom"
      password="uy2nqqjx"
      file.write("#{schema}_#{server}_#{user}   =(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=#{server})(PORT=#{port})))(CONNECT_DATA=(SID=#{schema})))\n")
      mkstorefile.write("#{mkstore_bin} -wrl #{wallet_dir} -createCredential #{tns}_#{user} #{user} #{password}\n")
    end


  }
rescue DBI:: DatabaseError => e
  puts "An error occurred"
  puts "Error code:    #{e.err}"
  puts "Error message: #{e.errstr}"
rescue IOError => e
    puts "Error open file :   #{e.err}"
ensure
  dbh.disconnect if dbh
  file.close if file
  mkstorefile.close if mkstorefile
end

