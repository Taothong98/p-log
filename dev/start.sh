# !/bin/bash
cheak=echo "/home/link/cheakdir.sh"
eval $check 
echo ""

/usr/share/logstash/bin/logstash-plugin install logstash-filter-cipher
# /usr/share/logstash/bin/logstash-plugin update logstash-filter-cipher
# /usr/share/logstash/bin/logstash-plugin remove logstash-filter-cipher
# /usr/share/logstash/bin/logstash-plugin install logstash-filter-cipher

# [Service]
# Environment="LOG_ENCRYPTION_KEY=รหัสลับ32ตัวอักษรของคุณที่นี่นะ"
# Environment="LOG_ENCRYPTION_IV=รหัสเริ่มต้น16ตัว"
# Environment="LOG_ENCRYPTION_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
# Environment="LOG_ENCRYPTION_IV=xxxxxxxxxxxxxxxx"
# sudo systemctl restart logstash
# service restart logstash

/usr/share/logstash/bin/logstash -f /etc/logstash/conf.d/logstash.conf
echo ""

cheak=echo "/home/link/cheakdir.sh"
eval $check 
echo ""

 







