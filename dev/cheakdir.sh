# ตั้งค่าไดเรกทอรีที่ต้องการตรวจสอบ
DIRECTORY="/var/log/logstash/outputlogs"


inotifywait -m -e create -e delete -e modify $DIRECTORY | while read path action file; do
    
    echo "The file '$file' at path '$path' was $action" > /home/link/createdfile.log
    /home/link/action.sh

done &