
# จำนวนไฟล์ที่จะบันทึก
NUMFILE="90"

# ตำแหน่งไดเรกทอรี
DIRECTORY="/var/log/logstash/outputlogs"

# นับจำนวนไฟล์ในไดเรกทอรี
FILE_COUNT=$(ls -1 "$DIRECTORY" | wc -l)

# ตรวจสอบว่าจำนวนไฟล์มีมากกว่า NUMFILE ไฟล์หรือไม่
if [ "$FILE_COUNT" -gt "$NUMFILE" ]; then
    # ลบไฟล์ที่เก่าที่สุดในไดเรกทอรี
    find "$DIRECTORY" -type f -printf '%T+ %p\n' | sort | head -n 1 | cut -d' ' -f2 | xargs rm
    echo "Deleted the oldest file because there are more than 31 files." >> /home/link/createdfile.log
    cat /home/link/createdfile.log
else
    echo "There are $FILE_COUNT files. No need to delete any files." >> /home/link/createdfile.log
    cat /home/link/createdfile.log
fi