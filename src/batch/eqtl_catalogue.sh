exec &> /mnt/share/log.$BATCH_TASK_INDEX.log

echo ">>> Update APT"
sudo apt -y update

echo ">>> Install packages"
sudo apt -y install python3 python3-pip python3-setuptools

echo ">>> Update PIP and setuptools"
sudo python3 -m pip install --upgrade pip setuptools
echo $?

echo ">>> Install packages"
sudo python3 -m pip install pandas fsspec ftputil gcsfs pyarrow
echo $?

echo ">>> Run script"
sudo python3 /mnt/share/eqtl_catalogue.py ${BATCH_TASK_INDEX}
echo $?
